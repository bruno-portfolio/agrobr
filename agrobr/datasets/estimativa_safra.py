from __future__ import annotations

import copy
import warnings
from collections.abc import Awaitable, Callable
from dataclasses import replace
from typing import Any, Literal, overload

import pandas as pd

from agrobr import _log, constants
from agrobr.contracts import estimativa_safra as safra_contract
from agrobr.datasets.base import BaseDataset, DatasetInfo, DatasetSource, _unpack_result
from agrobr.datasets.deterministic import get_snapshot
from agrobr.datasets.registry import register
from agrobr.exceptions import (
    ContractViolationError,
    InvalidParameterError,
    ParseError,
    SourceUnavailableError,
)
from agrobr.models import MetaInfo
from agrobr.normalize import dates, regions
from agrobr.utils import result as result_utils
from agrobr.utils import validation
from agrobr.utils.time import hoje

logger = _log.get_logger(__name__)

_SAFRA_OUTPUT_COLS = safra_contract.ESTIMATIVA_SAFRA_V3_1.list_columns()


def _lspa_violation(message: str) -> ContractViolationError:
    return ContractViolationError(dataset="estimativa_safra", violation=message)


def _select_lspa_period(df: pd.DataFrame, safra: str, mes: int | None) -> pd.DataFrame:
    required = {
        "ano",
        "mes",
        "produto",
        "variavel",
        "valor",
        "unidade",
        "localidade",
        "localidade_cod",
    }
    missing = sorted(required - set(df.columns))
    if missing:
        raise _lspa_violation(f"Dimensões LSPA ausentes: {missing}")
    expected_year = dates.safra_para_anos(safra)[1]
    if df["ano"].isna().any() or not df["ano"].eq(expected_year).all():
        raise _lspa_violation(f"Ano LSPA incompatível com o ano final da safra: {expected_year}")
    if not pd.api.types.is_integer_dtype(df["mes"]) or not df["mes"].between(1, 12).all():
        raise _lspa_violation("Mês LSPA inválido")
    selected = df[df["variavel"].isin(constants.LSPA_ESTIMATIVA_UNIDADES)].copy()
    if mes is not None:
        return selected[selected["mes"] == mes]
    observed = selected.loc[selected["valor"].notna(), "mes"]
    if observed.empty:
        return selected.iloc[:0]
    latest_month = int(observed.max())
    return selected.loc[selected["mes"] == latest_month].copy()


def _validate_lspa_dimensions(df: pd.DataFrame, produto: str, uf: str | None) -> None:
    expected_code = regions.uf_para_ibge(uf) if uf else 1
    if df["localidade_cod"].isna().any() or not df["localidade_cod"].eq(expected_code).all():
        raise _lspa_violation("Localidade LSPA incompatível com o filtro solicitado")
    if df["localidade"].isna().any() or df["localidade"].nunique() != 1:
        raise _lspa_violation("Localidades LSPA misturadas ou ausentes")
    components = constants.LSPA_ESTIMATIVA_COMPONENTES[produto]
    expected_pairs = {
        (component, variable)
        for component in components
        for variable in constants.LSPA_ESTIMATIVA_UNIDADES
    }
    actual_pairs = set(df[["produto", "variavel"]].itertuples(index=False, name=None))
    if actual_pairs != expected_pairs:
        raise _lspa_violation("Componentes ou variáveis LSPA incompletos ou inesperados")
    if df.duplicated(["produto", "variavel"]).any():
        raise _lspa_violation("Observações LSPA duplicadas por componente e variável")
    expected_units = df["variavel"].map(constants.LSPA_ESTIMATIVA_UNIDADES)
    if df["unidade"].isna().any() or not df["unidade"].eq(expected_units).all():
        raise _lspa_violation("Unidades LSPA incompatíveis com a normalização de safra")
    values = df["valor"]
    if (
        not pd.api.types.is_numeric_dtype(values)
        or pd.api.types.is_bool_dtype(values)
        or pd.api.types.is_complex_dtype(values)
    ):
        raise _lspa_violation("Valores LSPA devem ser numéricos")
    if values.dropna().isin([float("inf"), float("-inf")]).any() or (values.dropna() < 0).any():
        raise _lspa_violation("Valores LSPA devem ser finitos e não negativos")


def _sum_lspa(df: pd.DataFrame, variable: str) -> float | None:
    values = df.loc[df["variavel"] == variable, "valor"]
    return float(values.sum()) if values.notna().all() else None


def _normalize_lspa(
    df: pd.DataFrame,
    produto: str,
    safra: str,
    uf: str | None,
    *,
    mes: int | None = None,
) -> pd.DataFrame:
    if df.empty:
        return safra_contract.ESTIMATIVA_SAFRA_V3_1.empty_frame()
    selected = _select_lspa_period(df, safra, mes)
    if selected.empty or selected["valor"].isna().all():
        return safra_contract.ESTIMATIVA_SAFRA_V3_1.empty_frame()
    _validate_lspa_dimensions(selected, produto, uf)
    area_plantada = _sum_lspa(selected, "Área plantada")
    area_colhida = _sum_lspa(selected, "Área colhida")
    producao = _sum_lspa(selected, "Produção")
    produtividade = (
        producao * 1000 / area_colhida if producao is not None and area_colhida else None
    )
    record = {
        "fonte": "ibge_lspa",
        "produto": produto,
        "safra": safra,
        "uf": uf.upper() if uf else None,
        "area_plantada": area_plantada / 1000 if area_plantada is not None else None,
        "area_colhida": area_colhida / 1000 if area_colhida is not None else None,
        "produtividade": produtividade,
        "producao": producao / 1000 if producao is not None else None,
        "levantamento": None,
        "data_publicacao": None,
        "ano_lspa": int(selected["ano"].iloc[0]),
        "mes_lspa": int(selected["mes"].iloc[0]),
    }
    result = pd.DataFrame([record], columns=_SAFRA_OUTPUT_COLS)
    for column in ("area_plantada", "area_colhida", "produtividade", "producao"):
        result[column] = result[column].astype("float64")
    for column in ("levantamento", "ano_lspa", "mes_lspa"):
        result[column] = result[column].astype("Int64")
    result["data_publicacao"] = pd.to_datetime(result["data_publicacao"])
    return result


class _SemObservacoes(SourceUnavailableError):
    pass


_FetchFn = Callable[..., Awaitable[tuple[pd.DataFrame, MetaInfo | None]]]


def _registrando(nome: str, fetch_fn: _FetchFn, registro: dict[str, Exception]) -> _FetchFn:
    async def chamada(produto: str, **kwargs: Any) -> tuple[pd.DataFrame, MetaInfo | None]:
        try:
            return await fetch_fn(produto, **kwargs)
        except (ParseError, _SemObservacoes) as exc:
            registro[nome] = exc
            raise

    return chamada


async def _fetch_conab(produto: str, **kwargs: Any) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr import conab

    result = await conab.safras(
        produto,
        safra=kwargs.get("safra"),
        uf=kwargs.get("uf"),
        levantamento=kwargs.get("levantamento"),
        return_meta=True,
    )
    df, meta = _unpack_result(result)
    if df.empty:
        raise _SemObservacoes(source="conab", last_error=f"CONAB sem estimativa de {produto}")
    requested = kwargs.get("levantamento")
    if requested is not None and (
        "levantamento" not in df
        or df["levantamento"].isna().any()
        or not df["levantamento"].eq(requested).all()
    ):
        raise ContractViolationError(
            dataset="estimativa_safra", violation="CONAB retornou outro levantamento"
        )
    return df, meta


async def _fetch_ibge_lspa(produto: str, **kwargs: Any) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr import ibge

    safra = kwargs.get("safra")
    uf = kwargs.get("uf")
    mes = kwargs.get("mes")
    corrente = hoje().year
    ano = dates.safra_para_anos(safra)[1] if safra else corrente
    if ano > corrente:
        raise SourceUnavailableError(
            source="ibge_lspa", last_error=f"o LSPA ainda não publica {ano}"
        )
    safra_resultado = safra or dates.anos_para_safra(ano - 1)
    result = await ibge.lspa(produto, ano=ano, mes=mes, uf=uf, return_meta=True)
    df, meta = _unpack_result(result)
    df = _normalize_lspa(df, produto, safra_resultado, uf, mes=mes)
    if df.empty:
        raise _SemObservacoes(
            source="ibge_lspa",
            last_error=f"LSPA sem dados de {produto} em {ano}"
            + (f"/{mes:02d}" if mes else "")
            + (f"/{uf}" if uf else ""),
        )
    return df, meta


def _resolve_selection(
    fonte: str | None, levantamento: int | None, mes: int | str | None
) -> tuple[str | None, int | None]:
    if fonte is not None and fonte not in ("conab", "ibge_lspa"):
        raise InvalidParameterError("fonte deve ser 'conab', 'ibge_lspa' ou None")
    if levantamento is not None and (
        isinstance(levantamento, bool)
        or not isinstance(levantamento, int)
        or not 1 <= levantamento <= 12
    ):
        raise InvalidParameterError("levantamento deve ser um inteiro entre 1 e 12")
    month = None
    if mes is not None:
        if isinstance(mes, bool) or not isinstance(mes, (int, str)):
            raise InvalidParameterError("mes deve ser um inteiro entre 1 e 12")
        try:
            month = int(mes)
        except ValueError as exc:
            raise InvalidParameterError("mes deve ser um inteiro entre 1 e 12") from exc
        if not 1 <= month <= 12:
            raise InvalidParameterError("mes deve ser um inteiro entre 1 e 12")
    if levantamento is not None and mes is not None:
        raise InvalidParameterError("levantamento CONAB e mes LSPA não podem ser combinados")
    if (levantamento is not None and fonte == "ibge_lspa") or (
        mes is not None and fonte == "conab"
    ):
        raise InvalidParameterError("Seletor temporal incompatível com a fonte solicitada")
    selected = "conab" if levantamento is not None else "ibge_lspa" if mes is not None else fonte
    return selected, month


ESTIMATIVA_SAFRA_INFO = DatasetInfo(
    name="estimativa_safra",
    description="Estimativas de safra por levantamento CONAB ou mês LSPA",
    sources=[
        DatasetSource(
            name="conab",
            priority=1,
            fetch_fn=_fetch_conab,
            description="CONAB Acompanhamento de Safra",
        ),
        DatasetSource(
            name="ibge_lspa",
            priority=2,
            fetch_fn=_fetch_ibge_lspa,
            description="IBGE LSPA",
        ),
    ],
    products=["soja", "milho", "arroz", "feijao", "trigo", "algodao"],
    contract_version="3.1",
    update_frequency="monthly",
    typical_latency="M+0",
    source_url="https://www.gov.br/conab/",
    source_institution="CONAB",
    min_date="2005-01-01",
    unit="mil ha / mil ton / kg/ha",
    license="livre",
)


class EstimativaSafraDataset(BaseDataset):
    info = ESTIMATIVA_SAFRA_INFO

    async def fetch(  # type: ignore[override]
        self,
        produto: str,
        safra: str | None = None,
        uf: str | None = None,
        *,
        return_meta: bool = False,
        fonte: Literal["conab", "ibge_lspa"] | None = None,
        levantamento: int | None = None,
        mes: int | str | None = None,
        as_polars: bool = False,
    ) -> result_utils.DataFrameResult:
        self._validate_produto(produto)
        selected, month = _resolve_selection(fonte, levantamento, mes)
        safra = validation.validate_safra(safra)
        if uf is not None and not isinstance(uf, str):
            raise InvalidParameterError("uf deve ser uma string ou None")
        uf = validation.validate_uf(uf)
        logger.info(
            "dataset_fetch",
            dataset="estimativa_safra",
            produto=produto,
            safra=safra,
            fonte=selected,
            levantamento=levantamento,
            mes=month,
        )
        snapshot = get_snapshot()
        registro: dict[str, Exception] = {}
        runner = copy.copy(self)
        runner.info = replace(
            self.info,
            sources=[
                replace(s, fetch_fn=_registrando(s.name, s.fetch_fn, registro))
                for s in self.info.sources
                if selected is None or s.name == selected
            ],
        )
        try:
            df, source_name, source_meta, attempted = await runner._try_sources(
                produto, safra=safra, uf=uf, levantamento=levantamento, mes=month
            )
        except SourceUnavailableError as erro:
            vazias = [nome for nome, exc in registro.items() if isinstance(exc, _SemObservacoes)]
            falhas = [nome for nome, _, _ in erro.errors if nome not in vazias]
            if not vazias:
                raise
            layout = [exc for nome in falhas if isinstance(exc := registro.get(nome), ParseError)]
            if falhas and len(layout) == len(falhas):
                raise ParseError(
                    source=f"estimativa_safra/{produto}",
                    parser_version=layout[-1].parser_version,
                    reason="As fontes com resposta falharam por layout; as demais vieram sem observações",
                    errors=erro.errors,
                    attempted_sources=erro.attempted_sources,
                ) from erro
            if falhas:
                raise
            aviso = (
                f"estimativa_safra: {', '.join(vazias)} responderam sem observações de {produto} "
                "para o recorte pedido; o resultado sai vazio"
            )
            warnings.warn(aviso, UserWarning, stacklevel=2)
            vazio = safra_contract.ESTIMATIVA_SAFRA_V3_1.empty_frame()
            meta = (
                self._build_meta(vazio, vazias[-1], None, erro.attempted_sources, snapshot)
                if return_meta
                else None
            )
            if meta is not None:
                meta.validation_warnings.append(aviso)
            return result_utils.finalize_result(
                vazio, meta, as_polars=as_polars, return_meta=return_meta
            )
        df = self._normalize(df, produto)
        self._validate_contract(df)
        meta = (
            self._build_meta(df, source_name, source_meta, attempted, snapshot)
            if return_meta
            else None
        )
        return result_utils.finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)

    def _normalize(self, df: pd.DataFrame, produto: str) -> pd.DataFrame:
        result = df.copy()
        if "produto" not in result:
            result["produto"] = produto
        if "fonte" not in result:
            result["fonte"] = "conab"
        for column in ("ano_lspa", "mes_lspa"):
            if column not in result:
                result[column] = pd.Series(pd.NA, index=result.index, dtype="Int64")
        result["unidade_producao"] = "mil_ton"
        result["unidade_area"] = "mil_ha"
        return result.reset_index(drop=True)


_estimativa_safra = EstimativaSafraDataset()
register(_estimativa_safra)


@overload
async def estimativa_safra(
    produto: str,
    safra: str | None = None,
    uf: str | None = None,
    *,
    return_meta: Literal[False] = False,
    fonte: Literal["conab", "ibge_lspa"] | None = None,
    levantamento: int | None = None,
    mes: int | str | None = None,
    as_polars: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def estimativa_safra(
    produto: str,
    safra: str | None = None,
    uf: str | None = None,
    *,
    return_meta: Literal[False] = False,
    fonte: Literal["conab", "ibge_lspa"] | None = None,
    levantamento: int | None = None,
    mes: int | str | None = None,
    as_polars: bool = False,
) -> result_utils.DataFrame: ...


@overload
async def estimativa_safra(
    produto: str,
    safra: str | None = None,
    uf: str | None = None,
    *,
    return_meta: Literal[True],
    fonte: Literal["conab", "ibge_lspa"] | None = None,
    levantamento: int | None = None,
    mes: int | str | None = None,
    as_polars: Literal[False] = False,
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def estimativa_safra(
    produto: str,
    safra: str | None = None,
    uf: str | None = None,
    *,
    return_meta: Literal[True],
    fonte: Literal["conab", "ibge_lspa"] | None = None,
    levantamento: int | None = None,
    mes: int | str | None = None,
    as_polars: bool = False,
) -> tuple[result_utils.DataFrame, MetaInfo]: ...


async def estimativa_safra(
    produto: str,
    safra: str | None = None,
    uf: str | None = None,
    *,
    return_meta: bool = False,
    fonte: Literal["conab", "ibge_lspa"] | None = None,
    levantamento: int | None = None,
    mes: int | str | None = None,
    as_polars: bool = False,
) -> result_utils.DataFrameResult:
    return await _estimativa_safra.fetch(
        produto,
        safra=safra,
        uf=uf,
        return_meta=return_meta,
        fonte=fonte,
        levantamento=levantamento,
        mes=mes,
        as_polars=as_polars,
    )
