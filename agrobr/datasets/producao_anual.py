from __future__ import annotations

from datetime import date
from typing import Any, Literal, overload

import pandas as pd

from agrobr import _log, constants
from agrobr.datasets.base import BaseDataset, DatasetInfo, DatasetSource, _unpack_result
from agrobr.datasets.deterministic import get_snapshot
from agrobr.exceptions import InvalidParameterError, SourceUnavailableError
from agrobr.ibge._helpers import SIDRA_BASE
from agrobr.models import MetaInfo
from agrobr.normalize import regions
from agrobr.normalize.dates import anos_para_safra, safra_para_anos
from agrobr.normalize.regions import uf_para_nome
from agrobr.utils.time import hoje
from agrobr.utils.validation import validate_uf

logger = _log.get_logger(__name__)

_PRODUCAO_ANUAL_COLS = [
    "ano",
    "localidade",
    "produto",
    "area_plantada",
    "area_colhida",
    "producao",
    "rendimento",
    "valor_producao",
    "fonte",
]


def _hoje() -> date:
    return hoje()


async def _fetch_ibge_pam(produto: str, **kwargs: Any) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr import ibge

    ano = kwargs.get("ano")
    nivel = kwargs.get("nivel", "uf")
    uf = kwargs.get("uf")

    result = await ibge.pam(produto, ano=ano, nivel=nivel, uf=uf, return_meta=True)

    df, meta = _unpack_result(result)
    for column in ("area_plantada", "valor_producao"):
        if column not in df.columns:
            df[column] = pd.Series(pd.NA, index=df.index, dtype="Float64")
            logger.info("ibge_pam_coluna_historica_ausente", coluna=column, ano=ano)
    return df, meta


def _aggregate_conab_brasil(df: pd.DataFrame, produto: str) -> pd.DataFrame:
    area_plantada = df["area_plantada"].sum(min_count=1)
    producao = df["producao"].sum(min_count=1)
    rendimento = (
        producao * 1000 / area_plantada
        if pd.notna(producao) and pd.notna(area_plantada) and area_plantada
        else pd.NA
    )

    result = pd.DataFrame(
        [
            {
                "ano": df["ano"].iloc[0],
                "localidade": "Brasil",
                "produto": produto,
                "area_plantada": area_plantada,
                "area_colhida": pd.NA,
                "producao": producao,
                "rendimento": rendimento,
                "valor_producao": pd.NA,
                "fonte": "conab",
            }
        ]
    )
    result[["area_colhida", "valor_producao"]] = result[["area_colhida", "valor_producao"]].astype(
        "Float64"
    )
    return result[_PRODUCAO_ANUAL_COLS]


def _normalize_conab(df: pd.DataFrame, produto: str, nivel: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=_PRODUCAO_ANUAL_COLS)

    result = pd.DataFrame(index=df.index)
    result["ano"] = df["safra"].map(lambda value: safra_para_anos(str(value))[1]).astype("Int64")
    result["localidade"] = df["uf"].map(lambda value: uf_para_nome(str(value)))
    result["produto"] = produto

    source_columns = {
        "area_plantada": ("area_plantada", 1000.0),
        "producao": ("producao", 1000.0),
        "rendimento": ("produtividade", 1.0),
    }
    for target, (source, multiplier) in source_columns.items():
        if source in df.columns:
            result[target] = pd.to_numeric(df[source], errors="coerce") * multiplier
        else:
            result[target] = pd.Series(pd.NA, index=df.index, dtype="Float64")

    for column in ("area_colhida", "valor_producao"):
        result[column] = pd.Series(pd.NA, index=df.index, dtype="Float64")
    result["fonte"] = "conab"

    if nivel == "brasil":
        return _aggregate_conab_brasil(result, produto)
    return result[_PRODUCAO_ANUAL_COLS].reset_index(drop=True)


async def _fetch_conab(produto: str, **kwargs: Any) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr import conab

    if produto not in constants.CONAB_PRODUTOS:
        raise SourceUnavailableError(
            source="conab",
            last_error=f"CONAB Safras nao oferece o produto {produto!r}",
        )

    ano = kwargs.get("ano")
    nivel = kwargs.get("nivel", "uf")
    uf = kwargs.get("uf")

    if nivel == "municipio":
        raise SourceUnavailableError(
            source="conab",
            last_error="CONAB Safras nao oferece granularidade municipal",
        )

    ano = int(ano) if ano is not None else _hoje().year - 1
    safra = anos_para_safra(ano - 1)
    conab_uf = uf if nivel == "uf" else None
    result = await conab.safras(produto, safra=safra, uf=conab_uf, return_meta=True)

    df, meta = _unpack_result(result)
    df = _normalize_conab(df, produto, nivel)
    if df.empty:
        raise SourceUnavailableError(
            source="conab",
            last_error=f"CONAB sem dados de {produto}" + (f" na safra {safra}" if safra else ""),
        )
    df["unidade_producao"] = "ton"
    df["unidade_rendimento"] = "kg/ha"
    df["unidade_valor_producao"] = pd.Series(pd.NA, index=df.index, dtype=object)
    df["condicao_produto"] = pd.Series(pd.NA, index=df.index, dtype=object)
    return df, meta


PRODUCAO_ANUAL_INFO = DatasetInfo(
    name="producao_anual",
    description="Produção agrícola anual consolidada por UF ou município",
    sources=[
        DatasetSource(
            name="ibge_pam",
            priority=1,
            fetch_fn=_fetch_ibge_pam,
            description="IBGE Produção Agrícola Municipal",
        ),
        DatasetSource(
            name="conab",
            priority=2,
            fetch_fn=_fetch_conab,
            description="CONAB Safras",
        ),
    ],
    products=[
        "soja",
        "milho",
        "arroz",
        "feijao",
        "trigo",
        "algodao",
        "cafe",
        "cacau",
        "cana",
        "mandioca",
        "laranja",
    ],
    contract_version="2.2",
    update_frequency="yearly",
    typical_latency="Y+1",
    source_url=SIDRA_BASE,
    source_institution="IBGE",
    min_date="1974-01-01",
    unit="ha / unidade por produto e período",
    license="livre",
)


class ProducaoAnualDataset(BaseDataset):
    info = PRODUCAO_ANUAL_INFO

    async def fetch(  # type: ignore[override]
        self,
        produto: str,
        ano: int | list[int] | None = None,
        nivel: Literal["brasil", "uf", "municipio"] = "uf",
        uf: str | None = None,
        return_meta: bool = False,
    ) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
        logger.info("dataset_fetch", dataset="producao_anual", produto=produto, ano=ano)

        if nivel not in ("brasil", "uf", "municipio"):
            raise InvalidParameterError(
                f"nível inválido: {nivel!r}. Use: 'brasil', 'uf' ou 'municipio'"
            )

        uf = validate_uf(uf)

        snapshot = get_snapshot()
        if snapshot and ano is None:
            ano = int(snapshot[:4]) - 1

        df, source_name, source_meta, attempted = await self._try_sources(
            produto, ano=ano, nivel=nivel, uf=uf
        )

        df = self._normalize(df, produto)
        df = df.assign(
            cod_municipio=regions.cod_municipio(df["localidade_cod"])
            if "localidade_cod" in df
            else pd.Series(pd.NA, index=df.index, dtype="Int64")
        )
        self._validate_contract(df)

        if return_meta:
            return df, self._build_meta(df, source_name, source_meta, attempted, snapshot)

        return df

    def _normalize(self, df: pd.DataFrame, produto: str) -> pd.DataFrame:
        if "produto" not in df.columns:
            df["produto"] = produto

        if "fonte" not in df.columns:
            df["fonte"] = "ibge_pam"

        return df


_producao_anual = ProducaoAnualDataset()

from agrobr.datasets.registry import register  # noqa: E402

register(_producao_anual)


@overload
async def producao_anual(
    produto: str,
    ano: int | list[int] | None = None,
    nivel: Literal["brasil", "uf", "municipio"] = "uf",
    uf: str | None = None,
    *,
    return_meta: Literal[False] = False,
    as_polars: bool = False,
) -> pd.DataFrame: ...


@overload
async def producao_anual(
    produto: str,
    ano: int | list[int] | None = None,
    nivel: Literal["brasil", "uf", "municipio"] = "uf",
    uf: str | None = None,
    *,
    return_meta: Literal[True],
    as_polars: bool = False,
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def producao_anual(
    produto: str,
    ano: int | list[int] | None = None,
    nivel: Literal["brasil", "uf", "municipio"] = "uf",
    uf: str | None = None,
    return_meta: bool = False,
    as_polars: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    return await _producao_anual.fetch(  # type: ignore[call-arg]
        produto, ano=ano, nivel=nivel, uf=uf, return_meta=return_meta, as_polars=as_polars
    )
