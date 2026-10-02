from __future__ import annotations

from typing import Any, Literal, overload

import pandas as pd

from agrobr import _log
from agrobr.b3.models import B3_CONTRATOS_AGRO
from agrobr.datasets.base import BaseDataset, DatasetInfo, DatasetSource, _unpack_result
from agrobr.datasets.deterministic import get_snapshot
from agrobr.exceptions import InvalidParameterError
from agrobr.models import MetaInfo
from agrobr.utils.result import DataFrameResult
from agrobr.utils.validation import parse_data

logger = _log.get_logger(__name__)

_PRODUCTS = list(B3_CONTRATOS_AGRO.keys())


def _dias_uteis_recentes(n: int = 5) -> list[str]:
    from datetime import timedelta

    from agrobr.utils.time import utcnow

    dias: list[str] = []
    dia = utcnow().date()
    while len(dias) < n:
        if dia.weekday() < 5:
            dias.append(dia.isoformat())
        dia -= timedelta(days=1)
    return dias


async def _fetch_pregao_recente(
    fetch_fn: Any, **fetch_kwargs: Any
) -> tuple[pd.DataFrame, MetaInfo]:
    from agrobr.exceptions import ParseError, SourceUnavailableError

    last_error: Exception | None = None
    empty_result: tuple[pd.DataFrame, MetaInfo] | None = None
    for dia in _dias_uteis_recentes():
        try:
            df, meta = await fetch_fn(data=dia, return_meta=True, **fetch_kwargs)
            if not df.empty:
                return df, meta
            empty_result = (df, meta)
        except (SourceUnavailableError, ParseError) as e:
            logger.debug("b3_pregao_indisponivel", data=dia, error=str(e))
            last_error = e
    if empty_result is not None:
        return empty_result
    if last_error is not None:
        raise last_error
    raise SourceUnavailableError(source="b3", last_error="Nenhum pregão disponível na janela")


async def _fetch_b3(produto: str, **kwargs: Any) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr import b3

    tipo: str = kwargs.get("tipo", "ajustes")
    contrato = produto if produto else None
    data: str = kwargs["data"] if "data" in kwargs and kwargs["data"] else ""
    vencimento: str | None = kwargs.get("vencimento")

    if tipo in ("historico", "oi_historico"):
        if not kwargs.get("inicio") or not kwargs.get("fim"):
            raise InvalidParameterError(f"tipo='{tipo}' requer inicio e fim (YYYY-MM-DD)")
        fetch_historico = b3.posicoes_abertas_historico if tipo == "oi_historico" else b3.historico
        result = await fetch_historico(
            contrato=contrato or "",
            inicio=kwargs["inicio"],
            fim=kwargs["fim"],
            vencimento=vencimento,
            return_meta=True,
        )
    elif tipo == "posicoes":
        if data:
            result = await b3.posicoes_abertas(data=data, contrato=contrato, return_meta=True)
        else:
            result = await _fetch_pregao_recente(b3.posicoes_abertas, contrato=contrato)
    elif data:
        result = await b3.ajustes(data=data, contrato=contrato, return_meta=True)
    else:
        result = await _fetch_pregao_recente(b3.ajustes, contrato=contrato)

    return _unpack_result(result)


FUTUROS_AGRICOLAS_INFO = DatasetInfo(
    name="futuros_agricolas",
    description="Futuros agrícolas B3 — ajustes diários, histórico e posições abertas",
    sources=[
        DatasetSource(
            name="b3",
            priority=1,
            fetch_fn=_fetch_b3,
            description="B3 — Bolsa de Valores do Brasil",
        ),
    ],
    products=_PRODUCTS,
    contract_version="1.0",
    update_frequency="daily",
    typical_latency="D+1",
    source_url="https://www.b3.com.br",
    source_institution="B3",
    unit=(
        "cotação por unidade da mercadoria na coluna unidade (BRL/@, BRL/sc60kg, USD/sc60kg, "
        "BRL/m3, USD/ton); ajuste_por_contrato em BRL ou USD por contrato; posições em contratos"
    ),
    license="zona_cinza",
)


class FuturosAgricolasDataset(BaseDataset):
    info = FUTUROS_AGRICOLAS_INFO
    _modos_de_contrato = {
        "tipo='ajustes' ou 'historico'": {"tipo": "ajustes"},
        "tipo='posicoes' ou 'oi_historico'": {"tipo": "posicoes"},
    }

    def _contract_name(self, **kwargs: Any) -> str | None:
        tipo = kwargs.get("tipo", "ajustes")
        return "ajuste_diario" if tipo in ("ajustes", "historico") else "posicoes_abertas"

    def _validate_produto(self, produto: str) -> None:
        if not produto:
            return
        if produto not in self.info.products:
            raise InvalidParameterError(
                f"Produto '{produto}' não suportado por {self.info.name}. "
                f"Válidos: {self.info.products}"
            )

    @staticmethod
    def _validate_params(
        tipo: str,
        produto: str | None,
        data: str | None,
        inicio: str | None,
        fim: str | None,
    ) -> None:
        parse_data(data, "data")
        inicio_dt, fim_dt = parse_data(inicio, "inicio"), parse_data(fim, "fim")
        if tipo in ("historico", "oi_historico"):
            if not produto:
                raise InvalidParameterError(f"produto é obrigatório para tipo='{tipo}'")
            if inicio_dt is None or fim_dt is None:
                raise InvalidParameterError(f"inicio e fim são obrigatórios para tipo='{tipo}'")
            if inicio_dt > fim_dt:
                raise InvalidParameterError("inicio deve ser anterior ou igual a fim")
        if tipo in ("posicoes", "oi_historico") and produto == "soja_fob":
            raise InvalidParameterError(
                "soja_fob não possui dados de posições abertas na B3 (SOY ausente de TICKERS_AGRO_OI)"
            )

    async def fetch(  # type: ignore[override]
        self,
        produto: str | None = None,
        *,
        tipo: Literal["ajustes", "historico", "posicoes", "oi_historico"] = "ajustes",
        data: str | None = None,
        inicio: str | None = None,
        fim: str | None = None,
        vencimento: str | None = None,
        return_meta: bool = False,
    ) -> DataFrameResult:
        produto = self._produto_do_dataset(produto)
        if tipo not in ("ajustes", "historico", "posicoes", "oi_historico"):
            raise InvalidParameterError(
                f"tipo deve ser 'ajustes', 'historico', 'posicoes' ou 'oi_historico', recebeu '{tipo}'"
            )
        if tipo in ("historico", "oi_historico") and data is not None:
            raise InvalidParameterError(f"tipo='{tipo}' usa inicio e fim; omita data")
        if tipo in ("ajustes", "posicoes") and (inicio is not None or fim is not None):
            raise InvalidParameterError(
                f"tipo='{tipo}' usa data; inicio e fim valem só para 'historico' e 'oi_historico'"
            )
        if tipo in ("ajustes", "posicoes") and vencimento is not None:
            raise InvalidParameterError(
                f"tipo='{tipo}' traz todos os vencimentos do pregão; vencimento vale só para "
                "'historico' e 'oi_historico' (ou filtre a coluna vencimento_codigo)"
            )

        self._validate_params(tipo, produto, data, inicio, fim)

        snapshot = get_snapshot()
        if snapshot and tipo in ("ajustes", "posicoes") and data is None:
            data = snapshot[:10]

        logger.info("dataset_fetch", dataset="futuros_agricolas", produto=produto, tipo=tipo)

        df, source_name, source_meta, attempted = await self._try_sources(
            produto or "",
            tipo=tipo,
            data=data,
            inicio=inicio,
            fim=fim,
            vencimento=vencimento,
        )

        self._validate_contract(df, tipo=tipo)

        if return_meta:
            return df, self._build_meta(
                df,
                source_name,
                source_meta,
                attempted,
                snapshot,
                contract_name=self._contract_name(tipo=tipo),
            )
        return df


_futuros_agricolas = FuturosAgricolasDataset()

from agrobr.datasets.registry import register  # noqa: E402

register(_futuros_agricolas)


@overload
async def futuros_agricolas(
    produto: str | None = None,
    *,
    tipo: Literal["ajustes", "historico", "posicoes", "oi_historico"] = "ajustes",
    data: str | None = None,
    inicio: str | None = None,
    fim: str | None = None,
    vencimento: str | None = None,
    return_meta: Literal[False] = False,
    as_polars: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def futuros_agricolas(
    produto: str | None = None,
    *,
    tipo: Literal["ajustes", "historico", "posicoes", "oi_historico"] = "ajustes",
    data: str | None = None,
    inicio: str | None = None,
    fim: str | None = None,
    vencimento: str | None = None,
    return_meta: Literal[True],
    as_polars: Literal[False] = False,
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def futuros_agricolas(
    produto: str | None = None,
    *,
    tipo: Literal["ajustes", "historico", "posicoes", "oi_historico"] = "ajustes",
    data: str | None = None,
    inicio: str | None = None,
    fim: str | None = None,
    vencimento: str | None = None,
    return_meta: bool = False,
    as_polars: bool = False,
) -> DataFrameResult: ...


async def futuros_agricolas(
    produto: str | None = None,
    *,
    tipo: Literal["ajustes", "historico", "posicoes", "oi_historico"] = "ajustes",
    data: str | None = None,
    inicio: str | None = None,
    fim: str | None = None,
    vencimento: str | None = None,
    return_meta: bool = False,
    as_polars: bool = False,
) -> DataFrameResult:
    return await _futuros_agricolas.fetch(  # type: ignore[call-arg]
        produto,
        tipo=tipo,
        data=data,
        inicio=inicio,
        fim=fim,
        vencimento=vencimento,
        return_meta=return_meta,
        as_polars=as_polars,
    )
