from __future__ import annotations

from typing import Any, Literal

import pandas as pd
import structlog

from agrobr.b3.models import B3_CONTRATOS_AGRO
from agrobr.datasets.base import BaseDataset, DatasetInfo, DatasetSource, _unpack_result
from agrobr.datasets.deterministic import get_snapshot
from agrobr.exceptions import InvalidParameterError
from agrobr.models import MetaInfo

logger = structlog.get_logger()

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
    for dia in _dias_uteis_recentes():
        try:
            return await fetch_fn(data=dia, return_meta=True, **fetch_kwargs)  # type: ignore[no-any-return]
        except (SourceUnavailableError, ParseError) as e:
            logger.debug("b3_pregao_indisponivel", data=dia, error=str(e))
            last_error = e
    raise last_error  # type: ignore[misc]


async def _fetch_b3(produto: str, **kwargs: Any) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr import b3

    tipo: str = kwargs.get("tipo", "ajustes")
    contrato = produto if produto else None
    data: str = kwargs["data"] if "data" in kwargs and kwargs["data"] else ""
    vencimento: str | None = kwargs.get("vencimento")

    if tipo == "historico":
        if not kwargs.get("inicio") or not kwargs.get("fim"):
            raise InvalidParameterError("tipo='historico' requer inicio e fim (YYYY-MM-DD)")
        result = await b3.historico(
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
    unit="BRL ou USD por contrato",
    license="zona_cinza",
)


class FuturosAgricolasDataset(BaseDataset):
    info = FUTUROS_AGRICOLAS_INFO

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
        inicio: str | None,
        fim: str | None,
    ) -> None:
        if tipo == "historico":
            if not produto:
                raise InvalidParameterError("produto é obrigatório para tipo='historico'")
            if not inicio or not fim:
                raise InvalidParameterError("inicio e fim são obrigatórios para tipo='historico'")
        if tipo == "posicoes" and produto == "soja_fob":
            raise InvalidParameterError(
                "soja_fob não possui dados de posições abertas na B3 (SOY ausente de TICKERS_AGRO_OI)"
            )

    async def fetch(  # type: ignore[override]
        self,
        produto: str | None = None,
        *,
        tipo: Literal["ajustes", "historico", "posicoes"] = "ajustes",
        data: str | None = None,
        inicio: str | None = None,
        fim: str | None = None,
        vencimento: str | None = None,
        return_meta: bool = False,
        **kwargs: Any,
    ) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
        if tipo not in ("ajustes", "historico", "posicoes"):
            raise InvalidParameterError(
                f"tipo deve ser 'ajustes', 'historico' ou 'posicoes', recebeu '{tipo}'"
            )

        self._validate_params(tipo, produto, inicio, fim)

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
            **kwargs,
        )

        df = self._normalize(df)

        from agrobr.contracts import has_contract, validate_dataset

        contract_key = "ajuste_diario" if tipo in ("ajustes", "historico") else "posicoes_abertas"
        if has_contract(contract_key):
            validate_dataset(df, contract_key)

        if return_meta:
            return df, self._build_meta(df, source_name, source_meta, attempted, snapshot)
        return df

    def _normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        return df


_futuros_agricolas = FuturosAgricolasDataset()

from agrobr.datasets.registry import register  # noqa: E402

register(_futuros_agricolas)


async def futuros_agricolas(
    produto: str | None = None,
    *,
    tipo: Literal["ajustes", "historico", "posicoes"] = "ajustes",
    data: str | None = None,
    inicio: str | None = None,
    fim: str | None = None,
    vencimento: str | None = None,
    return_meta: bool = False,
    **kwargs: Any,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    return await _futuros_agricolas.fetch(
        produto,
        tipo=tipo,
        data=data,
        inicio=inicio,
        fim=fim,
        vencimento=vencimento,
        return_meta=return_meta,
        **kwargs,
    )
