from __future__ import annotations

from typing import Any, Literal, cast, overload

import pandas as pd

from agrobr import _log
from agrobr.datasets.base import BaseDataset, DatasetInfo, DatasetSource, _unpack_result
from agrobr.datasets.deterministic import get_snapshot
from agrobr.exceptions import InvalidParameterError
from agrobr.models import MetaInfo
from agrobr.utils.result import DataFrame, DataFrameResult

logger = _log.get_logger(__name__)


async def _fetch_antaq(
    produto: str,  # noqa: ARG001
    **kwargs: Any,
) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr import antaq

    result = await antaq.movimentacao(
        ano=kwargs["ano"],
        tipo_navegacao=kwargs.get("tipo_navegacao"),
        natureza_carga=kwargs.get("natureza_carga"),
        mercadoria=kwargs.get("mercadoria"),
        porto=kwargs.get("porto"),
        uf=kwargs.get("uf"),
        sentido=kwargs.get("sentido"),
        return_meta=True,
    )
    frame, meta = _unpack_result(result)
    return cast("pd.DataFrame", frame), meta


MOVIMENTACAO_PORTUARIA_INFO = DatasetInfo(
    name="movimentacao_portuaria",
    description="Movimentação portuária de cargas — ANTAQ",
    sources=[
        DatasetSource(
            name="antaq",
            priority=1,
            fetch_fn=_fetch_antaq,
            description="ANTAQ — Agência Nacional de Transportes Aquaviários",
        ),
    ],
    products=[],
    contract_version="2.0",
    update_frequency="yearly",
    typical_latency="ano+6 meses",
    source_url="https://estatistica.antaq.gov.br/ea/sense/",
    source_institution="ANTAQ",
    unit="ton",
    license="livre",
)


class MovimentacaoPortuariaDataset(BaseDataset):
    info = MOVIMENTACAO_PORTUARIA_INFO

    def _validate_produto(self, produto: str) -> None:
        pass

    async def fetch(  # type: ignore[override]
        self,
        mercadoria: str | None = None,
        *,
        ano: int,
        porto: str | None = None,
        uf: str | None = None,
        sentido: str | None = None,
        tipo_navegacao: str | None = None,
        natureza_carga: str | None = None,
        return_meta: bool = False,
    ) -> DataFrameResult:
        snapshot = get_snapshot()

        logger.info(
            "dataset_fetch",
            dataset="movimentacao_portuaria",
            ano=ano,
        )

        df, source_name, source_meta, attempted = await self._try_sources(
            "",
            ano=ano,
            mercadoria=mercadoria,
            porto=porto,
            uf=uf,
            sentido=sentido,
            tipo_navegacao=tipo_navegacao,
            natureza_carga=natureza_carga,
        )

        df = self._normalize(df)
        self._validate_contract(df)

        if return_meta:
            return df, self._build_meta(df, source_name, source_meta, attempted, snapshot)
        return df

    def _normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        pk = ["ano", "mes", "porto", "cd_mercadoria", "sentido", "tipo_navegacao"]
        df = df.dropna(subset=["ano", "mes"])

        def _single_or_none(s: pd.Series) -> Any:
            return s.iloc[0] if s.nunique(dropna=False) == 1 else None

        agg: dict[str, Any] = {}
        for col in ("peso_bruto_ton", "qt_carga", "teu"):
            if col in df.columns:
                agg[col] = "sum"
        for col in (
            "complexo_portuario",
            "municipio",
            "uf",
            "regiao",
            "mercadoria",
            "grupo_mercadoria",
        ):
            if col in df.columns:
                agg[col] = "first"
        for col in (
            "data_atracacao",
            "tipo_operacao",
            "natureza_carga",
            "terminal",
            "origem",
            "destino",
        ):
            if col in df.columns:
                agg[col] = _single_or_none

        return df.groupby(pk, dropna=False, as_index=False).agg(agg).reset_index(drop=True)


_movimentacao_portuaria = MovimentacaoPortuariaDataset()

from agrobr.datasets.registry import register  # noqa: E402

register(_movimentacao_portuaria)


@overload
async def movimentacao_portuaria(
    *,
    ano: int,
    mercadoria: str | None = None,
    porto: str | None = None,
    uf: str | None = None,
    sentido: str | None = None,
    tipo_navegacao: str | None = None,
    natureza_carga: str | None = None,
    return_meta: Literal[False] = False,
    as_polars: bool = False,
) -> DataFrame: ...


@overload
async def movimentacao_portuaria(
    *,
    ano: int,
    mercadoria: str | None = None,
    porto: str | None = None,
    uf: str | None = None,
    sentido: str | None = None,
    tipo_navegacao: str | None = None,
    natureza_carga: str | None = None,
    return_meta: Literal[True],
    as_polars: bool = False,
) -> tuple[DataFrame, MetaInfo]: ...


@overload
async def movimentacao_portuaria(
    *,
    ano: int,
    mercadoria: str | None = None,
    porto: str | None = None,
    uf: str | None = None,
    sentido: str | None = None,
    tipo_navegacao: str | None = None,
    natureza_carga: str | None = None,
    return_meta: bool = False,
    as_polars: bool = False,
) -> DataFrameResult: ...


async def movimentacao_portuaria(
    *,
    ano: int,
    mercadoria: str | None = None,
    porto: str | None = None,
    uf: str | None = None,
    sentido: str | None = None,
    tipo_navegacao: str | None = None,
    natureza_carga: str | None = None,
    return_meta: bool = False,
    as_polars: bool = False,
) -> DataFrameResult:
    if not isinstance(as_polars, bool) or not isinstance(return_meta, bool):
        raise InvalidParameterError("as_polars e return_meta devem ser booleanos")
    return await _movimentacao_portuaria.fetch(  # type: ignore[call-arg]
        ano=ano,
        mercadoria=mercadoria,
        porto=porto,
        uf=uf,
        sentido=sentido,
        tipo_navegacao=tipo_navegacao,
        natureza_carga=natureza_carga,
        return_meta=return_meta,
        as_polars=as_polars,
    )
