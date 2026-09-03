from __future__ import annotations

from typing import Any, Literal

import pandas as pd
import structlog

from agrobr.datasets.base import BaseDataset, DatasetInfo, DatasetSource, _unpack_result
from agrobr.datasets.deterministic import get_snapshot
from agrobr.exceptions import InvalidParameterError
from agrobr.models import MetaInfo
from agrobr.normalize.regions import BIOMAS_VALIDOS, normalizar_bioma

logger = structlog.get_logger()

PRODUCTS = sorted(BIOMAS_VALIDOS)

_DETER_BIOMAS = frozenset({"Amazônia", "Cerrado"})


async def _fetch_desmatamento(bioma: str, **kwargs: Any) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr import desmatamento as desmatamento_api

    tipo: str = kwargs.get("tipo", "prodes")
    if tipo == "prodes":
        result = await desmatamento_api.prodes(
            bioma=bioma,
            ano=kwargs.get("ano"),
            uf=kwargs.get("uf"),
            return_meta=True,
        )
    else:
        result = await desmatamento_api.deter(
            bioma=bioma,
            uf=kwargs.get("uf"),
            data_inicio=kwargs.get("data_inicio"),
            data_fim=kwargs.get("data_fim"),
            classe=kwargs.get("classe"),
            return_meta=True,
        )
    return _unpack_result(result)


DESMATAMENTO_INFO = DatasetInfo(
    name="desmatamento",
    description="Desmatamento consolidado (PRODES) e alertas (DETER) por bioma — INPE/TerraBrasilis",
    sources=[
        DatasetSource(
            name="inpe",
            priority=1,
            fetch_fn=_fetch_desmatamento,
            description="INPE TerraBrasilis — PRODES e DETER",
        ),
    ],
    products=PRODUCTS,
    contract_version="1.0",
    update_frequency="monthly",
    typical_latency="D+30",
    source_url="https://terrabrasilis.dpi.inpe.br",
    source_institution="INPE",
    unit="km²",
    license="livre",
)


class DesmatamentoDataset(BaseDataset):
    info = DESMATAMENTO_INFO

    @staticmethod
    def _validate_params(tipo: str, bioma: str) -> None:
        if tipo == "deter" and bioma not in _DETER_BIOMAS:
            raise InvalidParameterError(
                f"DETER só está disponível para Amazônia e Cerrado, recebeu '{bioma}'"
            )

    async def fetch(  # type: ignore[override]
        self,
        bioma: str = "Cerrado",
        *,
        tipo: Literal["prodes", "deter"] = "prodes",
        ano: int | None = None,
        uf: str | None = None,
        data_inicio: str | None = None,
        data_fim: str | None = None,
        classe: str | None = None,
        return_meta: bool = False,
        **kwargs: Any,
    ) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
        if tipo not in ("prodes", "deter"):
            raise InvalidParameterError(f"tipo deve ser 'prodes' ou 'deter', recebeu '{tipo}'")

        bioma = normalizar_bioma(bioma)
        self._validate_params(tipo, bioma)

        snapshot = get_snapshot()

        logger.info(
            "dataset_fetch",
            dataset="desmatamento",
            bioma=bioma,
            tipo=tipo,
        )

        df, source_name, source_meta, attempted = await self._try_sources(
            bioma,
            tipo=tipo,
            ano=ano,
            uf=uf,
            data_inicio=data_inicio,
            data_fim=data_fim,
            classe=classe,
            **kwargs,
        )

        df = self._normalize(df, tipo)

        from agrobr.contracts import has_contract, validate_dataset

        contract_key = f"desmatamento_{tipo}"
        if has_contract(contract_key):
            validate_dataset(df, contract_key)

        if return_meta:
            return df, self._build_meta(df, source_name, source_meta, attempted, snapshot)
        return df

    def _normalize(self, df: pd.DataFrame, tipo: str) -> pd.DataFrame:
        if df.empty:
            return df

        if tipo == "prodes":
            pk = ["ano", "uf", "classe", "bioma"]
            df = df.dropna(subset=["ano"])
        else:
            pk = ["data", "classe", "uf", "municipio", "bioma"]
            df = df.dropna(subset=["data"])

        output_order = list(df.columns)

        def _single_or_none(s: pd.Series) -> str | None:
            return s.iloc[0] if s.nunique(dropna=False) == 1 else None

        agg: dict[str, Any] = {"area_km2": "sum"}
        if "municipio_id" in df.columns:
            agg["municipio_id"] = "first"
        for col in ("satelite", "sensor"):
            if col in df.columns:
                agg[col] = _single_or_none

        out = df.groupby(pk, dropna=False, as_index=False).agg(agg)
        return out[[c for c in output_order if c in out.columns]].reset_index(drop=True)


_desmatamento = DesmatamentoDataset()

from agrobr.datasets.registry import register  # noqa: E402

register(_desmatamento)


async def desmatamento(
    bioma: str = "Cerrado",
    *,
    tipo: Literal["prodes", "deter"] = "prodes",
    ano: int | None = None,
    uf: str | None = None,
    data_inicio: str | None = None,
    data_fim: str | None = None,
    classe: str | None = None,
    return_meta: bool = False,
    **kwargs: Any,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    return await _desmatamento.fetch(
        bioma,
        tipo=tipo,
        ano=ano,
        uf=uf,
        data_inicio=data_inicio,
        data_fim=data_fim,
        classe=classe,
        return_meta=return_meta,
        **kwargs,
    )
