from __future__ import annotations

from typing import Any, Literal

import pandas as pd
import structlog

from agrobr.datasets.base import BaseDataset, DatasetInfo, DatasetSource, _unpack_result
from agrobr.datasets.deterministic import get_snapshot
from agrobr.exceptions import InvalidParameterError
from agrobr.models import MetaInfo

logger = structlog.get_logger()


async def _fetch_mapbiomas(
    produto: str,  # noqa: ARG001
    **kwargs: Any,
) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr import mapbiomas

    tipo: str = kwargs.get("tipo", "cobertura")
    bioma = kwargs.get("bioma")
    estado = kwargs.get("estado")

    if tipo == "cobertura":
        result = await mapbiomas.cobertura(
            bioma=bioma,
            estado=estado,
            ano=kwargs.get("ano"),
            classe_id=kwargs.get("classe_id"),
            nivel=kwargs.get("nivel", "estado"),
            municipio=kwargs.get("municipio"),
            return_meta=True,
        )
    else:
        result = await mapbiomas.transicao(
            bioma=bioma,
            estado=estado,
            periodo=kwargs.get("periodo"),
            classe_de_id=kwargs.get("classe_de_id"),
            classe_para_id=kwargs.get("classe_para_id"),
            return_meta=True,
        )
    return _unpack_result(result)


USO_DO_SOLO_INFO = DatasetInfo(
    name="uso_do_solo",
    description="Cobertura e uso da terra (MapBiomas) — cobertura anual e transições entre classes",
    sources=[
        DatasetSource(
            name="mapbiomas",
            priority=1,
            fetch_fn=_fetch_mapbiomas,
            description="MapBiomas — Mapeamento anual de cobertura e uso da terra",
        ),
    ],
    products=[],
    contract_version="1.0",
    update_frequency="yearly",
    typical_latency="ano+6 meses",
    source_url="https://brasil.mapbiomas.org",
    source_institution="MapBiomas",
    unit="ha",
    license="livre",
)


class UsodoSoloDataset(BaseDataset):
    info = USO_DO_SOLO_INFO

    def _validate_produto(self, produto: str) -> None:
        pass

    async def fetch(  # type: ignore[override]
        self,
        tipo: Literal["cobertura", "transicao"] = "cobertura",
        *,
        bioma: str | None = None,
        estado: str | None = None,
        ano: int | None = None,
        classe_id: int | None = None,
        nivel: str = "estado",
        municipio: str | None = None,
        periodo: str | None = None,
        classe_de_id: int | None = None,
        classe_para_id: int | None = None,
        return_meta: bool = False,
        **kwargs: Any,
    ) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
        if tipo not in ("cobertura", "transicao"):
            raise InvalidParameterError(
                f"tipo deve ser 'cobertura' ou 'transicao', recebeu '{tipo}'"
            )

        snapshot = get_snapshot()

        logger.info(
            "dataset_fetch",
            dataset="uso_do_solo",
            tipo=tipo,
            bioma=bioma,
        )

        df, source_name, source_meta, attempted = await self._try_sources(
            "",
            tipo=tipo,
            bioma=bioma,
            estado=estado,
            ano=ano,
            classe_id=classe_id,
            nivel=nivel,
            municipio=municipio,
            periodo=periodo,
            classe_de_id=classe_de_id,
            classe_para_id=classe_para_id,
            **kwargs,
        )

        df = self._normalize(df)

        from agrobr.contracts import has_contract, validate_dataset

        contract_key = f"mapbiomas_{tipo}"
        if nivel == "estado" and has_contract(contract_key):
            validate_dataset(df, contract_key)

        if return_meta:
            return df, self._build_meta(df, source_name, source_meta, attempted, snapshot)
        return df

    def _normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        return df


_uso_do_solo = UsodoSoloDataset()

from agrobr.datasets.registry import register  # noqa: E402

register(_uso_do_solo)


async def uso_do_solo(
    *,
    tipo: Literal["cobertura", "transicao"] = "cobertura",
    bioma: str | None = None,
    estado: str | None = None,
    ano: int | None = None,
    classe_id: int | None = None,
    nivel: str = "estado",
    municipio: str | None = None,
    periodo: str | None = None,
    classe_de_id: int | None = None,
    classe_para_id: int | None = None,
    return_meta: bool = False,
    **kwargs: Any,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    return await _uso_do_solo.fetch(
        tipo=tipo,
        bioma=bioma,
        estado=estado,
        ano=ano,
        classe_id=classe_id,
        nivel=nivel,
        municipio=municipio,
        periodo=periodo,
        classe_de_id=classe_de_id,
        classe_para_id=classe_para_id,
        return_meta=return_meta,
        **kwargs,
    )
