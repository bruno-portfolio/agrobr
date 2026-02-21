from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pandas as pd
import structlog

from agrobr.datasets.base import BaseDataset, DatasetInfo, DatasetSource
from agrobr.datasets.deterministic import get_snapshot
from agrobr.models import MetaInfo

logger = structlog.get_logger()


async def _fetch_sicar(uf: str, **kwargs: Any) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr.alt import sicar

    municipio = kwargs.get("municipio")
    status = kwargs.get("status")
    tipo = kwargs.get("tipo")
    area_min = kwargs.get("area_min")
    area_max = kwargs.get("area_max")
    criado_apos = kwargs.get("criado_apos")

    result = await sicar.imoveis(
        uf,
        municipio=municipio,
        status=status,
        tipo=tipo,
        area_min=area_min,
        area_max=area_max,
        criado_apos=criado_apos,
        return_meta=True,
    )

    if isinstance(result, tuple):
        return result[0], result[1]
    return result, None


CADASTRO_RURAL_INFO = DatasetInfo(
    name="cadastro_rural",
    description="Cadastro Ambiental Rural — registros de imóveis rurais por UF",
    sources=[
        DatasetSource(
            name="sicar",
            priority=1,
            fetch_fn=_fetch_sicar,
            description="SICAR/GeoServer WFS (Serviço Florestal Brasileiro)",
        ),
    ],
    products=sorted(
        [
            "AC",
            "AL",
            "AM",
            "AP",
            "BA",
            "CE",
            "DF",
            "ES",
            "GO",
            "MA",
            "MG",
            "MS",
            "MT",
            "PA",
            "PB",
            "PE",
            "PI",
            "PR",
            "RJ",
            "RN",
            "RO",
            "RR",
            "RS",
            "SC",
            "SE",
            "SP",
            "TO",
        ]
    ),
    contract_version="1.0",
    update_frequency="continuous",
    typical_latency="D+0",
    source_url="https://geoserver.car.gov.br/geoserver/sicar/wfs",
    source_institution="Serviço Florestal Brasileiro / MMA",
    min_date="2012-01-01",
    unit="imóveis rurais",
    license="livre",
)


class CadastroRuralDataset(BaseDataset):
    info = CADASTRO_RURAL_INFO

    async def fetch(  # type: ignore[override]
        self,
        produto: str,
        municipio: str | None = None,
        status: str | None = None,
        tipo: str | None = None,
        area_min: float | None = None,
        area_max: float | None = None,
        criado_apos: str | None = None,
        return_meta: bool = False,
        **kwargs: Any,
    ) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
        logger.info(
            "dataset_fetch",
            dataset="cadastro_rural",
            produto=produto,
            municipio=municipio,
        )

        snapshot = get_snapshot()
        if snapshot and criado_apos is None:
            criado_apos = snapshot[:10]

        df, source_name, source_meta, attempted = await self._try_sources(
            produto,
            municipio=municipio,
            status=status,
            tipo=tipo,
            area_min=area_min,
            area_max=area_max,
            criado_apos=criado_apos,
            **kwargs,
        )

        self._validate_contract(df)

        if return_meta:
            now = datetime.now(UTC)
            meta = MetaInfo(
                source=f"datasets.cadastro_rural/{source_name}",
                source_url=source_meta.source_url if source_meta else "",
                source_method="dataset",
                fetched_at=source_meta.fetched_at if source_meta else now,
                records_count=len(df),
                columns=df.columns.tolist(),
                from_cache=False,
                parser_version=source_meta.parser_version if source_meta else 1,
                dataset="cadastro_rural",
                contract_version=self.info.contract_version,
                snapshot=snapshot,
                attempted_sources=attempted,
                selected_source=source_name,
                fetch_timestamp=now,
            )
            return df, meta

        return df


_cadastro_rural = CadastroRuralDataset()

from agrobr.datasets.registry import register  # noqa: E402

register(_cadastro_rural)


async def cadastro_rural(
    uf: str,
    municipio: str | None = None,
    status: str | None = None,
    tipo: str | None = None,
    area_min: float | None = None,
    area_max: float | None = None,
    criado_apos: str | None = None,
    return_meta: bool = False,
    **kwargs: Any,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    return await _cadastro_rural.fetch(
        uf,
        municipio=municipio,
        status=status,
        tipo=tipo,
        area_min=area_min,
        area_max=area_max,
        criado_apos=criado_apos,
        return_meta=return_meta,
        **kwargs,
    )
