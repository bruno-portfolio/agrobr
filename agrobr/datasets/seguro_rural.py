from __future__ import annotations

from typing import Any, Literal, overload

import pandas as pd
import structlog

from agrobr.datasets.base import BaseDataset, DatasetInfo, DatasetSource, _unpack_result
from agrobr.datasets.deterministic import get_snapshot
from agrobr.exceptions import InvalidParameterError
from agrobr.models import MetaInfo

logger = structlog.get_logger()


async def _fetch_psr(produto: str, **kwargs: Any) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr.alt import mapa_psr

    tipo = kwargs.get("tipo", "apolices")
    evento = kwargs.get("evento")
    uf = kwargs.get("uf")
    ano = kwargs.get("ano")
    ano_inicio = kwargs.get("ano_inicio")
    ano_fim = kwargs.get("ano_fim")
    municipio = kwargs.get("municipio")
    cd_ibge = kwargs.get("cd_ibge")
    cultura = produto if produto else None

    if tipo == "sinistros":
        result = await mapa_psr.sinistros(
            cultura=cultura,
            evento=evento,
            uf=uf,
            ano=ano,
            ano_inicio=ano_inicio,
            ano_fim=ano_fim,
            municipio=municipio,
            cd_ibge=cd_ibge,
            return_meta=True,
        )
    else:
        result = await mapa_psr.apolices(
            cultura=cultura,
            uf=uf,
            ano=ano,
            ano_inicio=ano_inicio,
            ano_fim=ano_fim,
            municipio=municipio,
            cd_ibge=cd_ibge,
            return_meta=True,
        )

    return _unpack_result(result)


SEGURO_RURAL_INFO = DatasetInfo(
    name="seguro_rural",
    description="Seguro rural — apólices e sinistros (MAPA/PSR)",
    sources=[
        DatasetSource(
            name="mapa_psr",
            priority=1,
            fetch_fn=_fetch_psr,
            description="MAPA Programa de Subvenção ao Prêmio do Seguro Rural",
        ),
    ],
    products=[],
    contract_version="1.1",
    update_frequency="yearly",
    typical_latency="ano+3 meses",
    source_url="https://dados.agricultura.gov.br",
    source_institution="MAPA",
    unit="BRL / ha",
    license="livre",
)


class SeguroRuralDataset(BaseDataset):
    info = SEGURO_RURAL_INFO

    def _contract_name(self, **kwargs: Any) -> str | None:
        tipo = kwargs.get("tipo", "apolices")
        return f"mapa_psr_{tipo}"

    def _validate_produto(self, produto: str) -> None:
        pass

    async def fetch(  # type: ignore[override]
        self,
        produto: str | None = None,
        *,
        tipo: Literal["apolices", "sinistros"] = "apolices",
        uf: str | None = None,
        ano: int | None = None,
        ano_inicio: int | None = None,
        ano_fim: int | None = None,
        municipio: str | None = None,
        cd_ibge: str | None = None,
        evento: str | None = None,
        return_meta: bool = False,
    ) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
        if tipo not in ("apolices", "sinistros"):
            raise InvalidParameterError(
                f"tipo deve ser 'apolices' ou 'sinistros', recebeu '{tipo}'"
            )

        logger.info(
            "dataset_fetch",
            dataset="seguro_rural",
            produto=produto,
            tipo=tipo,
        )

        snapshot = get_snapshot()

        df, source_name, source_meta, attempted = await self._try_sources(
            produto or "",
            tipo=tipo,
            evento=evento,
            uf=uf,
            ano=ano,
            ano_inicio=ano_inicio,
            ano_fim=ano_fim,
            municipio=municipio,
            cd_ibge=cd_ibge,
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


_seguro_rural = SeguroRuralDataset()

from agrobr.datasets.registry import register  # noqa: E402

register(_seguro_rural)


@overload
async def seguro_rural(
    produto: str | None = None,
    *,
    tipo: Literal["apolices", "sinistros"] = "apolices",
    uf: str | None = None,
    ano: int | None = None,
    ano_inicio: int | None = None,
    ano_fim: int | None = None,
    municipio: str | None = None,
    cd_ibge: str | None = None,
    evento: str | None = None,
    return_meta: Literal[False] = False,
    as_polars: bool = False,
) -> pd.DataFrame: ...


@overload
async def seguro_rural(
    produto: str | None = None,
    *,
    tipo: Literal["apolices", "sinistros"] = "apolices",
    uf: str | None = None,
    ano: int | None = None,
    ano_inicio: int | None = None,
    ano_fim: int | None = None,
    municipio: str | None = None,
    cd_ibge: str | None = None,
    evento: str | None = None,
    return_meta: Literal[True],
    as_polars: bool = False,
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def seguro_rural(
    produto: str | None = None,
    *,
    tipo: Literal["apolices", "sinistros"] = "apolices",
    uf: str | None = None,
    ano: int | None = None,
    ano_inicio: int | None = None,
    ano_fim: int | None = None,
    municipio: str | None = None,
    cd_ibge: str | None = None,
    evento: str | None = None,
    return_meta: bool = False,
    as_polars: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    return await _seguro_rural.fetch(  # type: ignore[call-arg]
        produto,
        tipo=tipo,
        uf=uf,
        ano=ano,
        ano_inicio=ano_inicio,
        ano_fim=ano_fim,
        municipio=municipio,
        cd_ibge=cd_ibge,
        evento=evento,
        return_meta=return_meta,
        as_polars=as_polars,
    )
