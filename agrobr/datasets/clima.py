from __future__ import annotations

from typing import Any

import pandas as pd
import structlog

from agrobr.datasets.base import BaseDataset, DatasetInfo, DatasetSource, _unpack_result
from agrobr.datasets.deterministic import get_snapshot
from agrobr.exceptions import InvalidParameterError
from agrobr.models import MetaInfo

logger = structlog.get_logger()


async def _fetch_inmet(uf: str, **kwargs: Any) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr import inmet

    ano: int = kwargs["ano"]
    result = await inmet.clima_uf(uf, ano, return_meta=True)
    df, meta = _unpack_result(result)
    df["fonte"] = "inmet"
    for col in ("umidade_media", "radiacao_media_mj", "vento_medio_ms"):
        if col not in df.columns:
            df[col] = pd.array([pd.NA] * len(df), dtype="Float64")
    return df, meta


async def _fetch_nasa(uf: str, **kwargs: Any) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr import nasa_power

    ano: int = kwargs["ano"]
    result = await nasa_power.clima_uf(uf, ano, agregacao="mensal", return_meta=True)
    df, meta = _unpack_result(result)
    df = df.drop(columns=["lat", "lon"], errors="ignore")
    df["fonte"] = "nasa_power"
    if "num_estacoes" not in df.columns:
        df["num_estacoes"] = pd.array([pd.NA] * len(df), dtype="Int64")
    return df, meta


CLIMA_INFO = DatasetInfo(
    name="clima",
    description="Dados climáticos mensais por UF (INMET → NASA POWER) ou por estação (INMET)",
    sources=[
        DatasetSource(
            name="inmet",
            priority=1,
            fetch_fn=_fetch_inmet,
            description="INMET — estações automáticas agregadas por UF",
        ),
        DatasetSource(
            name="nasa_power",
            priority=2,
            fetch_fn=_fetch_nasa,
            description="NASA POWER — reanálise satelital por UF",
        ),
    ],
    products=[],
    contract_version="1.0",
    update_frequency="daily",
    typical_latency="D+1",
    source_url="https://portal.inmet.gov.br",
    source_institution="INMET / NASA",
    min_date="2000-01-01",
    unit="°C / mm",
    license="livre",
)


class ClimaDataset(BaseDataset):
    info = CLIMA_INFO

    def _validate_produto(self, produto: str) -> None:
        pass

    async def fetch(  # type: ignore[override]
        self,
        uf: str | None = None,
        ano: int | None = None,
        *,
        estacao: str | None = None,
        inicio: str | None = None,
        fim: str | None = None,
        agregacao: str = "diario",
        return_meta: bool = False,
        **kwargs: Any,
    ) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
        if estacao is not None:
            return await self._fetch_estacao(
                estacao, inicio=inicio, fim=fim, agregacao=agregacao, return_meta=return_meta
            )

        if uf is None:
            raise InvalidParameterError("uf é obrigatório para modo UF")

        snapshot = get_snapshot()
        if snapshot and ano is None:
            ano = int(snapshot[:4])
        if ano is None:
            from agrobr.utils.time import utcnow

            ano = utcnow().year

        logger.info("dataset_fetch", dataset="clima", uf=uf, ano=ano)

        df, source_name, source_meta, attempted = await self._try_sources(uf, ano=ano, **kwargs)

        df = self._normalize(df, uf)
        self._validate_contract(df)

        if return_meta:
            return df, self._build_meta(df, source_name, source_meta, attempted, snapshot)
        return df

    async def _fetch_estacao(
        self,
        codigo: str,
        *,
        inicio: str | None,
        fim: str | None,
        agregacao: str,
        return_meta: bool,
    ) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
        from agrobr import inmet

        if inicio is None or fim is None:
            raise InvalidParameterError("inicio e fim são obrigatórios para modo estacao")

        result = await inmet.estacao(codigo, inicio, fim, agregacao=agregacao, return_meta=True)
        df, meta = _unpack_result(result)

        snapshot = get_snapshot()

        if agregacao == "diario":
            from agrobr.contracts import has_contract, validate_dataset

            if has_contract("clima_estacao"):
                validate_dataset(df, "clima_estacao")

        if return_meta:
            return df, self._build_meta(df, "inmet", meta, ["inmet"], snapshot)
        return df

    def _normalize(self, df: pd.DataFrame, uf: str) -> pd.DataFrame:
        if "uf" in df.columns:
            df["uf"] = df["uf"].str.upper()
        else:
            df["uf"] = uf.upper()
        return df


_clima = ClimaDataset()

from agrobr.datasets.registry import register  # noqa: E402

register(_clima)


async def clima(
    uf: str | None = None,
    ano: int | None = None,
    *,
    estacao: str | None = None,
    inicio: str | None = None,
    fim: str | None = None,
    agregacao: str = "diario",
    return_meta: bool = False,
    **kwargs: Any,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    return await _clima.fetch(
        uf,
        ano,
        estacao=estacao,
        inicio=inicio,
        fim=fim,
        agregacao=agregacao,
        return_meta=return_meta,
        **kwargs,
    )
