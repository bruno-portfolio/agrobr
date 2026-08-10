from __future__ import annotations

from datetime import date
from typing import Any

import pandas as pd
import structlog

from agrobr.datasets.base import BaseDataset, DatasetInfo, DatasetSource, _unpack_result
from agrobr.datasets.deterministic import get_snapshot
from agrobr.models import MetaInfo
from agrobr.normalize.dates import anos_para_safra, month_to_number

logger = structlog.get_logger()

_LSPA_VARIAVEIS: frozenset[str] = frozenset({"Área plantada", "Área colhida", "Produção"})

_SAFRA_OUTPUT_COLS: list[str] = [
    "fonte",
    "produto",
    "safra",
    "uf",
    "area_plantada",
    "area_colhida",
    "produtividade",
    "producao",
    "levantamento",
    "data_publicacao",
]


async def _fetch_conab(produto: str, **kwargs: Any) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr import conab

    safra = kwargs.get("safra")
    uf = kwargs.get("uf")

    result = await conab.safras(produto, safra=safra, uf=uf, return_meta=True)

    return _unpack_result(result)


def _normalize_lspa(df: pd.DataFrame, produto: str, safra: str, uf: str | None) -> pd.DataFrame:
    """Converte a resposta SIDRA do LSPA para o schema CONAB_SAFRA_V1.

    O SIDRA entrega série mensal em formato longo com os eixos cruzados
    (``classificacao`` = variável, ``localidade`` = unidade, ``variavel`` = mês).
    Reduz ao levantamento mais recente, soma as sub-safras que o LSPA separa
    (milho 1ª/2ª, algodão pluma/caroço) e converte Hectares/Toneladas para
    mil_ha/mil_ton. Produtividade é recalculada como produção/área colhida para
    não depender do rendimento por sub-safra. ``levantamento`` e
    ``data_publicacao`` não existem no LSPA e ficam nulos.
    """
    if df.empty or "classificacao" not in df.columns:
        return pd.DataFrame(columns=_SAFRA_OUTPUT_COLS)

    df = df[df["classificacao"].isin(_LSPA_VARIAVEIS)].copy()
    df["_mes"] = df["variavel"].str.split().str[0].map(month_to_number)
    df = df.dropna(subset=["_mes", "valor"])
    if df.empty:
        return pd.DataFrame(columns=_SAFRA_OUTPUT_COLS)

    df = df[df["_mes"] == df["_mes"].max()]

    def _soma(classificacao: str) -> float | None:
        valores = df.loc[df["classificacao"] == classificacao, "valor"]
        return float(valores.sum(min_count=1)) if not valores.empty else None

    area_plantada = _soma("Área plantada")
    area_colhida = _soma("Área colhida")
    producao = _soma("Produção")
    produtividade = (
        producao * 1000 / area_colhida if producao is not None and area_colhida else None
    )

    registro = {
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
    }
    return pd.DataFrame([registro], columns=_SAFRA_OUTPUT_COLS)


async def _fetch_ibge_lspa(produto: str, **kwargs: Any) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr import ibge

    safra = kwargs.get("safra")
    uf = kwargs.get("uf")
    ano = int(safra.split("/")[0]) if safra else date.today().year

    result = await ibge.lspa(produto, ano=ano, uf=uf, return_meta=True)
    df, meta = _unpack_result(result)
    df = _normalize_lspa(df, produto, safra or anos_para_safra(ano), uf)
    return df, meta


ESTIMATIVA_SAFRA_INFO = DatasetInfo(
    name="estimativa_safra",
    description="Estimativas de safra corrente por UF",
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
    contract_version="1.0",
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
        return_meta: bool = False,
        **kwargs: Any,
    ) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
        logger.info("dataset_fetch", dataset="estimativa_safra", produto=produto, safra=safra)

        snapshot = get_snapshot()

        df, source_name, source_meta, attempted = await self._try_sources(
            produto, safra=safra, uf=uf, **kwargs
        )

        df = self._normalize(df, produto)
        self._validate_contract(df)

        if return_meta:
            return df, self._build_meta(df, source_name, source_meta, attempted, snapshot)

        return df

    def _normalize(self, df: pd.DataFrame, produto: str) -> pd.DataFrame:
        if "produto" not in df.columns:
            df["produto"] = produto

        if "fonte" not in df.columns:
            df["fonte"] = "conab"

        return df


_estimativa_safra = EstimativaSafraDataset()

from agrobr.datasets.registry import register  # noqa: E402

register(_estimativa_safra)


async def estimativa_safra(
    produto: str,
    safra: str | None = None,
    uf: str | None = None,
    return_meta: bool = False,
    **kwargs: Any,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    return await _estimativa_safra.fetch(
        produto, safra=safra, uf=uf, return_meta=return_meta, **kwargs
    )
