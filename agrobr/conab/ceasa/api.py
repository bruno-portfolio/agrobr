from __future__ import annotations

import asyncio
import time
import warnings
from datetime import UTC, datetime
from typing import Any, Literal, overload

import pandas as pd
import structlog

from agrobr.models import MetaInfo

from . import client, parser
from .models import CATEGORIAS, CEASA_UF_MAP, PRODUTOS_PROHORT

_WARNED = False

logger = structlog.get_logger()


@overload
async def precos(
    *,
    produto: str | None = None,
    ceasa: str | None = None,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def precos(
    *,
    produto: str | None = None,
    ceasa: str | None = None,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def precos(
    *,
    produto: str | None = None,
    ceasa: str | None = None,
    return_meta: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    global _WARNED  # noqa: PLW0603
    if not _WARNED:
        warnings.warn(
            "agrobr.conab.ceasa: dados CONAB/PROHORT via Pentaho CDA. "
            "Credenciais publicas embutidas no frontend, mas API nao e "
            "oficialmente documentada. Classificacao: zona_cinza. "
            "Veja docs/licenses.md.",
            UserWarning,
            stacklevel=2,
        )
        _WARNED = True

    logger.info("conab_ceasa_precos", produto=produto, ceasa=ceasa)

    t0 = time.monotonic()
    (precos_json, source_url), (ceasas_json, _) = await asyncio.gather(
        client.fetch_precos(),
        client.fetch_ceasas(),
    )
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    df = parser.parse_precos(precos_json, ceasas_json)
    parse_ms = int((time.monotonic() - t1) * 1000)

    if produto is not None:
        produto_upper = produto.strip().upper()
        df = df[df["produto"].str.upper() == produto_upper].reset_index(drop=True)

    if ceasa is not None:
        ceasa_upper = ceasa.strip().upper()
        df = df[df["ceasa"].str.upper().str.contains(ceasa_upper, regex=False)].reset_index(
            drop=True
        )

    if return_meta:
        meta = MetaInfo(
            source="conab_ceasa",
            source_url=source_url,
            source_method="httpx+json",
            fetched_at=datetime.now(UTC),
            fetch_duration_ms=fetch_ms,
            parse_duration_ms=parse_ms,
            records_count=len(df),
            columns=df.columns.tolist(),
            parser_version=parser.PARSER_VERSION,
            schema_version="1.0",
            attempted_sources=["conab_prohort"],
            selected_source="conab_prohort",
            fetch_timestamp=datetime.now(UTC),
        )
        return df, meta

    return df


def produtos() -> list[str]:
    return sorted(PRODUTOS_PROHORT)


def lista_ceasas() -> list[dict[str, str]]:
    result = []
    for nome, uf in sorted(CEASA_UF_MAP.items()):
        result.append({"nome": nome, "uf": uf})
    return result


def categorias() -> dict[str, list[str]]:
    return {k: list(v) for k, v in CATEGORIAS.items()}
