from __future__ import annotations

import hashlib
import time
from typing import Literal, overload

import pandas as pd

from agrobr import _log
from agrobr.exceptions import InvalidParameterError
from agrobr.models import MetaInfo
from agrobr.utils.result import DataFrameResult, build_source_meta, finalize_result
from agrobr.utils.warnings import warn_once

from . import client, parser
from .models import SAFRAS_FORA_DO_LAYOUT, SAFRAS_URLS

logger = _log.get_logger(__name__)


@overload
async def ensaio_soja(
    safra: str,
    *,
    cultivar: str | None = None,
    empresa: str | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def ensaio_soja(
    safra: str,
    *,
    cultivar: str | None = None,
    empresa: str | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def ensaio_soja(
    safra: str,
    *,
    cultivar: str | None = None,
    empresa: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult: ...


async def ensaio_soja(
    safra: str,
    *,
    cultivar: str | None = None,
    empresa: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult:
    if not isinstance(safra, str):
        raise InvalidParameterError(
            f"safra deve ser texto, recebido {safra!r}. Opções: {sorted(SAFRAS_URLS)}"
        )
    if safra in SAFRAS_FORA_DO_LAYOUT:
        raise InvalidParameterError(
            f"Safra {safra!r} publicada pela Fundação Rio Verde num layout que o agrobr não lê "
            f"({SAFRAS_FORA_DO_LAYOUT[safra]}). Opções: {sorted(SAFRAS_URLS)}"
        )
    if safra not in SAFRAS_URLS:
        raise InvalidParameterError(
            f"Safra {safra!r} não disponível. Opções: {sorted(SAFRAS_URLS)}"
        )
    warn_once(
        "rio_verde",
        (
            "Fundação Rio Verde: fonte privada sem licença de reutilização dos resultados "
            "localizada. Classificação: zona_cinza. Preserve a atribuição; ela não substitui "
            "eventual permissão necessária. Veja https://www.agrobr.dev/docs/licenses/."
        ),
    )
    logger.info("rio_verde_ensaio_soja", safra=safra, cultivar=cultivar, empresa=empresa)

    t0 = time.monotonic()
    raw, source_url = await client.fetch_ensaio_soja(safra)
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    df = parser.parse_ensaio_soja(raw, safra)
    parse_ms = int((time.monotonic() - t1) * 1000)

    if cultivar is not None:
        df = df[df["cultivar"].str.contains(cultivar, case=False, na=False, regex=False)]
    if empresa is not None:
        df = df[df["empresa"].str.contains(empresa, case=False, na=False, regex=False)]

    df = df.reset_index(drop=True)

    meta = build_source_meta(
        "rio_verde",
        source_url,
        "httpx+pdf",
        fetch_ms,
        parse_ms,
        df,
        parser.PARSER_VERSION,
        raw_content_hash=hashlib.sha256(raw).hexdigest(),
        raw_content_size=len(raw),
    )
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


async def safras_disponiveis() -> list[str]:
    return list(SAFRAS_URLS.keys())
