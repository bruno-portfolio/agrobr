from __future__ import annotations

import hashlib
import time
import warnings
from typing import Literal, overload

import pandas as pd
import structlog

from agrobr.exceptions import InvalidParameterError
from agrobr.models import MetaInfo
from agrobr.utils import time as time_utils
from agrobr.utils.result import ATRIBUTO_AVISOS, build_source_meta, finalize_result
from agrobr.utils.warnings import warn_once

from . import client, models, parser

logger = structlog.get_logger()


def _marcar_ano_em_curso(df: pd.DataFrame, ano: int) -> dict[str, object]:
    if ano != time_utils.hoje().year or df.empty:
        return {}
    meses = sorted(int(mes) for mes in df["mes"].dropna().unique())
    aviso = (
        f"anda: o ano {ano} está em curso; o boletim cobre de janeiro a {meses[-1]:02d}/{ano} "
        "e muda até a edição do ano fechado."
    )
    df.attrs.setdefault(ATRIBUTO_AVISOS, []).append(aviso)
    warnings.warn(aviso, UserWarning, stacklevel=3)
    return {"ano_em_curso": ano, "meses_cobertos": meses}


@overload
async def entregas(
    ano: int,
    *,
    produto: str = "total",
    agregacao: str = "detalhado",
    as_polars: bool = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def entregas(
    ano: int,
    *,
    produto: str = "total",
    agregacao: str = "detalhado",
    as_polars: bool = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def entregas(
    ano: int,
    *,
    produto: str = "total",
    agregacao: str = "detalhado",
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    corrente = time_utils.hoje().year
    if not isinstance(ano, int) or isinstance(ano, bool) or ano > corrente:
        raise InvalidParameterError(f"ano deve ser inteiro e não pode superar {corrente}")
    produto_normalizado = models.resolve_produto(produto)
    if agregacao not in ("detalhado", "mensal"):
        raise InvalidParameterError(
            f"agregacao deve ser 'detalhado' ou 'mensal', recebido {agregacao!r}"
        )

    warn_once(
        "anda",
        "ANDA: termos de uso não encontrados publicamente. "
        "Autorização solicitada em fev/2026. Classificação: zona_cinza. "
        "Veja docs/licenses.md para detalhes.",
    )

    logger.info(
        "anda_entregas",
        ano=ano,
        produto=produto_normalizado,
        agregacao=agregacao,
    )

    t0 = time.monotonic()
    pdf_bytes, ano_real, alvo = await client.fetch_entregas_pdf(ano)
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    df = parser.parse_entregas_pdf(pdf_bytes, ano=ano_real)
    parse_ms = int((time.monotonic() - t1) * 1000)

    if agregacao == "mensal":
        df = parser.agregar_mensal(df)
    parcial = _marcar_ano_em_curso(df, ano_real)

    sha256 = hashlib.sha256(pdf_bytes).hexdigest()
    meta = build_source_meta(
        "anda",
        alvo["url"],
        "httpx+pdfplumber",
        fetch_ms,
        parse_ms,
        df,
        parser.PARSER_VERSION,
        raw_content_hash=sha256,
        raw_content_size=len(pdf_bytes),
        source_details={
            "pdf": {
                "url": alvo["url"],
                "rotulo_catalogo": alvo["text"],
                "edicao_impressa": parser.edicao_impressa(pdf_bytes),
                "sha256": sha256,
                "bytes": len(pdf_bytes),
                "pagina_de_recursos": client.ESTATISTICAS_URL,
            },
            **parcial,
        },
    )
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)
