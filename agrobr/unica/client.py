from __future__ import annotations

import re

import httpx

from agrobr import _log
from agrobr.constants import MIN_PDF_SIZE, URLS, Fonte
from agrobr.exceptions import ParseError, SourceUnavailableError
from agrobr.http import responses
from agrobr.http.retry import retry_on_status
from agrobr.http.settings import get_timeout
from agrobr.http.user_agents import UserAgentRotator
from agrobr.normalize.encoding import detect_encoding_chain

from .models import (
    IDTABELA_HISTORICO_PRODUTO,
    PARSER_VERSION,
    UFS_FORM,
    validate_safras_historico,
)

logger = _log.get_logger(__name__)

TIMEOUT = get_timeout(read=90.0)

PDF_URL_RE = re.compile(r"arquivos/pdfs/\d{4}/\d{2}/[0-9a-f]{32}\.pdf")

XLSX_MAGIC = b"PK\x03\x04"

_pdf_cache: tuple[str, bytes] | None = None


async def fetch_quinzenal_pdf() -> tuple[bytes, str]:
    """Cache de 1 entrada por URL do PDF: edição publicada em outra URL força o
    download (o nome do arquivo na URL não é o md5 do conteúdo)."""
    global _pdf_cache
    page_url = URLS[Fonte.UNICA]["quinzenal_page"]

    logger.info("unica_quinzenal_page_request", url=page_url)

    async with httpx.AsyncClient(
        timeout=TIMEOUT, headers=UserAgentRotator.get_headers("unica"), follow_redirects=True
    ) as client:
        response = await retry_on_status(lambda: client.get(page_url), source="unica")
        responses.raise_for_status(response, source="unica")
        html = response.content.decode(detect_encoding_chain(response.content), errors="replace")

        match = PDF_URL_RE.search(html)
        if not match:
            raise ParseError(
                source="unica",
                parser_version=PARSER_VERSION,
                reason="URL do PDF quinzenal não encontrada na página",
                html_snippet=html[:500],
            )

        pdf_url = f"{URLS[Fonte.UNICA]['base']}/{match.group(0)}"

        if _pdf_cache is not None and _pdf_cache[0] == pdf_url:
            logger.debug("unica_quinzenal_pdf_cache_hit", url=pdf_url)
            return _pdf_cache[1], pdf_url

        logger.info("unica_quinzenal_pdf_request", url=pdf_url)

        pdf_response = await retry_on_status(lambda: client.get(pdf_url), source="unica")
        responses.raise_for_status(pdf_response, source="unica")
        content = pdf_response.content

    if len(content) < MIN_PDF_SIZE or not content.startswith(b"%PDF"):
        raise SourceUnavailableError(
            source="unica",
            url=pdf_url,
            last_error=f"PDF inválido ({len(content)} bytes)",
        )

    _pdf_cache = (pdf_url, content)
    logger.info("unica_quinzenal_pdf_ok", bytes=len(content))
    return content, pdf_url


async def fetch_historico_xlsx(
    produto: str,
    safra_inicio: str | None = None,
    safra_fim: str | None = None,
) -> tuple[bytes, str]:
    safra_inicio, safra_fim = validate_safras_historico(safra_inicio, safra_fim)

    url = URLS[Fonte.UNICA]["historico_xls"]
    params = {
        "idioma": "1",
        "tipoHistorico": "2",
        "idTabela": IDTABELA_HISTORICO_PRODUTO,
        "produto": produto,
        "safra": "",
        "safraIni": safra_inicio,
        "safraFim": safra_fim,
        "estado": ",".join(UFS_FORM),
    }

    logger.info(
        "unica_historico_request", produto=produto, safra_inicio=safra_inicio, safra_fim=safra_fim
    )

    async with httpx.AsyncClient(
        timeout=TIMEOUT, headers=UserAgentRotator.get_headers("unica"), follow_redirects=True
    ) as client:
        response = await retry_on_status(lambda: client.get(url, params=params), source="unica")
        responses.raise_for_status(response, source="unica")
        content = response.content

    if not content.startswith(XLSX_MAGIC):
        raise SourceUnavailableError(
            source="unica",
            url=url,
            last_error=f"Resposta não é XLSX válido ({len(content)} bytes)",
        )

    logger.info("unica_historico_ok", bytes=len(content))
    return content, f"{url}?produto={produto}&safraIni={safra_inicio}&safraFim={safra_fim}"
