"""Cliente HTTP para download de dados ANTAQ (bulk ZIP/TXT).

Fonte: Estatístico Aquaviário — ANTAQ
URL base: https://web3.antaq.gov.br/ea/txt/

Arquivos:
- {ANO}.zip — dados anuais (Atracacao, Carga, etc.)
- Mercadoria.zip — tabela de referência de mercadorias (NCM SH4)
- InstalacaoOrigem.zip — tabela de referência de portos (origem)
"""

from __future__ import annotations

import io
import zipfile

import httpx
import structlog

from agrobr.constants import HTTPSettings
from agrobr.http.retry import retry_on_status

logger = structlog.get_logger()

BULK_TXT_BASE = "https://web3.antaq.gov.br/ea/txt"

_settings = HTTPSettings()

TIMEOUT = httpx.Timeout(
    connect=_settings.timeout_connect,
    read=180.0,
    write=_settings.timeout_write,
    pool=_settings.timeout_pool,
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
}


async def _download_zip(url: str) -> bytes:
    """Baixa arquivo ZIP da ANTAQ.

    Args:
        url: URL completa do ZIP.

    Returns:
        Conteúdo do ZIP como bytes.

    Raises:
        SourceUnavailableError: Se não conseguir baixar.
    """
    logger.info("antaq_download_zip", url=url)

    async with httpx.AsyncClient(timeout=TIMEOUT, headers=HEADERS, follow_redirects=True) as client:
        response = await retry_on_status(
            lambda: client.get(url),
            source="antaq",
        )
        response.raise_for_status()

        logger.info(
            "antaq_download_ok",
            url=url,
            size_bytes=len(response.content),
        )
        return response.content


def _extract_txt_from_zip(zip_bytes: bytes, filename: str) -> str:
    """Extrai um TXT de dentro de um ZIP em memória.

    Args:
        zip_bytes: Conteúdo do ZIP.
        filename: Nome do arquivo a extrair.

    Returns:
        Conteúdo do TXT como string (UTF-8-sig para lidar com BOM).

    Raises:
        KeyError: Se o arquivo não existir no ZIP.
    """
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf, zf.open(filename) as f:
        return f.read().decode("utf-8-sig")


def list_zip_contents(zip_bytes: bytes) -> list[str]:
    """Lista arquivos dentro de um ZIP.

    Args:
        zip_bytes: Conteúdo do ZIP.

    Returns:
        Lista de nomes de arquivos.
    """
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        return zf.namelist()


async def fetch_ano_zip(ano: int) -> bytes:
    """Baixa ZIP de dados anuais da ANTAQ.

    Args:
        ano: Ano (2010-2025).

    Returns:
        Conteúdo do ZIP como bytes.
    """
    url = f"{BULK_TXT_BASE}/{ano}.zip"
    return await _download_zip(url)


async def fetch_mercadoria_zip() -> bytes:
    """Baixa ZIP da tabela de referência de mercadorias.

    Returns:
        Conteúdo do ZIP como bytes.
    """
    url = f"{BULK_TXT_BASE}/Mercadoria.zip"
    return await _download_zip(url)


def extract_atracacao(zip_bytes: bytes, ano: int) -> str:
    """Extrai arquivo de atracação do ZIP anual.

    Args:
        zip_bytes: Conteúdo do ZIP anual.
        ano: Ano para montar o nome do arquivo.

    Returns:
        Conteúdo TXT como string.
    """
    return _extract_txt_from_zip(zip_bytes, f"{ano}Atracacao.txt")


def extract_carga(zip_bytes: bytes, ano: int) -> str:
    """Extrai arquivo de carga do ZIP anual.

    Args:
        zip_bytes: Conteúdo do ZIP anual.
        ano: Ano para montar o nome do arquivo.

    Returns:
        Conteúdo TXT como string.
    """
    return _extract_txt_from_zip(zip_bytes, f"{ano}Carga.txt")


def extract_mercadoria(zip_bytes: bytes) -> str:
    """Extrai arquivo de mercadorias do ZIP de referência.

    Args:
        zip_bytes: Conteúdo do ZIP de mercadorias.

    Returns:
        Conteúdo TXT como string.
    """
    return _extract_txt_from_zip(zip_bytes, "Mercadoria.txt")
