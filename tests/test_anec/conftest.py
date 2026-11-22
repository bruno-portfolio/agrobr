from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from agrobr.utils import time as time_utils
from tests.helpers import make_mock_async_client, make_mock_response

GOLDEN_WORDPRESS = Path(__file__).parents[1] / "golden_data/anec/wordpress_20261121"
SITEMAP_URL = "https://anec.com.br/wp-sitemap-posts-cpt_biblioteca-1.xml"
BOLETIM_45_URL = "https://anec.com.br/biblioteca/estatisticas-semana-45-2026/"
BOLETIM_45_LASTMOD = "2026-11-19T19:31:55+00:00"
BOLETIM_45_PDF = "https://anec.com.br/redeilpf_content/Shipment-Flow-ANEC-Week-45-2026.pdf"


def sitemap_xml(*entradas: tuple[str, str]) -> bytes:
    urls = "".join(
        f"<url><loc>{loc}</loc><lastmod>{lastmod}</lastmod></url>" for loc, lastmod in entradas
    )
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        f'<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">{urls}</urlset>'
    ).encode()


def boletim_html(*, post_id: int, titulo: str, pdfs: tuple[str, ...] = ()) -> bytes:
    links = "".join(f'<a href="{pdf}">Baixar PDF</a>' for pdf in pdfs)
    return (
        f'<html><head><meta charset="UTF-8"><title>{titulo} – Anec</title>'
        f"<link rel='shortlink' href='https://anec.com.br/?p={post_id}' /></head>"
        f"<body>{links}</body></html>"
    ).encode()


def site_anec(paginas: dict[str, bytes]) -> MagicMock:
    async def get(url: str, *_args: object, **_kwargs: object) -> MagicMock:
        if url not in paginas:
            return make_mock_response(404, content=b"not found", url=url)
        return make_mock_response(200, content=paginas[url], url=url)

    mock_client = make_mock_async_client()
    mock_client.get = AsyncMock(side_effect=get)
    return mock_client


def urls_pedidas(mock_client: MagicMock) -> list[str]:
    return [chamada.args[0] for chamada in mock_client.get.await_args_list]


@pytest.fixture
def site_oficial() -> dict[str, bytes]:
    return {
        SITEMAP_URL: (GOLDEN_WORDPRESS / "sitemap_cpt_biblioteca.xml").read_bytes(),
        BOLETIM_45_URL: (GOLDEN_WORDPRESS / "estatisticas-semana-45-2026.html").read_bytes(),
    }


@pytest.fixture
def isolated_cache(tmp_path, monkeypatch):
    monkeypatch.setenv("AGROBR_CACHE_CACHE_DIR", str(tmp_path / "agrobr_cache"))
    return tmp_path / "agrobr_cache"


@pytest.fixture(autouse=True)
def _reset_anec_list_cache():
    from agrobr.anec import client as _client
    from agrobr.anec.api import _parse_cache_clear

    _client._list_cache_clear()
    _parse_cache_clear()
    yield
    _client._list_cache_clear()
    _parse_cache_clear()


@pytest.fixture(autouse=True)
def reference_year(monkeypatch):
    monkeypatch.setattr(time_utils, "utcnow_aware", lambda: datetime(2026, 9, 8, 12, tzinfo=UTC))
