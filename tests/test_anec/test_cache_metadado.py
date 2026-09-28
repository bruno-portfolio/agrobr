from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import httpx
import pandas as pd
import pytest

from agrobr import anec
from agrobr.anec import api, client, models, parser
from tests import helpers

GOLDEN = Path(__file__).parents[1] / "golden_data/anec/weekly_w34_2026"
ARTIGO = models.ANECArticle.model_validate_json(
    (GOLDEN / "article.json").read_text(encoding="utf-8")
)
PDF = (GOLDEN / "response.pdf").read_bytes()
SEMANA, ANO = ARTIGO.week_year


@pytest.fixture
def anec_servido(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> list[str]:
    monkeypatch.setenv("AGROBR_CACHE_CACHE_DIR", str(tmp_path))
    monkeypatch.setattr(client, "list_articles", AsyncMock(return_value=[ARTIGO]))
    monkeypatch.setattr(api, "_PARSE_CACHE", {})
    pedidos: list[str] = []
    original = httpx.AsyncClient

    def responder(request: httpx.Request) -> httpx.Response:
        pedidos.append(str(request.url))
        return httpx.Response(200, content=PDF, request=request)

    def fabrica(**kwargs: Any) -> httpx.AsyncClient:
        return original(transport=httpx.MockTransport(responder), **kwargs)

    monkeypatch.setattr(client.httpx, "AsyncClient", fabrica)
    return pedidos


async def test_acerto_do_cache_em_disco_publica_a_coleta_original(anec_servido):
    with helpers.sem_excecao():
        frio, meta_frio = await anec.embarques(ano=ANO, semana=SEMANA, return_meta=True)
        quente, meta_quente = await anec.embarques(ano=ANO, semana=SEMANA, return_meta=True)
    gravado = json.loads(client._cached_meta_path(ANO, SEMANA).read_text(encoding="utf-8"))
    coleta = datetime.fromisoformat(gravado["fetched_at"])
    assert anec_servido == [ARTIGO.pdf_url]
    assert (meta_frio.from_cache, meta_quente.from_cache) == (False, True)
    assert meta_frio.fetched_at == meta_quente.fetched_at == coleta
    assert meta_frio.fetch_timestamp == meta_quente.fetch_timestamp == coleta
    assert meta_quente.source_details == {
        "media_updated_at": ARTIGO.media_updated_at.isoformat(),
        "layout_fingerprint": parser.parse_anec_pdf(PDF).fingerprint,
    }
    helpers.conferir_corpo(meta_frio, PDF)
    helpers.conferir_corpo(meta_quente, PDF)
    pd.testing.assert_frame_equal(frio, quente)
