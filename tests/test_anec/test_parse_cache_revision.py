from __future__ import annotations

import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from agrobr.anec import api, client, models, parser
from tests.helpers import make_mock_async_client, make_mock_response


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "revision", ["timestamp", "url", "bytes", "unchanged", "sem_cache_depois", "sem_cache_antes"]
)
async def test_same_article_revision_updates_parse_and_provenance(revision):
    original = models.ANECArticle(
        id=1,
        cuid="same-article",
        title_en="ANEC - 04.2026",
        slug_en="week04",
        created_at=datetime(2026, 1, 22, tzinfo=UTC),
        pdf_url="https://www.anec.com.br/uploads/original.pdf",
        media_updated_at=datetime(2026, 1, 23, tzinfo=UTC),
    )
    updates = {}
    if revision == "timestamp":
        updates["media_updated_at"] = datetime(2026, 1, 24, tzinfo=UTC)
    if revision == "url":
        updates["pdf_url"] = "https://www.anec.com.br/uploads/revised.pdf"
    revised = original.model_copy(update=updates)
    reports = [
        parser.ParsedReport(
            pd.DataFrame(
                [
                    {
                        "porto": "SANTOS",
                        "produto": "soybean",
                        "periodo": "last_week",
                        "valor_ton": value,
                        "ano": 2026,
                        "semana": 4,
                        "data_inicio": pd.Timestamp("2026-01-25"),
                        "data_fim": pd.Timestamp("2026-01-31"),
                    }
                ]
            ),
            pd.DataFrame(),
            pd.DataFrame(),
            pd.DataFrame(),
            str(value),
        )
        for value in (10.0, 20.0)
    ]
    coleta = datetime(2026, 3, 25, tzinfo=UTC)
    fetched = [
        (client.Aquisicao(b"original", original.pdf_url, False, coleta, {}), original),
        (
            client.Aquisicao(
                b"revised" if revision == "bytes" else b"original",
                revised.pdf_url,
                False,
                coleta,
                {},
            ),
            revised,
        ),
    ]
    first_cache, second_cache = {
        "sem_cache_depois": (True, False),
        "sem_cache_antes": (False, True),
    }.get(revision, (True, True))
    with (
        patch("agrobr.anec.client._acquire_latest", AsyncMock(side_effect=fetched)),
        patch("agrobr.anec.parser.parse_anec_pdf", side_effect=reports) as parse,
    ):
        initial = await api.embarques(ano=2026, use_cache=first_cache)
        actual, meta = await api.embarques(ano=2026, use_cache=second_cache, return_meta=True)
    assert initial.iloc[0]["valor_ton"] == 10
    assert actual.iloc[0]["valor_ton"] == (10 if revision == "unchanged" else 20)
    assert parse.call_count == (1 if revision == "unchanged" else 2)
    assert meta.source_url == revised.pdf_url


@pytest.mark.asyncio
@pytest.mark.usefixtures("isolated_cache")
@pytest.mark.parametrize(
    ("remover", "atual", "usa_cache"),
    [
        ((), {}, True),
        ((), {"pdf_url": "https://www.anec.com.br/uploads/revised.pdf"}, False),
        ((), {"cuid": "outro-artigo"}, False),
        (("pdf_url", "cuid"), {}, False),
    ],
    ids=["mesmo_artigo", "outra_url", "outro_cuid", "cache_antigo_sem_identidade"],
)
async def test_cache_em_disco_so_serve_ao_mesmo_artigo_e_url(remover, atual, usa_cache):
    data = datetime(2026, 1, 23, tzinfo=UTC)
    original = models.ANECArticle(
        id=1,
        cuid="same-article",
        title_en="ANEC - 04.2026",
        slug_en="week04",
        created_at=data,
        pdf_url="https://www.anec.com.br/uploads/original.pdf",
        media_updated_at=data,
    )
    antigo = b"%PDF-antigo" + b"a" * 20_000
    novo = b"%PDF-novo" + b"n" * 20_000
    client._save_cache(original, antigo, data)
    semana, ano = original.week_year
    meta_path = client._cached_meta_path(ano, semana)
    gravado = json.loads(meta_path.read_text(encoding="utf-8"))
    for chave in remover:
        del gravado[chave]
    meta_path.write_text(json.dumps(gravado), encoding="utf-8")

    artigo = original.model_copy(update=atual)
    http = make_mock_async_client()
    http.get = AsyncMock(return_value=make_mock_response(200, content=novo, url=artigo.pdf_url))
    with patch("agrobr.anec.client.httpx.AsyncClient", return_value=http):
        content, _ = await client.fetch_pdf_bytes(artigo)

    assert content == (antigo if usa_cache else novo)
    assert http.get.await_count == (0 if usa_cache else 1)
