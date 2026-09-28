from __future__ import annotations

import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx
import pytest

from agrobr import acervo_fundiario
from agrobr.acervo_fundiario import client
from tests import helpers
from tests.test_acervo_fundiario import oficial

GOLDEN = Path(__file__).parents[1] / "golden_data/acervo_fundiario"
ZIP = (GOLDEN / "snci_rr_20260922/response.zip").read_bytes()
SIDECAR = json.loads((GOLDEN / "cache_snci_rr_20260923/RR.json").read_text(encoding="utf-8"))
COLETA_ORIGINAL = datetime.fromisoformat(SIDECAR["fetched_at"])


@pytest.fixture
def cache_real(isolated_cache: Path) -> Path:
    pasta = isolated_cache / "acervo_fundiario" / "snci"
    pasta.mkdir(parents=True)
    (pasta / "RR.zip").write_bytes(ZIP)
    sidecar = {**SIDECAR, "size_bytes": len(ZIP), "sha256": hashlib.sha256(ZIP).hexdigest()}
    (pasta / "RR.json").write_text(json.dumps(sidecar), encoding="utf-8")
    return pasta


def servir_incra(monkeypatch: pytest.MonkeyPatch, *, etag: str) -> list[str]:
    pedidos: list[str] = []
    original = httpx.AsyncClient
    cabecalhos = {
        "ETag": etag,
        "Last-Modified": SIDECAR["last_modified"],
        "Content-Length": str(len(ZIP)),
    }

    def responder(request: httpx.Request) -> httpx.Response:
        pedidos.append(request.method)
        conteudo = b"" if request.method == "HEAD" else ZIP
        return httpx.Response(200, headers=cabecalhos, content=conteudo, request=request)

    def fabrica(**kwargs: Any) -> httpx.AsyncClient:
        return original(transport=httpx.MockTransport(responder), **kwargs)

    monkeypatch.setattr(client.httpx, "AsyncClient", fabrica)
    return pedidos


async def test_acerto_de_cache_publica_a_coleta_original_e_a_revalidacao(cache_real, monkeypatch):
    pytest.importorskip("pyogrio")
    pedidos = servir_incra(monkeypatch, etag=SIDECAR["etag"])
    with helpers.sem_excecao():
        df, meta = await acervo_fundiario.snci("RR", return_meta=True)
    assert pedidos == ["HEAD"]
    assert meta.from_cache is True
    assert meta.fetched_at == meta.fetch_timestamp == COLETA_ORIGINAL
    helpers.conferir_corpo(meta, ZIP)
    assert meta.source_details["etag"] == SIDECAR["etag"]
    assert meta.source_details["last_modified"] == SIDECAR["last_modified"]
    revalidado_em = meta.source_details.get("revalidado_em")
    assert revalidado_em is not None
    assert datetime.fromisoformat(revalidado_em) > COLETA_ORIGINAL
    assert len(df) == len(oficial.dbf(ZIP))
    assert (
        json.loads((cache_real / "RR.json").read_text(encoding="utf-8"))["fetched_at"]
        == (SIDECAR["fetched_at"])
    )


async def test_validador_remoto_novo_baixa_e_publica_a_nova_coleta(cache_real, monkeypatch):
    pytest.importorskip("pyogrio")
    pedidos = servir_incra(monkeypatch, etag='"novo"')
    with helpers.sem_excecao():
        df, meta = await acervo_fundiario.snci("RR", return_meta=True)
    sidecar = json.loads((cache_real / "RR.json").read_text(encoding="utf-8"))
    assert pedidos == ["HEAD", "GET"]
    assert meta.from_cache is False
    helpers.conferir_corpo(meta, ZIP)
    assert meta.fetched_at == datetime.fromisoformat(sidecar["fetched_at"]) > COLETA_ORIGINAL
    assert meta.source_details == {"etag": '"novo"', "last_modified": SIDECAR["last_modified"]}
    assert len(df) == len(oficial.dbf(ZIP))


async def test_sidecar_sem_coleta_original_baixa_de_novo(cache_real, monkeypatch):
    pytest.importorskip("pyogrio")
    sidecar = json.loads((cache_real / "RR.json").read_text(encoding="utf-8"))
    del sidecar["fetched_at"]
    (cache_real / "RR.json").write_text(json.dumps(sidecar), encoding="utf-8")
    pedidos = servir_incra(monkeypatch, etag=SIDECAR["etag"])
    with helpers.sem_excecao():
        _, meta = await acervo_fundiario.snci("RR", return_meta=True)
    assert pedidos == ["GET", "HEAD"]
    assert meta.from_cache is False
    assert "fetched_at" in json.loads((cache_real / "RR.json").read_text(encoding="utf-8"))
