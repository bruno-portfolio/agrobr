from __future__ import annotations

import json
import warnings
from datetime import UTC, datetime
from types import SimpleNamespace

from scripts import fetch_structures


class _Relogio(datetime):
    @classmethod
    def now(cls, tz=None):
        return datetime(2026, 10, 1, 22, 15, 30, 123456, tzinfo=tz)


async def test_carimbo_utc_sem_utcnow(monkeypatch, tmp_path):
    from agrobr.cepea import client as cepea_client
    from agrobr.cepea.parsers import fingerprint

    async def pagina(_produto):
        return SimpleNamespace(source="cepea", html="<html></html>")

    impressao = SimpleNamespace(
        structure_hash="abc", model_dump=lambda **_opcoes: {"structure_hash": "abc"}
    )
    monkeypatch.setattr(cepea_client, "fetch_indicador_page", pagina)
    monkeypatch.setattr(fingerprint, "extract_fingerprint", lambda *_args: impressao)
    monkeypatch.setattr(fetch_structures, "datetime", _Relogio)
    saida = tmp_path / "estruturas.json"

    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        await fetch_structures.fetch_all_structures(str(saida))

    dados = json.loads(saida.read_text())
    assert dados["collected_at"] == "2026-10-01T22:15:30.123456Z"
    assert datetime.fromisoformat(dados["collected_at"]).tzinfo == UTC
    assert dados["sources"]["cepea"] == {"structure_hash": "abc"}
