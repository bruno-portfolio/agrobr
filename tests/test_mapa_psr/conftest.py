from __future__ import annotations

from pathlib import Path

import pytest

from agrobr.alt.mapa_psr import client

CATALOGO_OFICIAL = (
    Path(__file__).parents[1] / "golden_data/mapa_psr/catalogo_20260927/package_show.json"
)


@pytest.fixture(autouse=True)
def catalogo_oficial(monkeypatch):
    """Serve o pacote do PSR capturado em 27/09/2026 (sem 2026) a quem consulta o catálogo."""

    async def servir() -> bytes:
        return CATALOGO_OFICIAL.read_bytes()

    monkeypatch.setattr(client, "fetch_catalogo", servir)
