from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import pytest
from lxml import etree

from agrobr.cnuc import api

PASTA = Path(__file__).parents[1] / "golden_data" / "cnuc" / "se_20261001"
MANIFESTO: dict[str, Any] = json.loads((PASTA / "manifest.json").read_bytes())
HITS = (PASTA / "hits.xml").read_bytes()
TABULAR = (PASTA / "tabular.xml").read_bytes()
GEO = (PASTA / "geo.xml").read_bytes()


def hits(total: int) -> bytes:
    return (
        b'<wfs:FeatureCollection xmlns:wfs="http://www.opengis.net/wfs/2.0" '
        b'numberMatched="%d" numberReturned="0"/>' % total
    )


def instalar(
    monkeypatch: pytest.MonkeyPatch,
    *,
    contagem: bytes = HITS,
    tabular: bytes = TABULAR,
    geo: bytes = GEO,
) -> tuple[AsyncMock, AsyncMock]:
    corpos = {False: tabular, True: geo}

    async def feicoes(_filtro: Any, *, geo: bool, **_opcoes: Any) -> tuple[bytes, str]:
        return corpos[geo], "https://test/features"

    fetch_count = AsyncMock(return_value=(contagem, "https://test/hits"))
    fetch_ucs = AsyncMock(side_effect=feicoes)
    monkeypatch.setattr(api.client, "fetch_count", fetch_count)
    monkeypatch.setattr(api.client, "fetch_ucs", fetch_ucs)
    return fetch_count, fetch_ucs


def primeiras(corpo: bytes, quantidade: int) -> bytes:
    raiz = etree.fromstring(corpo)
    membros = raiz.findall("{http://www.opengis.net/wfs/2.0}member")
    for membro in membros[quantidade:]:
        raiz.remove(membro)
    raiz.set("numberReturned", str(min(quantidade, len(membros))))
    return etree.tostring(raiz, xml_declaration=True, encoding="UTF-8")
