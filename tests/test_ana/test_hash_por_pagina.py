from __future__ import annotations

import hashlib
import json
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from agrobr import ana
from agrobr.ana import client

GOLDEN = Path(__file__).parents[1] / "golden_data" / "ana"
BBOX = (-48.1, -16.1, -47.9, -15.9)
SOURCE_URL = "https://fonte.test/primeira-faixa/query"
CASOS = (
    ("hidrografia", "fetch_layer", "oficial_20260923/hidrografia_df/chave_0.json"),
    ("hidrografia_geo", "fetch_layer", "oficial_20260923/hidrografia_df/chave_0.geojson"),
    ("massas_dagua", "fetch_massas_dagua", "massas_dagua_20261001/barragem_df/faixa_0.json"),
    ("massas_dagua_geo", "fetch_massas_dagua", "massas_dagua_20261001/barragem_df/faixa_0.geojson"),
)


async def _consultar(monkeypatch, caso, paginas, *, bbox=BBOX, max_registros=123):
    funcao, metodo, _ = caso
    if funcao.endswith("_geo"):
        pytest.importorskip("geopandas")
    mock = AsyncMock(return_value=(paginas, SOURCE_URL))
    monkeypatch.setattr(client, metodo, mock)
    resultado = await getattr(ana, funcao)(bbox=bbox, max_registros=max_registros, return_meta=True)
    mock.assert_awaited_once()
    return resultado


def _consulta(caso, *, bbox=BBOX, max_registros=123):
    return {
        "fonte": "ana",
        "recurso": caso[0].removesuffix("_geo"),
        "where": "1=1",
        "bbox": list(bbox),
        "max_registros": max_registros,
        "formato": "geojson" if caso[0].endswith("_geo") else "json",
    }


@pytest.mark.parametrize("caso", CASOS, ids=[c[0] for c in CASOS])
@pytest.mark.parametrize("quantidade", [0, 1, 2])
async def test_hash_das_paginas_preserva_corpo_ou_manifesto(monkeypatch, caso, quantidade):
    corpo = (GOLDEN / caso[2]).read_bytes()
    paginas = [corpo, corpo + b" "][:quantidade]
    _, meta = await _consultar(monkeypatch, caso, paginas)
    assert meta.source_url == SOURCE_URL
    if quantidade < 2:
        assert meta.raw_content_hash == (hashlib.sha256(corpo).hexdigest() if paginas else None)
        assert meta.raw_content_size == (len(corpo) if paginas else 0)
        assert meta.source_details == {}
        return
    recursos = [
        {"pagina": n, "sha256": hashlib.sha256(p).hexdigest(), "bytes": len(p)}
        for n, p in enumerate(paginas, 1)
    ]
    manifesto = json.dumps(
        {"query": _consulta(caso), "resources": recursos},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    assert meta.raw_content_hash == hashlib.sha256(manifesto).hexdigest()
    assert meta.raw_content_size == len(manifesto)
    assert meta.source_details == {
        "hash_kind": "resource_manifest_sha256",
        "manifest_encoding": "canonical_json_utf8",
        "manifest_fields": ["query", "resources"],
        "query": _consulta(caso),
        "resources": recursos,
        "resource_bytes": sum(map(len, paginas)),
    }


@pytest.mark.parametrize("caso", CASOS, ids=[c[0] for c in CASOS])
async def test_hash_muda_com_segunda_pagina_ordem_ou_consulta(monkeypatch, caso):
    corpo = (GOLDEN / caso[2]).read_bytes()
    paginas = [corpo, corpo + b" "]
    _, original = await _consultar(monkeypatch, caso, paginas)
    _, outra_pagina = await _consultar(monkeypatch, caso, [corpo, corpo + b"\n"])
    _, outra_ordem = await _consultar(monkeypatch, caso, paginas[::-1])
    _, outro_limite = await _consultar(monkeypatch, caso, paginas, max_registros=124)
    _, outra_selecao = await _consultar(
        monkeypatch, caso, paginas, bbox=(-48.0, -16.0, -47.8, -15.8)
    )
    assert (
        len(
            {
                m.raw_content_hash
                for m in (original, outra_pagina, outra_ordem, outro_limite, outra_selecao)
            }
        )
        == 5
    )


async def test_hash_inclui_filtro_de_uf_sem_faixa_fid(monkeypatch):
    corpo = (GOLDEN / CASOS[2][2]).read_bytes()
    mock = AsyncMock(return_value=([corpo, corpo], SOURCE_URL))
    monkeypatch.setattr(client, "fetch_massas_dagua", mock)
    _, meta = await ana.massas_dagua(uf="DF", max_registros=3, return_meta=True)
    assert meta.source_details["query"] == {
        "fonte": "ana",
        "recurso": "massas_dagua",
        "bbox": None,
        "formato": "json",
        "where": "(nmufe = 'DISTRITO FEDERAL' OR nmufe LIKE 'DISTRITO FEDERAL, %' "
        "OR nmufe LIKE '%, DISTRITO FEDERAL' OR nmufe LIKE '%, DISTRITO FEDERAL, %')",
        "max_registros": 3,
    }
    mock.assert_awaited_once()
