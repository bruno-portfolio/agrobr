from __future__ import annotations

from pathlib import Path

from agrobr import conab, contracts
from agrobr.conab.ceasa import models
from tests import helpers

GOLDEN = Path(__file__).resolve().parents[2] / "tests/golden_data/conab_ceasa/precos_20260923"


async def test_ceasa_nome_parcial_preserva_preco_publicado(monkeypatch):
    seen = helpers.install_replay_http(
        monkeypatch,
        {
            "requests": [
                {
                    "match": {
                        "path": models.PENTAHO_BASE,
                        "params": {"path": models.CDA_PROHORT, "dataAccessId": query},
                        "skip": 0,
                    },
                    "file": filename,
                    "content_type": "application/json",
                }
                for query, filename in ((models.QUERY_PRECOS, "precos_response.json"),)
            ]
        },
        GOLDEN,
    )

    frame, meta = await conab.ceasa_precos(produto="abacate", ceasa="juazeiro", return_meta=True)

    assert [
        (
            row.data.strftime("%Y-%m-%d"),
            row.produto,
            row.unidade,
            row.ceasa,
            row.ceasa_uf,
            row.preco,
        )
        for row in frame.itertuples(index=False)
    ] == [("2026-09-21", "ABACATE", "KG", "AMA/BA - JUAZEIRO", "BA", 3.65)]
    assert meta.records_count == 1
    assert meta.schema_version == contracts.get_contract("preco_atacado").version == "2.0"
    helpers.assert_replay_served(seen)
