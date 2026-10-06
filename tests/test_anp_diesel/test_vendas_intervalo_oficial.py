from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from agrobr.alt import anp_diesel
from tests import helpers

GOLDEN = Path(__file__).resolve().parents[2] / "tests/golden_data/anp_diesel/vendas_sample"


async def test_vendas_intervalo_fechado_preserva_os_dois_limites_e_os_volumes(monkeypatch):
    metadata = json.loads((GOLDEN / "metadata.json").read_text(encoding="utf-8"))
    seen = helpers.install_replay_http(
        monkeypatch,
        {
            "requests": [
                {
                    "match": {"path": metadata["url"], "params": {}, "skip": 0},
                    "file": "response.csv",
                    "content_type": "text/csv; charset=utf-8",
                }
            ]
        },
        GOLDEN,
    )

    frame, meta = await anp_diesel.vendas_diesel(
        uf="RO", inicio="2013-02-01", fim="2013-03-01", return_meta=True
    )

    expected = {
        ("2013-02-01", "DIESEL S10"): 3681.7,
        ("2013-03-01", "DIESEL S10"): 4700.67,
        ("2013-02-01", "DIESEL S-500"): 290.0,
        ("2013-03-01", "DIESEL S-500"): 170.0,
        ("2013-02-01", "DIESEL S-1800"): 47880.411,
        ("2013-03-01", "DIESEL S-1800"): 54380.456,
        ("2013-02-01", "DIESEL MARÍTIMO"): 1294.389,
        ("2013-03-01", "DIESEL MARÍTIMO"): 994.644,
        ("2013-02-01", "DIESEL (OUTROS )"): 115.0,
        ("2013-03-01", "DIESEL (OUTROS )"): 344.0,
    }
    actual = {
        (row.data.strftime("%Y-%m-%d"), row.produto): row.volume_m3
        for row in frame.itertuples(index=False)
    }
    assert actual == expected
    assert frame["uf"].tolist() == ["RO"] * 10
    assert frame["regiao"].tolist() == ["Norte"] * 10
    assert frame["volume_m3"].dtype == "float64"
    assert frame.index.equals(pd.RangeIndex(10))
    assert meta.records_count == 10
    helpers.assert_replay_served(seen)
