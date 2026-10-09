from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pytest

from agrobr import datasets
from agrobr.zarc import api
from tests import helpers
from tests.test_zarc import test_reconciliacao as review

ORIGINAIS = Path(os.environ.get("AGROBR_RECONCILIACAO_ZARC_ORIGINALS", ""))
pytestmark = pytest.mark.skipif(
    not os.environ.get("AGROBR_RECONCILIACAO_ZARC_ORIGINALS"),
    reason="Defina AGROBR_RECONCILIACAO_ZARC_ORIGINALS com o diretório dos CSVs originais",
)
CASES = [case for case in review.MANIFEST["cases"] if case["id"].endswith("_last_city")]


@pytest.mark.parametrize("case", CASES, ids=lambda item: item["id"])
@pytest.mark.parametrize("layer", ["source", "dataset"])
async def test_zarc_corpo_integral_ultima_cidade(
    monkeypatch: pytest.MonkeyPatch, case: dict[str, Any], layer: str
):
    resource = review.RESOURCES[case["resource"]]
    assert case["complete_original_query"]
    path = ORIGINAIS / resource["original"]["file"]
    assert path.is_file(), f"Corpo original ausente: {path}"
    seen = review.install_replay(monkeypatch, resource, path)
    fetch = api.zoneamento if layer == "source" else datasets.zoneamento_agricola
    frame, meta = await fetch(**case["query"], use_cache=False, return_meta=True)
    review.assert_values(frame, review.expected_rows(case, original=True))
    review.assert_meta(meta, resource, len(frame), original=True)
    assert frame["registro_origem"].iloc[-1] == resource["source_rows"]
    assert len(seen["served"]) == 2
    helpers.assert_replay_served(seen)


@pytest.mark.parametrize(
    "label", ["Arroz Sequeiro", "arroz_sequeiro", "Trigo Sequeiro", "trigo_sequeiro"]
)
@pytest.mark.parametrize("layer", ["source", "dataset"])
async def test_zarc_corpo_integral_cultura_legada(
    monkeypatch: pytest.MonkeyPatch, label: str, layer: str
):
    resource = review.RESOURCES["2016_2017"]
    canonical = review.LEGACY["aliases"].get(label, label)
    selected = [dict(row) for row in review.LEGACY["expected"] if row["cultura"] == canonical]
    for row in selected:
        row["registro_origem"] = review.LEGACY["selected_original_records"][
            row["registro_origem"] - 1
        ]
    path = ORIGINAIS / resource["original"]["file"]
    assert path.is_file(), f"Corpo original ausente: {path}"
    seen = review.install_replay(monkeypatch, resource, path)
    fetch = api.zoneamento if layer == "source" else datasets.zoneamento_agricola
    frame, meta = await fetch(
        produto=label,
        municipio=selected[0]["geocodigo"],
        safra="2016/2017",
        use_cache=False,
        return_meta=True,
    )
    review.assert_values(frame, selected)
    review.assert_meta(meta, resource, len(frame), original=True)
    helpers.assert_replay_served(seen)
