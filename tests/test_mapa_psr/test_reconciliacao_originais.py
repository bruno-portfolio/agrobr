from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pytest

from agrobr import datasets
from agrobr.alt.mapa_psr import api
from tests.test_mapa_psr import test_reconciliacao as review

ORIGINAIS = Path(os.environ.get("AGROBR_RECONCILIACAO_PSR_ORIGINALS", ""))
pytestmark = pytest.mark.skipif(
    not os.environ.get("AGROBR_RECONCILIACAO_PSR_ORIGINALS"),
    reason="Defina AGROBR_RECONCILIACAO_PSR_ORIGINALS com o diretório dos CSVs originais",
)
CASES = [
    case
    for case in review.MANIFEST["cases"]
    if (case["tipo"] == "apolices" and "last" in case["labels"])
    or (
        case["tipo"] == "sinistros"
        and "positive_indemnity" in case["labels"]
        and case["family"] == "psr:2016-2024"
    )
    or "literal_NULL" in case["id"]
]


@pytest.mark.parametrize("layer", ["source", "dataset"])
@pytest.mark.parametrize("case", CASES, ids=lambda case: case["id"])
async def test_psr_corpo_integral_coorte_publicada(
    case: dict[str, Any], layer: str, monkeypatch: pytest.MonkeyPatch
):
    for resource in review.MANIFEST["resources"]:
        path = ORIGINAIS / resource["original"]["file"]
        assert path.is_file(), f"Corpo original ausente: {path}"
    seen = review.install_publication(monkeypatch, originals=ORIGINAIS)
    kwargs = dict(case["query"])
    kwargs["produto"] = kwargs.pop("cultura")
    if layer == "source":
        function = api.apolices if case["tipo"] == "apolices" else api.sinistros
        frame = await function(**kwargs)
    else:
        frame = await datasets.seguro_rural(**kwargs, tipo=case["tipo"])
    review.assert_publication(frame, case)
    review.helpers.assert_replay_served(seen)
