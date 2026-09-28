from __future__ import annotations

import json
from pathlib import Path

from agrobr.conab.ceasa.models import COLUNAS_SAIDA
from agrobr.conab.ceasa.parser import parse_precos

GOLDEN_DIR = Path(__file__).parent.parent / "golden_data" / "conab_ceasa" / "precos_sample"


def _ceasas_json() -> dict:
    return json.loads(GOLDEN_DIR.joinpath("ceasas_response.json").read_text(encoding="utf-8"))


class TestParseEdgeCases:
    def test_empty_resultset(self):
        df = parse_precos({"resultset": [], "metadata": []}, _ceasas_json())
        assert len(df) == 0
        for col in COLUNAS_SAIDA:
            assert col in df.columns
