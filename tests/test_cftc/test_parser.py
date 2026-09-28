from __future__ import annotations

import json
from pathlib import Path

import pytest

from agrobr.cftc.parser import parse_cot
from agrobr.exceptions import ParseError

GOLDEN_DIR = Path(__file__).parent.parent / "golden_data" / "cftc"


def _load_golden() -> list[dict[str, str]]:
    with open(GOLDEN_DIR / "cot_sample.json", encoding="utf-8") as f:
        return json.load(f)


class TestParseCotValidation:
    def test_campo_ausente_raises(self):
        records = _load_golden()
        for r in records:
            del r["m_money_positions_long_all"]

        with pytest.raises(ParseError, match="Campos ausentes"):
            parse_cot(records)

    def test_posicao_nula_raises(self):
        records = _load_golden()
        records[0]["open_interest_all"] = ""

        with pytest.raises(ParseError, match="Posições nulas"):
            parse_cot(records)

    def test_posicao_negativa_raises(self):
        records = _load_golden()
        records[0]["open_interest_all"] = "-100"

        with pytest.raises(ParseError, match="Posições negativas"):
            parse_cot(records)

    def test_data_invalida_raises(self):
        records = _load_golden()
        records[0]["report_date_as_yyyy_mm_dd"] = "data-quebrada"

        with pytest.raises(ParseError, match="Datas inválidas"):
            parse_cot(records)
