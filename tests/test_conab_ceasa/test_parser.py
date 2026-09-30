from __future__ import annotations

import json
from pathlib import Path

import pytest

from agrobr.conab.ceasa.models import COLUNAS_SAIDA
from agrobr.conab.ceasa.parser import parse_precos
from agrobr.exceptions import ParseError

GOLDEN_DIR = Path(__file__).parent.parent / "golden_data" / "conab_ceasa" / "precos_sample"


def _ceasas_json() -> dict:
    return json.loads(GOLDEN_DIR.joinpath("ceasas_response.json").read_text(encoding="utf-8"))


class TestParseEdgeCases:
    @pytest.mark.parametrize("resposta", [{}, {"metadata": []}, {"resultset": None}, []])
    def test_resposta_sem_resultset_nao_vira_vazio(self, resposta):
        with pytest.raises(ParseError, match="sem a lista 'resultset'"):
            parse_precos(resposta, _ceasas_json())

    def test_empty_resultset(self):
        df = parse_precos({"resultset": [], "metadata": []}, _ceasas_json())
        assert len(df) == 0
        for col in COLUNAS_SAIDA:
            assert col in df.columns
