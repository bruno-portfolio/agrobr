from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from agrobr.conab.ceasa import api, client

GOLDEN_DIR = Path(__file__).parent.parent / "golden_data" / "conab_ceasa" / "precos_sample"


def _precos_json() -> dict:
    return json.loads(GOLDEN_DIR.joinpath("precos_response.json").read_text(encoding="utf-8"))


def _ceasas_json() -> dict:
    return json.loads(GOLDEN_DIR.joinpath("ceasas_response.json").read_text(encoding="utf-8"))


@pytest.fixture()
def mock_fetch():
    precos = _precos_json()
    ceasas = _ceasas_json()
    with (
        patch.object(
            client,
            "fetch_precos",
            new_callable=AsyncMock,
            return_value=(precos, "https://pentahoportaldeinformacoes.conab.gov.br/test"),
        ),
        patch.object(
            client,
            "fetch_ceasas",
            new_callable=AsyncMock,
            return_value=(ceasas, "https://pentahoportaldeinformacoes.conab.gov.br/test"),
        ),
    ):
        yield


class TestProdutos:
    def test_returns_sorted_list(self):
        result = api.produtos()
        assert isinstance(result, list)
        assert result == sorted(result)


class TestCategorias:
    def test_returns_dict(self):
        result = api.categorias()
        assert isinstance(result, dict)
        assert "FRUTAS" in result
        assert "HORTALICAS" in result
