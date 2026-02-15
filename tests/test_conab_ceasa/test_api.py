from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from agrobr.conab.ceasa import api, client, parser
from agrobr.conab.ceasa.models import CEASA_UF_MAP, COLUNAS_SAIDA, PRODUTOS_PROHORT
from agrobr.models import MetaInfo

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


@pytest.mark.asyncio()
class TestPrecos:
    async def test_returns_dataframe(self, mock_fetch) -> None:  # noqa: ARG002
        df = await api.precos()
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0

    async def test_columns_match_schema(self, mock_fetch) -> None:  # noqa: ARG002
        df = await api.precos()
        for col in COLUNAS_SAIDA:
            assert col in df.columns

    async def test_filter_produto(self, mock_fetch) -> None:  # noqa: ARG002
        df = await api.precos(produto="tomate")
        assert len(df) > 0
        assert (df["produto"] == "TOMATE").all()

    async def test_filter_produto_case_insensitive(self, mock_fetch) -> None:  # noqa: ARG002
        df = await api.precos(produto="TOMATE")
        assert len(df) > 0
        assert (df["produto"] == "TOMATE").all()

    async def test_filter_produto_unknown_returns_empty(self, mock_fetch) -> None:  # noqa: ARG002
        df = await api.precos(produto="INEXISTENTE")
        assert len(df) == 0

    async def test_filter_ceasa(self, mock_fetch) -> None:  # noqa: ARG002
        df = await api.precos(ceasa="CEAGESP - SAO PAULO")
        assert len(df) > 0
        assert df["ceasa"].str.contains("CEAGESP - SAO PAULO").all()

    async def test_filter_ceasa_partial(self, mock_fetch) -> None:  # noqa: ARG002
        df = await api.precos(ceasa="SAO PAULO")
        assert len(df) > 0
        assert df["ceasa"].str.upper().str.contains("SAO PAULO").all()

    async def test_filter_combined(self, mock_fetch) -> None:  # noqa: ARG002
        df = await api.precos(produto="tomate", ceasa="CEAGESP - SAO PAULO")
        assert len(df) == 1
        assert df.iloc[0]["produto"] == "TOMATE"
        assert "SAO PAULO" in df.iloc[0]["ceasa"]

    async def test_return_meta(self, mock_fetch) -> None:  # noqa: ARG002
        result = await api.precos(return_meta=True)
        assert isinstance(result, tuple)
        df, meta = result
        assert isinstance(df, pd.DataFrame)
        assert isinstance(meta, MetaInfo)
        assert meta.source == "conab_ceasa"
        assert meta.records_count == len(df)
        assert meta.parser_version == parser.PARSER_VERSION
        assert meta.source_method == "httpx+json"

    async def test_meta_fetch_duration(self, mock_fetch) -> None:  # noqa: ARG002
        _, meta = await api.precos(return_meta=True)
        assert meta.fetch_duration_ms >= 0
        assert meta.parse_duration_ms >= 0

    async def test_meta_columns(self, mock_fetch) -> None:  # noqa: ARG002
        _, meta = await api.precos(return_meta=True)
        assert meta.columns == COLUNAS_SAIDA

    async def test_zona_cinza_warning(self, mock_fetch) -> None:  # noqa: ARG002
        api._WARNED = False
        with pytest.warns(UserWarning, match="zona_cinza"):
            await api.precos()

    async def test_no_double_warning(self, mock_fetch) -> None:  # noqa: ARG002
        api._WARNED = True
        import warnings

        with warnings.catch_warnings():
            warnings.simplefilter("error")
            await api.precos()


class TestProdutos:
    def test_returns_sorted_list(self):
        result = api.produtos()
        assert isinstance(result, list)
        assert result == sorted(result)

    def test_count_matches(self):
        result = api.produtos()
        assert len(result) == len(PRODUTOS_PROHORT)

    def test_tomate_present(self):
        result = api.produtos()
        assert "TOMATE" in result


class TestListaCeasas:
    def test_returns_list_of_dicts(self):
        result = api.lista_ceasas()
        assert isinstance(result, list)
        assert len(result) == len(CEASA_UF_MAP)
        for item in result:
            assert "nome" in item
            assert "uf" in item

    def test_sorted_by_name(self):
        result = api.lista_ceasas()
        names = [r["nome"] for r in result]
        assert names == sorted(names)


class TestCategorias:
    def test_returns_dict(self):
        result = api.categorias()
        assert isinstance(result, dict)
        assert "FRUTAS" in result
        assert "HORTALICAS" in result

    def test_counts(self):
        result = api.categorias()
        assert len(result["FRUTAS"]) == 20
        assert len(result["HORTALICAS"]) == 28
