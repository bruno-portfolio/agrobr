from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from agrobr.ibge import client

GOLDEN_DIR = Path(__file__).parent.parent / "golden_data" / "ibge" / "censo_agro_efetivo_sample"


class TestTabelasCensoAgro:
    def test_efetivo_rebanho(self):
        assert client.TABELAS_CENSO_AGRO["efetivo_rebanho"] == "6907"

    def test_uso_terra(self):
        assert client.TABELAS_CENSO_AGRO["uso_terra"] == "6881"

    def test_lavoura_temporaria(self):
        assert client.TABELAS_CENSO_AGRO["lavoura_temporaria"] == "6957"

    def test_lavoura_permanente(self):
        assert client.TABELAS_CENSO_AGRO["lavoura_permanente"] == "6956"

    def test_all_codes_numeric(self):
        for name, code in client.TABELAS_CENSO_AGRO.items():
            assert code.isdigit(), f"{name} has non-numeric code: {code}"


class TestVariaveisCensoAgro:
    def test_efetivo_has_cabecas(self):
        assert "cabecas" in client.VARIAVEIS_CENSO_AGRO["efetivo_rebanho"]
        assert client.VARIAVEIS_CENSO_AGRO["efetivo_rebanho"]["cabecas"] == "2209"

    def test_uso_terra_has_area(self):
        assert "area" in client.VARIAVEIS_CENSO_AGRO["uso_terra"]
        assert client.VARIAVEIS_CENSO_AGRO["uso_terra"]["area"] == "184"

    def test_lavoura_temp_has_producao(self):
        assert "producao" in client.VARIAVEIS_CENSO_AGRO["lavoura_temporaria"]

    def test_lavoura_perm_has_producao(self):
        assert "producao" in client.VARIAVEIS_CENSO_AGRO["lavoura_permanente"]

    def test_all_themes_have_estabelecimentos(self):
        for tema, vars_map in client.VARIAVEIS_CENSO_AGRO.items():
            assert "estabelecimentos" in vars_map, f"{tema} missing estabelecimentos"

    def test_all_codes_numeric(self):
        for tema, vars_map in client.VARIAVEIS_CENSO_AGRO.items():
            for name, code in vars_map.items():
                assert code.isdigit(), f"{tema}.{name} has non-numeric code: {code}"


class TestTemasCensoAgro:
    def test_has_4_themes(self):
        assert len(client.TEMAS_CENSO_AGRO) == 4

    def test_efetivo_in_temas(self):
        assert "efetivo_rebanho" in client.TEMAS_CENSO_AGRO

    def test_uso_terra_in_temas(self):
        assert "uso_terra" in client.TEMAS_CENSO_AGRO

    def test_temas_match_tabelas(self):
        assert set(client.TEMAS_CENSO_AGRO) == set(client.TABELAS_CENSO_AGRO.keys())


class TestCensoAgroValidation:
    @pytest.mark.asyncio
    async def test_tema_invalido(self):
        from agrobr.ibge.api import censo_agro

        with pytest.raises(ValueError, match="Tema não suportado"):
            await censo_agro("tema_inexistente")


def _build_mock_efetivo(n_ufs=3):
    rows = []
    ufs = [("35", "São Paulo"), ("51", "Mato Grosso"), ("52", "Goiás")][:n_ufs]
    species = [("110056", "Bovinos"), ("110062", "Ovinos")]
    for cod_uf, nome_uf in ufs:
        for cod_sp, nome_sp in species:
            rows.append(
                {
                    "NC": "3",
                    "NN": "Unidade da Federação",
                    "MC": "24",
                    "MN": "Cabeças",
                    "V": str(1000000 + int(cod_uf) * 100),
                    "D1C": cod_uf,
                    "D1N": nome_uf,
                    "D2C": "2209",
                    "D2N": "Número de cabeças",
                    "D3C": "2017",
                    "D3N": "2017",
                    "D4C": "46302",
                    "D4N": "Total",
                    "D5C": cod_sp,
                    "D5N": nome_sp,
                    "D6C": "46502",
                    "D6N": "Total",
                }
            )
            rows.append(
                {
                    "NC": "3",
                    "NN": "Unidade da Federação",
                    "MC": "1020",
                    "MN": "Unidades",
                    "V": str(50000 + int(cod_uf) * 10),
                    "D1C": cod_uf,
                    "D1N": nome_uf,
                    "D2C": "10010",
                    "D2N": "Número de estabelecimentos",
                    "D3C": "2017",
                    "D3N": "2017",
                    "D4C": "46302",
                    "D4N": "Total",
                    "D5C": cod_sp,
                    "D5N": nome_sp,
                    "D6C": "46502",
                    "D6N": "Total",
                }
            )
    return pd.DataFrame(rows)


def _build_mock_uso_terra(n_ufs=2):
    rows = []
    ufs = [("35", "São Paulo"), ("51", "Mato Grosso")][:n_ufs]
    usos = [
        ("111543", "Lavouras temporárias"),
        ("111544", "Pastagens naturais"),
    ]
    for cod_uf, nome_uf in ufs:
        for cod_uso, nome_uso in usos:
            rows.append(
                {
                    "NC": "3",
                    "NN": "Unidade da Federação",
                    "MC": "1019",
                    "MN": "Hectares",
                    "V": str(500000 + int(cod_uf) * 100),
                    "D1C": cod_uf,
                    "D1N": nome_uf,
                    "D2C": "184",
                    "D2N": "Área dos estabelecimentos agropecuários",
                    "D3C": "2017",
                    "D3N": "2017",
                    "D4C": "46302",
                    "D4N": "Total",
                    "D5C": cod_uso,
                    "D5N": nome_uso,
                    "D6C": "46502",
                    "D6N": "Total",
                    "D7C": "113601",
                    "D7N": "Total",
                    "D8C": "41151",
                    "D8N": "Total",
                }
            )
            rows.append(
                {
                    "NC": "3",
                    "NN": "Unidade da Federação",
                    "MC": "1020",
                    "MN": "Unidades",
                    "V": str(10000 + int(cod_uf)),
                    "D1C": cod_uf,
                    "D1N": nome_uf,
                    "D2C": "9587",
                    "D2N": "Número de estabelecimentos agropecuários com área",
                    "D3C": "2017",
                    "D3N": "2017",
                    "D4C": "46302",
                    "D4N": "Total",
                    "D5C": cod_uso,
                    "D5N": nome_uso,
                    "D6C": "46502",
                    "D6N": "Total",
                    "D7C": "113601",
                    "D7N": "Total",
                    "D8C": "41151",
                    "D8N": "Total",
                }
            )
    return pd.DataFrame(rows)


class TestCensoAgroMocked:
    @pytest.mark.asyncio
    async def test_efetivo_returns_dataframe(self):
        from agrobr.ibge.api import censo_agro

        with patch("agrobr.ibge.client.fetch_sidra", new_callable=AsyncMock) as mock:
            mock.return_value = _build_mock_efetivo()
            df = await censo_agro("efetivo_rebanho")
            assert isinstance(df, pd.DataFrame)
            assert len(df) > 0

    @pytest.mark.asyncio
    async def test_efetivo_output_columns(self):
        from agrobr.ibge.api import censo_agro

        with patch("agrobr.ibge.client.fetch_sidra", new_callable=AsyncMock) as mock:
            mock.return_value = _build_mock_efetivo()
            df = await censo_agro("efetivo_rebanho")
            expected = [
                "ano",
                "localidade",
                "localidade_cod",
                "tema",
                "categoria",
                "variavel",
                "valor",
                "unidade",
                "fonte",
            ]
            assert list(df.columns) == expected

    @pytest.mark.asyncio
    async def test_efetivo_tema_value(self):
        from agrobr.ibge.api import censo_agro

        with patch("agrobr.ibge.client.fetch_sidra", new_callable=AsyncMock) as mock:
            mock.return_value = _build_mock_efetivo()
            df = await censo_agro("efetivo_rebanho")
            assert (df["tema"] == "efetivo_rebanho").all()

    @pytest.mark.asyncio
    async def test_efetivo_fonte_value(self):
        from agrobr.ibge.api import censo_agro

        with patch("agrobr.ibge.client.fetch_sidra", new_callable=AsyncMock) as mock:
            mock.return_value = _build_mock_efetivo()
            df = await censo_agro("efetivo_rebanho")
            assert (df["fonte"] == "ibge_censo_agro").all()

    @pytest.mark.asyncio
    async def test_efetivo_ano_2017(self):
        from agrobr.ibge.api import censo_agro

        with patch("agrobr.ibge.client.fetch_sidra", new_callable=AsyncMock) as mock:
            mock.return_value = _build_mock_efetivo()
            df = await censo_agro("efetivo_rebanho")
            assert (df["ano"] == 2017).all()

    @pytest.mark.asyncio
    async def test_efetivo_categorias(self):
        from agrobr.ibge.api import censo_agro

        with patch("agrobr.ibge.client.fetch_sidra", new_callable=AsyncMock) as mock:
            mock.return_value = _build_mock_efetivo()
            df = await censo_agro("efetivo_rebanho")
            categorias = df["categoria"].unique()
            assert "Bovinos" in categorias
            assert "Ovinos" in categorias

    @pytest.mark.asyncio
    async def test_efetivo_variaveis(self):
        from agrobr.ibge.api import censo_agro

        with patch("agrobr.ibge.client.fetch_sidra", new_callable=AsyncMock) as mock:
            mock.return_value = _build_mock_efetivo()
            df = await censo_agro("efetivo_rebanho")
            variaveis = df["variavel"].unique()
            assert "cabecas" in variaveis
            assert "estabelecimentos" in variaveis

    @pytest.mark.asyncio
    async def test_efetivo_unidades(self):
        from agrobr.ibge.api import censo_agro

        with patch("agrobr.ibge.client.fetch_sidra", new_callable=AsyncMock) as mock:
            mock.return_value = _build_mock_efetivo()
            df = await censo_agro("efetivo_rebanho")
            cabecas = df[df["variavel"] == "cabecas"]
            assert (cabecas["unidade"] == "cabeças").all()
            estab = df[df["variavel"] == "estabelecimentos"]
            assert (estab["unidade"] == "unidades").all()

    @pytest.mark.asyncio
    async def test_efetivo_valor_numerico(self):
        from agrobr.ibge.api import censo_agro

        with patch("agrobr.ibge.client.fetch_sidra", new_callable=AsyncMock) as mock:
            mock.return_value = _build_mock_efetivo()
            df = await censo_agro("efetivo_rebanho")
            assert pd.api.types.is_numeric_dtype(df["valor"])
            assert (df["valor"] > 0).all()

    @pytest.mark.asyncio
    async def test_efetivo_localidade_cod_inteiro(self):
        from agrobr.ibge.api import censo_agro

        with patch("agrobr.ibge.client.fetch_sidra", new_callable=AsyncMock) as mock:
            mock.return_value = _build_mock_efetivo()
            df = await censo_agro("efetivo_rebanho")
            assert df["localidade_cod"].dtype == "Int64"

    @pytest.mark.asyncio
    async def test_efetivo_return_meta(self):
        from agrobr.ibge.api import censo_agro

        with patch("agrobr.ibge.client.fetch_sidra", new_callable=AsyncMock) as mock:
            mock.return_value = _build_mock_efetivo()
            result = await censo_agro("efetivo_rebanho", return_meta=True)
            assert isinstance(result, tuple)
            df, meta = result
            assert isinstance(df, pd.DataFrame)
            assert meta.source == "ibge_censo_agro"

    @pytest.mark.asyncio
    async def test_efetivo_uf_filter(self):
        from agrobr.ibge.api import censo_agro

        with patch("agrobr.ibge.client.fetch_sidra", new_callable=AsyncMock) as mock:
            mock.return_value = _build_mock_efetivo(n_ufs=1)
            df = await censo_agro("efetivo_rebanho", uf="SP")
            assert len(df) > 0
            mock.assert_called_once()
            call_kwargs = mock.call_args
            assert call_kwargs.kwargs.get("ibge_territorial_code") == "35"

    @pytest.mark.asyncio
    async def test_uso_terra_returns_dataframe(self):
        from agrobr.ibge.api import censo_agro

        with patch("agrobr.ibge.client.fetch_sidra", new_callable=AsyncMock) as mock:
            mock.return_value = _build_mock_uso_terra()
            df = await censo_agro("uso_terra")
            assert isinstance(df, pd.DataFrame)
            assert len(df) > 0
            assert (df["tema"] == "uso_terra").all()

    @pytest.mark.asyncio
    async def test_uso_terra_variaveis(self):
        from agrobr.ibge.api import censo_agro

        with patch("agrobr.ibge.client.fetch_sidra", new_callable=AsyncMock) as mock:
            mock.return_value = _build_mock_uso_terra()
            df = await censo_agro("uso_terra")
            variaveis = df["variavel"].unique()
            assert "area" in variaveis
            assert "estabelecimentos" in variaveis

    @pytest.mark.asyncio
    async def test_uso_terra_unidade_area(self):
        from agrobr.ibge.api import censo_agro

        with patch("agrobr.ibge.client.fetch_sidra", new_callable=AsyncMock) as mock:
            mock.return_value = _build_mock_uso_terra()
            df = await censo_agro("uso_terra")
            area = df[df["variavel"] == "area"]
            assert (area["unidade"] == "hectares").all()

    @pytest.mark.asyncio
    async def test_empty_response(self):
        from agrobr.ibge.api import censo_agro

        with patch("agrobr.ibge.client.fetch_sidra", new_callable=AsyncMock) as mock:
            mock.return_value = pd.DataFrame()
            df = await censo_agro("efetivo_rebanho")
            assert isinstance(df, pd.DataFrame)
            assert len(df) == 0

    @pytest.mark.asyncio
    async def test_total_filtered_out(self):
        from agrobr.ibge.api import censo_agro

        mock_df = _build_mock_efetivo(n_ufs=1)
        total_row = mock_df.iloc[0:1].copy()
        total_row["D5C"] = "111197"
        total_row["D5N"] = "Total"
        mock_with_total = pd.concat([total_row, mock_df], ignore_index=True)

        with patch("agrobr.ibge.client.fetch_sidra", new_callable=AsyncMock) as mock:
            mock.return_value = mock_with_total
            df = await censo_agro("efetivo_rebanho")
            assert "Total" not in df["categoria"].values


class TestTemasCensoAgroFunc:
    @pytest.mark.asyncio
    async def test_returns_list(self):
        from agrobr.ibge.api import temas_censo_agro

        result = await temas_censo_agro()
        assert isinstance(result, list)
        assert len(result) == 4

    @pytest.mark.asyncio
    async def test_contains_all_themes(self):
        from agrobr.ibge.api import temas_censo_agro

        result = await temas_censo_agro()
        assert "efetivo_rebanho" in result
        assert "uso_terra" in result
        assert "lavoura_temporaria" in result
        assert "lavoura_permanente" in result


class TestCensoAgroPolarsSupport:
    @pytest.mark.asyncio
    async def test_as_polars_returns_polars_df(self):
        pytest.importorskip("polars")
        import polars as pl

        from agrobr.ibge.api import censo_agro

        with patch("agrobr.ibge.client.fetch_sidra", new_callable=AsyncMock) as mock:
            mock.return_value = _build_mock_efetivo(n_ufs=1)
            df = await censo_agro("efetivo_rebanho", as_polars=True)
            assert isinstance(df, pl.DataFrame)

    @pytest.mark.asyncio
    async def test_as_polars_with_meta(self):
        pytest.importorskip("polars")
        import polars as pl

        from agrobr.ibge.api import censo_agro

        with patch("agrobr.ibge.client.fetch_sidra", new_callable=AsyncMock) as mock:
            mock.return_value = _build_mock_efetivo(n_ufs=1)
            result = await censo_agro("efetivo_rebanho", as_polars=True, return_meta=True)
            assert isinstance(result, tuple)
            assert isinstance(result[0], pl.DataFrame)


class TestCensoAgroGoldenData:
    def test_golden_data_exists(self):
        assert GOLDEN_DIR.exists()
        assert (GOLDEN_DIR / "metadata.json").exists()
        assert (GOLDEN_DIR / "response.csv").exists()
        assert (GOLDEN_DIR / "expected.json").exists()

    def test_golden_metadata_valid(self):
        meta = json.loads((GOLDEN_DIR / "metadata.json").read_text(encoding="utf-8"))
        assert meta["source"] == "ibge_censo_agro"
        assert meta["table"] == "6907"
        assert meta["tema"] == "efetivo_rebanho"

    def test_golden_response_parseable(self):
        df = pd.read_csv(GOLDEN_DIR / "response.csv", dtype=str)
        assert len(df) > 0
        assert "D5N" in df.columns

    def test_golden_expected_format(self):
        expected = json.loads((GOLDEN_DIR / "expected.json").read_text(encoding="utf-8"))
        assert "row_count" in expected
        assert "columns" in expected
        assert "sample_values" in expected
        assert expected["tema"] == "efetivo_rebanho"

    def test_golden_row_count_matches(self):
        expected = json.loads((GOLDEN_DIR / "expected.json").read_text(encoding="utf-8"))
        df = pd.read_csv(GOLDEN_DIR / "response.csv", dtype=str)
        assert len(df) == expected["row_count"]


class TestCensoAgroContract:
    def test_contract_registered(self):
        from agrobr.contracts import get_contract

        contract = get_contract("censo_agropecuario")
        assert contract is not None
        assert contract.name == "ibge.censo_agro"

    def test_contract_validates_valid_df(self):
        from agrobr.contracts import validate_dataset

        df = pd.DataFrame(
            {
                "ano": [2017, 2017],
                "localidade": ["São Paulo", "São Paulo"],
                "localidade_cod": [35, 35],
                "tema": ["efetivo_rebanho", "efetivo_rebanho"],
                "categoria": ["Bovinos", "Bovinos"],
                "variavel": ["cabecas", "estabelecimentos"],
                "valor": [10391878.0, 131234.0],
                "unidade": ["cabeças", "unidades"],
                "fonte": ["ibge_censo_agro", "ibge_censo_agro"],
            }
        )
        validate_dataset(df, "censo_agropecuario")

    def test_contract_rejects_negative_values(self):
        from agrobr.contracts import validate_dataset
        from agrobr.exceptions import ContractViolationError

        df = pd.DataFrame(
            {
                "ano": [2017],
                "localidade": ["Test"],
                "localidade_cod": [11],
                "tema": ["efetivo_rebanho"],
                "categoria": ["Bovinos"],
                "variavel": ["cabecas"],
                "valor": [-100.0],
                "unidade": ["cabeças"],
                "fonte": ["ibge_censo_agro"],
            }
        )
        with pytest.raises(ContractViolationError):
            validate_dataset(df, "censo_agropecuario")


class TestCensoAgroDataset:
    def test_dataset_registered(self):
        from agrobr.datasets.registry import list_datasets

        datasets = list_datasets()
        assert "censo_agropecuario" in datasets

    def test_dataset_info(self):
        from agrobr.datasets.censo_agropecuario import CENSO_AGROPECUARIO_INFO

        assert CENSO_AGROPECUARIO_INFO.name == "censo_agropecuario"
        assert CENSO_AGROPECUARIO_INFO.update_frequency == "decennial"

    def test_dataset_products(self):
        from agrobr.datasets.censo_agropecuario import CENSO_AGROPECUARIO_INFO

        assert "efetivo_rebanho" in CENSO_AGROPECUARIO_INFO.products
        assert "uso_terra" in CENSO_AGROPECUARIO_INFO.products

    def test_dataset_source(self):
        from agrobr.datasets.censo_agropecuario import CENSO_AGROPECUARIO_INFO

        assert len(CENSO_AGROPECUARIO_INFO.sources) == 1
        assert CENSO_AGROPECUARIO_INFO.sources[0].name == "ibge_censo_agro"


class TestCensoAgroCachePolicy:
    def test_policy_exists(self):
        from agrobr.cache.policies import POLICIES

        assert "ibge_censo_agro" in POLICIES

    def test_policy_ttl_30_days(self):
        from agrobr.cache.policies import POLICIES, TTL

        policy = POLICIES["ibge_censo_agro"]
        assert policy.ttl_seconds == TTL.DAYS_30.value

    def test_policy_stale_90_days(self):
        from agrobr.cache.policies import POLICIES, TTL

        policy = POLICIES["ibge_censo_agro"]
        assert policy.stale_max_seconds == TTL.DAYS_90.value


class TestCensoAgroIntegration:
    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_efetivo_rebanho_real_api(self):
        from agrobr.ibge.api import censo_agro

        df = await censo_agro("efetivo_rebanho", uf="SP", nivel="uf")
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0
        assert "categoria" in df.columns
        assert (df["tema"] == "efetivo_rebanho").all()
        assert (df["fonte"] == "ibge_censo_agro").all()

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_uso_terra_real_api(self):
        from agrobr.ibge.api import censo_agro

        df = await censo_agro("uso_terra", uf="MT", nivel="uf")
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0
        assert (df["tema"] == "uso_terra").all()
