from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from agrobr import ibge
from agrobr.ibge import client

GOLDEN_DIR = Path(__file__).parent.parent / "golden_data" / "ibge" / "ppm_bovino_sample"


class TestRebanhosMappings:
    def test_rebanhos_contains_bovino(self):
        assert "bovino" in client.REBANHOS_PPM
        assert client.REBANHOS_PPM["bovino"] == "2670"

    def test_rebanhos_contains_suino(self):
        assert "suino_total" in client.REBANHOS_PPM
        assert client.REBANHOS_PPM["suino_total"] == "32794"

    def test_rebanhos_contains_all_species(self):
        expected = [
            "bovino",
            "bubalino",
            "equino",
            "suino_total",
            "suino_matrizes",
            "caprino",
            "ovino",
            "galinaceos_total",
            "galinhas_poedeiras",
            "codornas",
        ]
        for especie in expected:
            assert especie in client.REBANHOS_PPM, f"{especie} not in REBANHOS_PPM"

    def test_rebanhos_codes_are_numeric_strings(self):
        for especie, code in client.REBANHOS_PPM.items():
            assert code.isdigit(), f"{especie}: {code} is not numeric"

    def test_rebanhos_has_10_species(self):
        assert len(client.REBANHOS_PPM) == 10


class TestProdutosOrigemAnimal:
    def test_contains_leite(self):
        assert "leite" in client.PRODUTOS_ORIGEM_ANIMAL
        assert client.PRODUTOS_ORIGEM_ANIMAL["leite"] == "2682"

    def test_contains_ovos_galinha(self):
        assert "ovos_galinha" in client.PRODUTOS_ORIGEM_ANIMAL
        assert client.PRODUTOS_ORIGEM_ANIMAL["ovos_galinha"] == "2685"

    def test_contains_mel(self):
        assert "mel" in client.PRODUTOS_ORIGEM_ANIMAL
        assert client.PRODUTOS_ORIGEM_ANIMAL["mel"] == "2687"

    def test_contains_all_products(self):
        expected = ["leite", "ovos_galinha", "ovos_codorna", "mel", "casulos", "la"]
        for prod in expected:
            assert prod in client.PRODUTOS_ORIGEM_ANIMAL, f"{prod} not in PRODUTOS_ORIGEM_ANIMAL"

    def test_has_6_products(self):
        assert len(client.PRODUTOS_ORIGEM_ANIMAL) == 6

    def test_codes_are_numeric_strings(self):
        for prod, code in client.PRODUTOS_ORIGEM_ANIMAL.items():
            assert code.isdigit(), f"{prod}: {code} is not numeric"


class TestUnidadesPPM:
    def test_rebanhos_have_cabecas(self):
        for especie in client.REBANHOS_PPM:
            assert client.UNIDADES_PPM[especie] == "cabeças"

    def test_leite_has_mil_litros(self):
        assert client.UNIDADES_PPM["leite"] == "mil litros"

    def test_ovos_have_mil_duzias(self):
        assert client.UNIDADES_PPM["ovos_galinha"] == "mil dúzias"
        assert client.UNIDADES_PPM["ovos_codorna"] == "mil dúzias"

    def test_mel_has_kg(self):
        assert client.UNIDADES_PPM["mel"] == "kg"

    def test_all_species_have_unit(self):
        all_species = list(client.REBANHOS_PPM.keys()) + list(client.PRODUTOS_ORIGEM_ANIMAL.keys())
        for especie in all_species:
            assert especie in client.UNIDADES_PPM, f"{especie} missing from UNIDADES_PPM"


class TestVariaveisPPM:
    def test_efetivo(self):
        assert client.VARIAVEIS_PPM["efetivo"] == "105"

    def test_producao(self):
        assert client.VARIAVEIS_PPM["producao"] == "106"

    def test_valor_producao(self):
        assert client.VARIAVEIS_PPM["valor_producao"] == "215"

    def test_codes_are_numeric(self):
        for var, code in client.VARIAVEIS_PPM.items():
            assert code.isdigit(), f"{var}: {code} is not numeric"


class TestTabelasPPM:
    def test_tabela_rebanho(self):
        assert "ppm_rebanho" in client.TABELAS
        assert client.TABELAS["ppm_rebanho"] == "3939"

    def test_tabela_producao(self):
        assert "ppm_producao" in client.TABELAS
        assert client.TABELAS["ppm_producao"] == "74"


class TestPpmValidation:
    @pytest.mark.asyncio
    async def test_especie_invalida(self):
        with pytest.raises(ValueError) as exc:
            await ibge.ppm("especie_inexistente")

        assert "não suportado" in str(exc.value)

    @pytest.mark.asyncio
    async def test_erro_lista_especies_disponiveis(self):
        with pytest.raises(ValueError) as exc:
            await ibge.ppm("xyz")

        msg = str(exc.value)
        assert "bovino" in msg
        assert "leite" in msg

    @pytest.mark.asyncio
    async def test_aceita_bovino(self):
        mock_df = pd.DataFrame(
            {
                "NC": ["3"],
                "NN": ["Unidade da Federação"],
                "MC": ["51"],
                "MN": ["Mato Grosso"],
                "V": ["33500000"],
                "D1C": ["2023"],
                "D1N": ["2023"],
                "D2C": ["105"],
                "D2N": ["Efetivo dos rebanhos"],
                "D3C": ["2670"],
                "D3N": ["Bovino"],
            }
        )
        with patch.object(client, "fetch_sidra", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = mock_df
            df = await ibge.ppm("bovino", ano=2023)
            assert isinstance(df, pd.DataFrame)
            mock_fetch.assert_called_once()

    @pytest.mark.asyncio
    async def test_aceita_leite(self):
        mock_df = pd.DataFrame(
            {
                "NC": ["3"],
                "NN": ["Unidade da Federação"],
                "MC": ["31"],
                "MN": ["Minas Gerais"],
                "V": ["9500000"],
                "D1C": ["2023"],
                "D1N": ["2023"],
                "D2C": ["106"],
                "D2N": ["Produção de origem animal"],
                "D3C": ["2682"],
                "D3N": ["Leite"],
            }
        )
        with patch.object(client, "fetch_sidra", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = mock_df
            df = await ibge.ppm("leite", ano=2023)
            assert isinstance(df, pd.DataFrame)
            mock_fetch.assert_called_once()


class TestPpmMocked:
    @pytest.fixture
    def mock_rebanho_response(self):
        return pd.DataFrame(
            {
                "NC": ["3", "3", "3"],
                "NN": ["Unidade da Federação"] * 3,
                "MC": ["51", "41", "43"],
                "MN": ["Mato Grosso", "Paraná", "Rio Grande do Sul"],
                "V": ["33500000", "9500000", "12500000"],
                "D1C": ["2023", "2023", "2023"],
                "D1N": ["2023", "2023", "2023"],
                "D2C": ["105", "105", "105"],
                "D2N": ["Efetivo dos rebanhos"] * 3,
                "D3C": ["2670", "2670", "2670"],
                "D3N": ["Bovino", "Bovino", "Bovino"],
            }
        )

    @pytest.fixture
    def mock_producao_response(self):
        return pd.DataFrame(
            {
                "NC": ["3", "3"],
                "NN": ["Unidade da Federação"] * 2,
                "MC": ["31", "52"],
                "MN": ["Minas Gerais", "Goiás"],
                "V": ["9500000", "4200000"],
                "D1C": ["2023", "2023"],
                "D1N": ["2023", "2023"],
                "D2C": ["106", "106"],
                "D2N": ["Produção de origem animal"] * 2,
                "D3C": ["2682", "2682"],
                "D3N": ["Leite", "Leite"],
            }
        )

    @pytest.mark.asyncio
    async def test_ppm_returns_dataframe(self, mock_rebanho_response):
        with patch.object(client, "fetch_sidra", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = mock_rebanho_response
            df = await ibge.ppm("bovino", ano=2023)
            assert isinstance(df, pd.DataFrame)
            assert len(df) == 3

    @pytest.mark.asyncio
    async def test_ppm_adds_especie_column(self, mock_rebanho_response):
        with patch.object(client, "fetch_sidra", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = mock_rebanho_response
            df = await ibge.ppm("bovino", ano=2023)
            assert "especie" in df.columns
            assert df["especie"].iloc[0] == "bovino"

    @pytest.mark.asyncio
    async def test_ppm_adds_fonte_column(self, mock_rebanho_response):
        with patch.object(client, "fetch_sidra", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = mock_rebanho_response
            df = await ibge.ppm("bovino", ano=2023)
            assert "fonte" in df.columns
            assert df["fonte"].iloc[0] == "ibge_ppm"

    @pytest.mark.asyncio
    async def test_ppm_adds_unidade_column(self, mock_rebanho_response):
        with patch.object(client, "fetch_sidra", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = mock_rebanho_response
            df = await ibge.ppm("bovino", ano=2023)
            assert "unidade" in df.columns
            assert df["unidade"].iloc[0] == "cabeças"

    @pytest.mark.asyncio
    async def test_ppm_leite_unidade(self, mock_producao_response):
        with patch.object(client, "fetch_sidra", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = mock_producao_response
            df = await ibge.ppm("leite", ano=2023)
            assert df["unidade"].iloc[0] == "mil litros"

    @pytest.mark.asyncio
    async def test_ppm_output_columns(self, mock_rebanho_response):
        with patch.object(client, "fetch_sidra", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = mock_rebanho_response
            df = await ibge.ppm("bovino", ano=2023)
            expected_cols = [
                "ano",
                "localidade",
                "localidade_cod",
                "especie",
                "valor",
                "unidade",
                "fonte",
            ]
            assert list(df.columns) == expected_cols

    @pytest.mark.asyncio
    async def test_ppm_valor_is_numeric(self, mock_rebanho_response):
        with patch.object(client, "fetch_sidra", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = mock_rebanho_response
            df = await ibge.ppm("bovino", ano=2023)
            assert pd.api.types.is_numeric_dtype(df["valor"])
            assert df["valor"].iloc[0] == 33500000.0

    @pytest.mark.asyncio
    async def test_ppm_rebanho_calls_table_3939(self, mock_rebanho_response):
        with patch.object(client, "fetch_sidra", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = mock_rebanho_response
            await ibge.ppm("bovino", ano=2023)
            call_args = mock_fetch.call_args
            assert call_args.kwargs["table_code"] == "3939"
            assert call_args.kwargs["variable"] == "105"
            assert call_args.kwargs["classifications"] == {"79": "2670"}

    @pytest.mark.asyncio
    async def test_ppm_producao_calls_table_74(self, mock_producao_response):
        with patch.object(client, "fetch_sidra", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = mock_producao_response
            await ibge.ppm("leite", ano=2023)
            call_args = mock_fetch.call_args
            assert call_args.kwargs["table_code"] == "74"
            assert call_args.kwargs["variable"] == "106"
            assert call_args.kwargs["classifications"] == {"80": "2682"}

    @pytest.mark.asyncio
    async def test_ppm_uf_filter(self, mock_rebanho_response):
        with patch.object(client, "fetch_sidra", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = mock_rebanho_response
            await ibge.ppm("bovino", ano=2023, uf="MT", nivel="uf")
            call_args = mock_fetch.call_args
            assert call_args.kwargs["territorial_level"] == "3"
            assert call_args.kwargs["ibge_territorial_code"] == "51"

    @pytest.mark.asyncio
    async def test_ppm_nivel_brasil(self, mock_rebanho_response):
        with patch.object(client, "fetch_sidra", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = mock_rebanho_response
            await ibge.ppm("bovino", ano=2023, nivel="brasil")
            call_args = mock_fetch.call_args
            assert call_args.kwargs["territorial_level"] == "1"

    @pytest.mark.asyncio
    async def test_ppm_nivel_municipio(self, mock_rebanho_response):
        with patch.object(client, "fetch_sidra", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = mock_rebanho_response
            await ibge.ppm("bovino", ano=2023, nivel="municipio")
            call_args = mock_fetch.call_args
            assert call_args.kwargs["territorial_level"] == "6"

    @pytest.mark.asyncio
    async def test_ppm_list_of_years(self, mock_rebanho_response):
        with patch.object(client, "fetch_sidra", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = mock_rebanho_response
            await ibge.ppm("bovino", ano=[2021, 2022, 2023])
            call_args = mock_fetch.call_args
            assert call_args.kwargs["period"] == "2021,2022,2023"

    @pytest.mark.asyncio
    async def test_ppm_no_ano_uses_last(self, mock_rebanho_response):
        with patch.object(client, "fetch_sidra", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = mock_rebanho_response
            await ibge.ppm("bovino")
            call_args = mock_fetch.call_args
            assert call_args.kwargs["period"] == "last"

    @pytest.mark.asyncio
    async def test_ppm_return_meta(self, mock_rebanho_response):
        with patch.object(client, "fetch_sidra", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = mock_rebanho_response
            result = await ibge.ppm("bovino", ano=2023, return_meta=True)
            assert isinstance(result, tuple)
            df, meta = result
            assert isinstance(df, pd.DataFrame)
            assert meta.source == "ibge_ppm"
            assert meta.records_count == 3


class TestEspeciesPpm:
    @pytest.mark.asyncio
    async def test_especies_ppm_returns_list(self):
        especies = await ibge.especies_ppm()
        assert isinstance(especies, list)
        assert len(especies) == 16

    @pytest.mark.asyncio
    async def test_especies_ppm_contains_rebanhos(self):
        especies = await ibge.especies_ppm()
        assert "bovino" in especies
        assert "suino_total" in especies
        assert "galinaceos_total" in especies

    @pytest.mark.asyncio
    async def test_especies_ppm_contains_producao(self):
        especies = await ibge.especies_ppm()
        assert "leite" in especies
        assert "ovos_galinha" in especies
        assert "mel" in especies


class TestPpmPolarsSupport:
    @pytest.fixture
    def mock_response(self):
        return pd.DataFrame(
            {
                "NC": ["3"],
                "NN": ["Unidade da Federação"],
                "MC": ["51"],
                "MN": ["Mato Grosso"],
                "V": ["33500000"],
                "D1C": ["2023"],
                "D1N": ["2023"],
                "D2C": ["105"],
                "D2N": ["Efetivo dos rebanhos"],
                "D3C": ["2670"],
                "D3N": ["Bovino"],
            }
        )

    @pytest.mark.asyncio
    async def test_ppm_polars_conversion(self, mock_response):
        pytest.importorskip("polars")
        with patch.object(client, "fetch_sidra", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = mock_response
            df = await ibge.ppm("bovino", ano=2023, as_polars=True)
            import polars as pl

            assert isinstance(df, pl.DataFrame)

    @pytest.mark.asyncio
    async def test_ppm_polars_fallback_pandas(self, mock_response, monkeypatch):
        import builtins

        real_import = builtins.__import__

        def mock_import(name, *args, **kwargs):
            if name == "polars":
                raise ImportError("No module named 'polars'")
            return real_import(name, *args, **kwargs)

        with patch.object(client, "fetch_sidra", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = mock_response
            monkeypatch.setattr(builtins, "__import__", mock_import)
            df = await ibge.ppm("bovino", ano=2023, as_polars=True)
            assert isinstance(df, pd.DataFrame)


class TestPpmGoldenData:
    def test_golden_data_exists(self):
        assert GOLDEN_DIR.exists()
        assert (GOLDEN_DIR / "metadata.json").exists()
        assert (GOLDEN_DIR / "response.csv").exists()
        assert (GOLDEN_DIR / "expected.json").exists()

    def test_golden_metadata(self):
        meta = json.loads((GOLDEN_DIR / "metadata.json").read_text(encoding="utf-8"))
        assert meta["source"] == "ibge"
        assert meta["query"]["table"] == "3939"
        assert meta["query"]["variable"] == "105"
        assert meta["query"]["classification_79"] == "2670"

    def test_golden_response_parseable(self):
        df = pd.read_csv(GOLDEN_DIR / "response.csv")
        assert len(df) == 27
        assert "V" in df.columns
        assert "MN" in df.columns

    def test_golden_parse_matches_expected(self):
        df = pd.read_csv(GOLDEN_DIR / "response.csv")
        parsed = client.parse_sidra_response(df)
        expected = json.loads((GOLDEN_DIR / "expected.json").read_text(encoding="utf-8"))

        assert len(parsed) == expected["count"]
        assert parsed["localidade"].iloc[0] == expected["first_row"]["localidade"]
        assert parsed["valor"].iloc[0] == expected["first_row"]["valor"]
        assert parsed["localidade"].iloc[-1] == expected["last_row"]["localidade"]
        assert parsed["valor"].iloc[-1] == expected["last_row"]["valor"]

    def test_golden_all_values_positive(self):
        df = pd.read_csv(GOLDEN_DIR / "response.csv")
        parsed = client.parse_sidra_response(df)
        assert (parsed["valor"].dropna() >= 0).all()


class TestPpmContract:
    def test_contract_registered(self):
        from agrobr.contracts import has_contract

        assert has_contract("pecuaria_municipal")

    def test_contract_validates_valid_df(self):
        from agrobr.contracts import validate_dataset

        df = pd.DataFrame(
            {
                "ano": [2023, 2023],
                "localidade": ["Mato Grosso", "Paraná"],
                "localidade_cod": [51, 41],
                "especie": ["bovino", "bovino"],
                "valor": [33500000.0, 9500000.0],
                "unidade": ["cabeças", "cabeças"],
                "fonte": ["ibge_ppm", "ibge_ppm"],
            }
        )
        validate_dataset(df, "pecuaria_municipal")

    def test_contract_rejects_negative_values(self):
        from agrobr.contracts import validate_dataset
        from agrobr.exceptions import ContractViolationError

        df = pd.DataFrame(
            {
                "ano": [2023],
                "localidade": ["Mato Grosso"],
                "localidade_cod": [51],
                "especie": ["bovino"],
                "valor": [-100.0],
                "unidade": ["cabeças"],
                "fonte": ["ibge_ppm"],
            }
        )
        with pytest.raises(ContractViolationError):
            validate_dataset(df, "pecuaria_municipal")


class TestPpmDataset:
    def test_dataset_registered(self):
        from agrobr.datasets.registry import list_datasets

        datasets = list_datasets()
        assert "pecuaria_municipal" in datasets

    def test_dataset_info(self):
        from agrobr.datasets.registry import info

        i = info("pecuaria_municipal")
        assert i["name"] == "pecuaria_municipal"
        assert i["source_institution"] == "IBGE"
        assert i["license"] == "livre"
        assert "bovino" in i["products"]
        assert "leite" in i["products"]

    def test_dataset_has_16_products(self):
        from agrobr.datasets.registry import list_products

        products = list_products("pecuaria_municipal")
        assert len(products) == 16

    def test_dataset_sources(self):
        from agrobr.datasets.registry import info

        i = info("pecuaria_municipal")
        assert "ibge_ppm" in i["sources"]


class TestPpmCachePolicy:
    def test_ppm_cache_policy_exists(self):
        from agrobr.cache.policies import POLICIES

        assert "ibge_ppm" in POLICIES

    def test_ppm_cache_ttl_7_days(self):
        from agrobr.cache.policies import POLICIES

        policy = POLICIES["ibge_ppm"]
        assert policy.ttl_seconds == 7 * 24 * 3600

    def test_ppm_cache_stale_90_days(self):
        from agrobr.cache.policies import POLICIES

        policy = POLICIES["ibge_ppm"]
        assert policy.stale_max_seconds == 90 * 24 * 3600


@pytest.mark.integration
class TestPpmIntegration:
    @pytest.mark.asyncio
    @pytest.mark.slow
    async def test_ppm_bovino_real(self):
        df = await ibge.ppm("bovino", ano=2022, nivel="brasil")
        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert "especie" in df.columns
        assert df["especie"].iloc[0] == "bovino"

    @pytest.mark.asyncio
    @pytest.mark.slow
    async def test_ppm_leite_real(self):
        df = await ibge.ppm("leite", ano=2022, nivel="brasil")
        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert df["unidade"].iloc[0] == "mil litros"
