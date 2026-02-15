from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from agrobr.ibge import client

GOLDEN_DIR = Path(__file__).parent.parent / "golden_data" / "ibge" / "abate_bovino_sample"


class TestTabelasAbate:
    def test_tabela_bovino(self):
        assert client.TABELAS_ABATE["bovino"] == "1092"

    def test_tabela_suino(self):
        assert client.TABELAS_ABATE["suino"] == "1093"

    def test_tabela_frango(self):
        assert client.TABELAS_ABATE["frango"] == "1094"

    def test_all_tables_are_numeric_strings(self):
        for name, code in client.TABELAS_ABATE.items():
            assert code.isdigit(), f"{name} has non-numeric code: {code}"


class TestVariaveisAbate:
    def test_animais_abatidos(self):
        assert client.VARIAVEIS_ABATE["animais_abatidos"] == "284"

    def test_peso_carcacas(self):
        assert client.VARIAVEIS_ABATE["peso_carcacas"] == "285"

    def test_codes_are_numeric(self):
        for name, code in client.VARIAVEIS_ABATE.items():
            assert code.isdigit(), f"{name} has non-numeric code: {code}"


class TestEspeciesAbate:
    def test_contains_bovino(self):
        assert "bovino" in client.ESPECIES_ABATE

    def test_contains_suino(self):
        assert "suino" in client.ESPECIES_ABATE

    def test_contains_frango(self):
        assert "frango" in client.ESPECIES_ABATE

    def test_has_3_species(self):
        assert len(client.ESPECIES_ABATE) == 3


class TestCategoriasAbate:
    def test_ref_trimestre(self):
        assert client.CATEGORIAS_ABATE["ref_trimestre"]["12716"] == "115236"

    def test_tipo_rebanho_bovino(self):
        assert client.CATEGORIAS_ABATE["tipo_rebanho_bovino"]["18"] == "992"

    def test_tipo_inspecao(self):
        assert client.CATEGORIAS_ABATE["tipo_inspecao"]["12529"] == "118225"


class TestAbateValidation:
    async def test_especie_invalida(self):
        from agrobr.ibge.api import abate

        with pytest.raises(ValueError, match="Espécie não suportada"):
            await abate("cavalo")

    async def test_erro_lista_especies(self):
        from agrobr.ibge.api import abate

        with pytest.raises(ValueError, match="bovino"):
            await abate("lagosta")

    async def test_aceita_bovino(self):
        from agrobr.ibge.api import abate

        with patch("agrobr.ibge.client.fetch_sidra", new_callable=AsyncMock) as mock:
            mock.return_value = _build_mock_df(1)
            df = await abate("bovino", trimestre="202303")
            assert isinstance(df, pd.DataFrame)

    async def test_aceita_suino(self):
        from agrobr.ibge.api import abate

        with patch("agrobr.ibge.client.fetch_sidra", new_callable=AsyncMock) as mock:
            mock.return_value = _build_mock_df(1)
            df = await abate("suino", trimestre="202303")
            assert isinstance(df, pd.DataFrame)


def _build_mock_df(n_ufs=3):
    rows = []
    ufs = [("11", "Rondônia"), ("51", "Mato Grosso"), ("35", "São Paulo")][:n_ufs]
    for cod, nome in ufs:
        rows.append(
            {
                "D1C": cod,
                "D1N": nome,
                "D2C": "284",
                "D2N": "Animais abatidos",
                "D3C": "202303",
                "D3N": "3º trimestre 2023",
                "V": str(100000 + int(cod) * 1000),
                "NC": "3",
                "NN": "Unidade da Federação",
                "MC": "24",
                "MN": "Cabeças",
                "D4C": "115236",
                "D4N": "Total do trimestre",
                "D5C": "992",
                "D5N": "Total",
                "D6C": "118225",
                "D6N": "Total",
            }
        )
        rows.append(
            {
                "D1C": cod,
                "D1N": nome,
                "D2C": "285",
                "D2N": "Peso total das carcaças",
                "D3C": "202303",
                "D3N": "3º trimestre 2023",
                "V": str(50000000 + int(cod) * 100000),
                "NC": "3",
                "NN": "Unidade da Federação",
                "MC": "1007",
                "MN": "Quilogramas",
                "D4C": "115236",
                "D4N": "Total do trimestre",
                "D5C": "992",
                "D5N": "Total",
                "D6C": "118225",
                "D6N": "Total",
            }
        )
    return pd.DataFrame(rows)


class TestAbateMocked:
    async def test_returns_dataframe(self):
        from agrobr.ibge.api import abate

        with patch("agrobr.ibge.client.fetch_sidra", new_callable=AsyncMock) as mock:
            mock.return_value = _build_mock_df()
            df = await abate("bovino", trimestre="202303")
            assert isinstance(df, pd.DataFrame)
            assert len(df) > 0

    async def test_output_columns(self):
        from agrobr.ibge.api import abate

        with patch("agrobr.ibge.client.fetch_sidra", new_callable=AsyncMock) as mock:
            mock.return_value = _build_mock_df()
            df = await abate("bovino", trimestre="202303")
            expected = [
                "trimestre",
                "localidade",
                "localidade_cod",
                "especie",
                "animais_abatidos",
                "peso_carcacas",
                "fonte",
            ]
            assert list(df.columns) == expected

    async def test_adds_especie_column(self):
        from agrobr.ibge.api import abate

        with patch("agrobr.ibge.client.fetch_sidra", new_callable=AsyncMock) as mock:
            mock.return_value = _build_mock_df()
            df = await abate("bovino", trimestre="202303")
            assert (df["especie"] == "bovino").all()

    async def test_adds_fonte_column(self):
        from agrobr.ibge.api import abate

        with patch("agrobr.ibge.client.fetch_sidra", new_callable=AsyncMock) as mock:
            mock.return_value = _build_mock_df()
            df = await abate("bovino", trimestre="202303")
            assert (df["fonte"] == "ibge_abate").all()

    async def test_animais_abatidos_numeric(self):
        from agrobr.ibge.api import abate

        with patch("agrobr.ibge.client.fetch_sidra", new_callable=AsyncMock) as mock:
            mock.return_value = _build_mock_df()
            df = await abate("bovino", trimestre="202303")
            assert pd.api.types.is_numeric_dtype(df["animais_abatidos"])

    async def test_peso_carcacas_numeric(self):
        from agrobr.ibge.api import abate

        with patch("agrobr.ibge.client.fetch_sidra", new_callable=AsyncMock) as mock:
            mock.return_value = _build_mock_df()
            df = await abate("bovino", trimestre="202303")
            assert pd.api.types.is_numeric_dtype(df["peso_carcacas"])

    async def test_bovino_calls_table_1092(self):
        from agrobr.ibge.api import abate

        with patch("agrobr.ibge.client.fetch_sidra", new_callable=AsyncMock) as mock:
            mock.return_value = _build_mock_df()
            await abate("bovino", trimestre="202303")
            call_kwargs = mock.call_args.kwargs
            assert call_kwargs["table_code"] == "1092"

    async def test_suino_calls_table_1093(self):
        from agrobr.ibge.api import abate

        with patch("agrobr.ibge.client.fetch_sidra", new_callable=AsyncMock) as mock:
            mock.return_value = _build_mock_df()
            await abate("suino", trimestre="202303")
            call_kwargs = mock.call_args.kwargs
            assert call_kwargs["table_code"] == "1093"

    async def test_frango_calls_table_1094(self):
        from agrobr.ibge.api import abate

        with patch("agrobr.ibge.client.fetch_sidra", new_callable=AsyncMock) as mock:
            mock.return_value = _build_mock_df()
            await abate("frango", trimestre="202303")
            call_kwargs = mock.call_args.kwargs
            assert call_kwargs["table_code"] == "1094"

    async def test_bovino_includes_classification_18(self):
        from agrobr.ibge.api import abate

        with patch("agrobr.ibge.client.fetch_sidra", new_callable=AsyncMock) as mock:
            mock.return_value = _build_mock_df()
            await abate("bovino", trimestre="202303")
            call_kwargs = mock.call_args.kwargs
            assert "18" in call_kwargs["classifications"]

    async def test_suino_no_classification_18(self):
        from agrobr.ibge.api import abate

        with patch("agrobr.ibge.client.fetch_sidra", new_callable=AsyncMock) as mock:
            mock.return_value = _build_mock_df()
            await abate("suino", trimestre="202303")
            call_kwargs = mock.call_args.kwargs
            assert "18" not in call_kwargs["classifications"]

    async def test_uf_filter(self):
        from agrobr.ibge.api import abate

        with patch("agrobr.ibge.client.fetch_sidra", new_callable=AsyncMock) as mock:
            mock.return_value = _build_mock_df(1)
            await abate("bovino", trimestre="202303", uf="MT")
            call_kwargs = mock.call_args.kwargs
            assert call_kwargs["ibge_territorial_code"] == "51"

    async def test_list_of_trimestres(self):
        from agrobr.ibge.api import abate

        with patch("agrobr.ibge.client.fetch_sidra", new_callable=AsyncMock) as mock:
            mock.return_value = _build_mock_df()
            await abate("bovino", trimestre=["202301", "202302", "202303"])
            call_kwargs = mock.call_args.kwargs
            assert call_kwargs["period"] == "202301,202302,202303"

    async def test_no_trimestre_uses_last(self):
        from agrobr.ibge.api import abate

        with patch("agrobr.ibge.client.fetch_sidra", new_callable=AsyncMock) as mock:
            mock.return_value = _build_mock_df()
            await abate("bovino")
            call_kwargs = mock.call_args.kwargs
            assert call_kwargs["period"] == "last"

    async def test_return_meta(self):
        from agrobr.ibge.api import abate

        with patch("agrobr.ibge.client.fetch_sidra", new_callable=AsyncMock) as mock:
            mock.return_value = _build_mock_df()
            result = await abate("bovino", trimestre="202303", return_meta=True)
            assert isinstance(result, tuple)
            df, meta = result
            assert isinstance(df, pd.DataFrame)
            assert meta.source == "ibge_abate"

    async def test_trimestre_column_value(self):
        from agrobr.ibge.api import abate

        with patch("agrobr.ibge.client.fetch_sidra", new_callable=AsyncMock) as mock:
            mock.return_value = _build_mock_df()
            df = await abate("bovino", trimestre="202303")
            assert (df["trimestre"] == "202303").all()


class TestEspeciesAbateFunc:
    async def test_returns_list(self):
        from agrobr.ibge.api import especies_abate

        result = await especies_abate()
        assert isinstance(result, list)

    async def test_contains_all_species(self):
        from agrobr.ibge.api import especies_abate

        result = await especies_abate()
        assert "bovino" in result
        assert "suino" in result
        assert "frango" in result


class TestAbatePolarsSupport:
    async def test_polars_conversion(self):
        pytest.importorskip("polars")
        from agrobr.ibge.api import abate

        with patch("agrobr.ibge.client.fetch_sidra", new_callable=AsyncMock) as mock:
            mock.return_value = _build_mock_df()
            df = await abate("bovino", trimestre="202303", as_polars=True)
            import polars as pl

            assert isinstance(df, pl.DataFrame)

    async def test_polars_fallback_pandas(self):
        from agrobr.ibge.api import abate

        with (
            patch("agrobr.ibge.client.fetch_sidra", new_callable=AsyncMock) as mock,
            patch.dict("sys.modules", {"polars": None}),
        ):
            mock.return_value = _build_mock_df()
            df = await abate("bovino", trimestre="202303", as_polars=True)
            assert isinstance(df, pd.DataFrame)


class TestAbateGoldenData:
    def test_golden_data_exists(self):
        assert GOLDEN_DIR.exists()
        assert (GOLDEN_DIR / "metadata.json").exists()
        assert (GOLDEN_DIR / "response.csv").exists()
        assert (GOLDEN_DIR / "expected.json").exists()

    def test_golden_metadata(self):
        meta = json.loads((GOLDEN_DIR / "metadata.json").read_text(encoding="utf-8"))
        assert meta["source"] == "ibge_abate"
        assert meta["table"] == "1092"
        assert meta["especie"] == "bovino"

    def test_golden_response_parseable(self):
        df = pd.read_csv(GOLDEN_DIR / "response.csv", dtype=str)
        assert len(df) > 0
        assert "V" in df.columns
        assert "D1N" in df.columns
        assert "D2C" in df.columns

    def test_golden_parse_matches_expected(self):
        expected = json.loads((GOLDEN_DIR / "expected.json").read_text(encoding="utf-8"))
        df = pd.read_csv(GOLDEN_DIR / "response.csv", dtype=str)

        parsed = client.parse_sidra_response(
            df,
            rename_columns={
                "MC": "unidade_cod",
                "MN": "unidade",
                "D1C": "localidade_cod",
                "D1N": "localidade",
                "D2C": "variavel_cod",
                "D2N": "variavel_nome",
                "D3C": "trimestre_cod",
                "D3N": "trimestre_nome",
            },
        )

        cabecas = parsed[parsed["variavel_cod"].astype(str) == "284"]
        peso = parsed[parsed["variavel_cod"].astype(str) == "285"]

        merge_keys = ["localidade"]
        result = (
            cabecas[merge_keys + ["valor"]]
            .rename(columns={"valor": "animais_abatidos"})
            .merge(
                peso[merge_keys + ["valor"]].rename(columns={"valor": "peso_carcacas"}),
                on=merge_keys,
                how="outer",
            )
        )

        assert len(result) == expected["row_count"]

        for loc, vals in expected["sample_values"].items():
            row = result[result["localidade"] == loc]
            assert len(row) == 1, f"Missing {loc}"
            assert float(row["animais_abatidos"].iloc[0]) == vals["animais_abatidos"]
            assert float(row["peso_carcacas"].iloc[0]) == vals["peso_carcacas"]

    def test_golden_all_values_positive(self):
        df = pd.read_csv(GOLDEN_DIR / "response.csv", dtype=str)
        numeric_vals = pd.to_numeric(df["V"], errors="coerce").dropna()
        assert (numeric_vals > 0).all()


class TestAbateContract:
    def test_contract_registered(self):
        from agrobr.contracts import get_contract

        contract = get_contract("abate_trimestral")
        assert contract is not None
        assert contract.name == "ibge.abate"

    def test_contract_validates_valid_df(self):
        from agrobr.contracts import validate_dataset

        df = pd.DataFrame(
            {
                "trimestre": ["202303"],
                "localidade": ["Mato Grosso"],
                "localidade_cod": [51],
                "especie": ["bovino"],
                "animais_abatidos": [1602321.0],
                "peso_carcacas": [454267996.0],
                "fonte": ["ibge_abate"],
            }
        )
        validate_dataset(df, "abate_trimestral")

    def test_contract_rejects_negative_values(self):
        from agrobr.contracts import validate_dataset
        from agrobr.exceptions import ContractViolationError

        df = pd.DataFrame(
            {
                "trimestre": ["202303"],
                "localidade": ["Test"],
                "localidade_cod": [11],
                "especie": ["bovino"],
                "animais_abatidos": [-100.0],
                "peso_carcacas": [1000.0],
                "fonte": ["ibge_abate"],
            }
        )
        with pytest.raises(ContractViolationError):
            validate_dataset(df, "abate_trimestral")


class TestAbateDataset:
    def test_dataset_registered(self):
        from agrobr.datasets.registry import list_datasets

        datasets = list_datasets()
        assert "abate_trimestral" in datasets

    def test_dataset_info(self):
        from agrobr.datasets.abate_trimestral import ABATE_TRIMESTRAL_INFO

        assert ABATE_TRIMESTRAL_INFO.name == "abate_trimestral"
        assert ABATE_TRIMESTRAL_INFO.update_frequency == "quarterly"

    def test_dataset_has_3_products(self):
        from agrobr.datasets.abate_trimestral import ABATE_TRIMESTRAL_INFO

        assert len(ABATE_TRIMESTRAL_INFO.products) == 3

    def test_dataset_sources(self):
        from agrobr.datasets.abate_trimestral import ABATE_TRIMESTRAL_INFO

        assert len(ABATE_TRIMESTRAL_INFO.sources) == 1
        assert ABATE_TRIMESTRAL_INFO.sources[0].name == "ibge_abate"


class TestAbateCachePolicy:
    def test_cache_policy_exists(self):
        from agrobr.cache.policies import POLICIES

        assert "ibge_abate" in POLICIES

    def test_cache_ttl_7_days(self):
        from agrobr.cache.policies import POLICIES

        policy = POLICIES["ibge_abate"]
        assert policy.ttl_seconds == 7 * 24 * 3600

    def test_cache_stale_90_days(self):
        from agrobr.cache.policies import POLICIES

        policy = POLICIES["ibge_abate"]
        assert policy.stale_max_seconds == 90 * 24 * 3600


@pytest.mark.integration
class TestAbateIntegration:
    async def test_abate_bovino_real(self):
        from agrobr.ibge.api import abate

        df = await abate("bovino", trimestre="202303")
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0
        assert "animais_abatidos" in df.columns
        assert "peso_carcacas" in df.columns

    async def test_abate_frango_real(self):
        from agrobr.ibge.api import abate

        df = await abate("frango", trimestre="202303")
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0
