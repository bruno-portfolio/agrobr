from unittest.mock import AsyncMock, patch

import httpx
import pandas as pd
import pytest

from agrobr.datasets.clima import ClimaDataset
from agrobr.exceptions import ContractViolationError, SourceFallbackWarning, SourceUnavailableError
from tests.helpers import collect_failures, isolated_dataset_case, levanta_exatamente

from .conftest import make_source as shared_make_source
from .conftest import mock_source_meta as shared_mock_source_meta


def _mock_nasa_df():
    return pd.DataFrame(
        {
            "mes": pd.to_datetime(["2024-01-01", "2024-02-01"]),
            "uf": ["SP", "SP"],
            "precip_acum_mm": [145.0, 115.0],
            "temp_media": [24.5, 25.5],
            "temp_max_media": [29.5, 30.5],
            "temp_min_media": [19.5, 20.5],
            "umidade_media": [75.0, 72.0],
            "radiacao_media_mj": [18.0, 19.0],
            "vento_medio_ms": [2.5, 2.8],
            "lat": [-23.5, -23.5],
            "lon": [-46.6, -46.6],
        }
    )


def _add_nasa_nullable_cols(df: pd.DataFrame) -> pd.DataFrame:
    df["fonte"] = "nasa_power"
    df["num_estacoes"] = pd.array([pd.NA] * len(df), dtype="Int64")
    return df


def mock_source_meta():
    meta = shared_mock_source_meta()
    meta.source_details = {}
    return meta


def make_source(frame):
    return shared_make_source(frame, mock_source_meta())


def _mock_inmet_df():
    return pd.DataFrame(
        {
            "mes": pd.to_datetime(["2024-01-01", "2024-02-01"]),
            "uf": ["SP", "SP"],
            "precip_acum_mm": [150.0, 120.0],
            "temp_media": [25.0, 26.0],
            "temp_max_media": [30.0, 31.0],
            "temp_min_media": [20.0, 21.0],
            "num_estacoes": pd.array([15, 15], dtype="Int64"),
        }
    )


def _add_inmet_nullable_cols(df: pd.DataFrame) -> pd.DataFrame:
    df["fonte"] = "inmet"
    for col in ("umidade_media", "radiacao_media_mj", "vento_medio_ms"):
        df[col] = pd.array([pd.NA] * len(df), dtype="Float64")
    return df


class TestClimaEstacao:
    async def test_station_failure_does_not_attempt_nasa_grid(self):
        dataset = ClimaDataset()
        fetchers = {}
        for source in dataset.info.sources:
            fetchers[source.name] = AsyncMock(
                side_effect=SourceUnavailableError(source.name, last_error="unavailable")
            )
            source.fetch_fn = fetchers[source.name]
        with pytest.raises(SourceUnavailableError) as caught:
            await dataset.fetch(estacao="A001", inicio="2000-12-30", fim="2000-12-31")
        assert [name for name, _, _ in caught.value.errors] == ["inmet", "inmet_historico"]
        assert fetchers["nasa_power"].await_count == 0
        assert fetchers["inmet"].await_count == fetchers["inmet_historico"].await_count == 1


class TestClimaFetchFunctions:
    @pytest.mark.asyncio
    async def test_fetch_inmet_adds_fonte_and_nullable_cols(self):
        df = pd.DataFrame(
            {
                "mes": [pd.Timestamp("2024-01-01")],
                "uf": ["SP"],
                "precip_acum_mm": [150.0],
                "temp_media": [25.0],
            }
        )
        meta = mock_source_meta()
        with patch("agrobr.inmet.clima_uf", new_callable=AsyncMock, return_value=(df, meta)):
            from agrobr.datasets.clima import _fetch_inmet

            result_df, _ = await _fetch_inmet("SP", ano=2024)
        assert "fonte" in result_df.columns and result_df["fonte"].eq("inmet").all()
        assert "umidade_media" in result_df.columns
        assert "radiacao_media_mj" in result_df.columns
        assert "vento_medio_ms" in result_df.columns


class TestClimaNormalize:
    async def test_clima_normalize_casos_1(self):
        with collect_failures() as check:
            case = "test_normalize_uppercases_uf"
            with check(case), isolated_dataset_case(case):
                dataset = ClimaDataset()
                df_inmet = _mock_inmet_df()
                df_inmet["uf"] = "sp"
                df_inmet = _add_inmet_nullable_cols(df_inmet)
                dataset.info.sources[0].fetch_fn = make_source(df_inmet)

                df = await dataset.fetch("sp", ano=2024)

                assert (df["uf"] == "SP").all()
            case = "test_normalize_adds_uf_when_missing"
            with check(case), isolated_dataset_case(case):
                dataset = ClimaDataset()
                df_inmet = _mock_inmet_df().drop(columns=["uf"])
                df_inmet = _add_inmet_nullable_cols(df_inmet)
                dataset.info.sources[0].fetch_fn = make_source(df_inmet)

                df = await dataset.fetch("sp", ano=2024)

                assert (df["uf"] == "SP").all()
            case = "test_normalize_recusa_uf_diferente_da_pedida"
            with check(case), isolated_dataset_case(case):
                dataset = ClimaDataset()
                df_inmet = _mock_inmet_df()
                df_inmet["uf"] = "rj"
                df_inmet = _add_inmet_nullable_cols(df_inmet)
                dataset.info.sources[0].fetch_fn = make_source(df_inmet)

                with levanta_exatamente(ContractViolationError, match="outra UF"):
                    await dataset.fetch("SP", ano=2024)
            case = "test_inmet_has_estacoes_nasa_null"
            with check(case), isolated_dataset_case(case):
                dataset = ClimaDataset()
                df_inmet = _add_inmet_nullable_cols(_mock_inmet_df())
                dataset.info.sources[0].fetch_fn = make_source(df_inmet)

                df = await dataset.fetch("SP", ano=2024)

                assert df["num_estacoes"].notna().all()
                assert df["umidade_media"].isna().all()
            case = "test_nasa_has_umidade_inmet_null"
            with check(case), isolated_dataset_case(case):
                dataset = ClimaDataset()
                dataset.info.sources[0].fetch_fn = AsyncMock(side_effect=httpx.ConnectError("down"))
                dataset.info.sources[1].fetch_fn = AsyncMock(side_effect=httpx.ConnectError("down"))

                df_nasa = _add_nasa_nullable_cols(_mock_nasa_df())
                dataset.info.sources[2].fetch_fn = make_source(df_nasa)

                with pytest.warns(SourceFallbackWarning, match="nasa_power"):
                    df = await dataset.fetch("SP", ano=2024)

                assert df["umidade_media"].notna().all()
                assert df["num_estacoes"].isna().all()
