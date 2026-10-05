from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from agrobr.datasets.deterministic import deterministic
from agrobr.datasets.preco_diario import PrecoDiarioDataset, _fetch_cepea
from agrobr.exceptions import ParseError
from tests.helpers import collect_failures, isolated_dataset_case, levanta_exatamente

from .conftest import make_source, mock_source_meta


def _mock_df():
    return pd.DataFrame(
        [
            {
                "data": pd.Timestamp("2025-01-15"),
                "valor": 145.30,
                "unidade": "R$/saca 60kg",
                "produto": "soja",
                "fonte": "cepea",
                "praca": "Paranaguá",
            },
            {
                "data": pd.Timestamp("2025-01-14"),
                "valor": 144.80,
                "unidade": "R$/saca 60kg",
                "produto": "soja",
                "fonte": "cepea",
                "praca": "Paranaguá",
            },
        ]
    )


class TestPrecoDiarioFetchFunctions:
    async def test_preco_diario_fetch_functions_casos_1(self):
        with collect_failures() as check:
            case = "test_fetch_cepea_deterministic_offline"
            with check(case), isolated_dataset_case(case):
                mock_df = _mock_df()
                meta = mock_source_meta()
                with patch(
                    "agrobr.cepea.indicador", new_callable=AsyncMock, return_value=(mock_df, meta)
                ) as mock_ind:
                    async with deterministic("2025-01-15"):
                        await _fetch_cepea("soja")
                    _, kwargs = mock_ind.call_args
                    assert kwargs.get("offline") is True
            case = "test_fetch_cepea_limits_fim_to_snapshot"
            with check(case), isolated_dataset_case(case):
                with patch("agrobr.cepea.indicador", new_callable=AsyncMock) as indicador:
                    indicador.return_value = (_mock_df(), mock_source_meta())

                    async with deterministic("2025-01-15"):
                        await _fetch_cepea("soja", fim="2025-01-31")
                        await _fetch_cepea("soja", fim="2025-01-10")

                assert indicador.await_args_list[0].kwargs["fim"] == "2025-01-15"
                assert indicador.await_args_list[1].kwargs["fim"] == "2025-01-10"
                assert all(call.kwargs["offline"] is True for call in indicador.await_args_list)

    async def test_layout_novo_do_cepea_chega_como_parse_error(self):
        erro = ParseError("cepea", 1, "layout alterado")
        with (
            patch("agrobr.cepea.indicador", new_callable=AsyncMock, side_effect=erro),
            pytest.raises(ParseError) as caught,
        ):
            await PrecoDiarioDataset().fetch("soja")
        assert caught.value.attempted_sources == ["cepea"]
        assert caught.value.__cause__ is erro


class TestPrecoDiarioFetch:
    async def test_preco_diario_fetch_casos_2(self):
        with collect_failures() as check:
            case = "test_fetch_snapshot_filters_dates"
            with check(case), isolated_dataset_case(case):
                dataset = PrecoDiarioDataset()
                df_with_future = pd.DataFrame(
                    [
                        {
                            "data": pd.Timestamp("2025-01-20"),
                            "valor": 150.0,
                            "unidade": "R$/saca 60kg",
                            "praca": "Paranaguá",
                        },
                        {
                            "data": pd.Timestamp("2025-01-15"),
                            "valor": 145.0,
                            "unidade": "R$/saca 60kg",
                            "praca": "Paranaguá",
                        },
                        {
                            "data": pd.Timestamp("2025-01-10"),
                            "valor": 140.0,
                            "unidade": "R$/saca 60kg",
                            "praca": "Paranaguá",
                        },
                    ]
                )
                dataset.info.sources[0].fetch_fn = make_source(df_with_future)

                async with deterministic("2025-01-15"):
                    df = await dataset.fetch("soja")

                assert len(df) == 2
                assert df["data"].max().date() <= pd.Timestamp("2025-01-15").date()
            case = "test_fetch_snapshot_sets_fim"
            with check(case), isolated_dataset_case(case):
                dataset = PrecoDiarioDataset()
                mock_fn = make_source(_mock_df())
                dataset.info.sources[0].fetch_fn = mock_fn

                async with deterministic("2025-01-15"):
                    await dataset.fetch("soja")

                _, kwargs = mock_fn.call_args
                assert kwargs["fim"] == "2025-01-15"


class TestPrecoDiarioNormalize:
    async def test_preco_diario_normalize_casos_2(self):
        with collect_failures() as check:
            case = "test_normalize_missing_required_raises"
            with check(case), isolated_dataset_case(case):
                df = pd.DataFrame(
                    [
                        {
                            "data": pd.Timestamp("2025-01-15"),
                            "unidade": "R$/saca 60kg",
                        },
                    ]
                )
                dataset = PrecoDiarioDataset()
                dataset.info.sources[0].fetch_fn = make_source(df)

                with levanta_exatamente(ValueError, match="Missing required column"):
                    await dataset.fetch("soja")
            case = "test_normalize_missing_data_column"
            with check(case), isolated_dataset_case(case):
                df = pd.DataFrame(
                    [
                        {
                            "valor": 145.0,
                            "unidade": "R$/saca 60kg",
                        },
                    ]
                )
                dataset = PrecoDiarioDataset()
                dataset.info.sources[0].fetch_fn = make_source(df)

                with levanta_exatamente(ValueError, match="Missing required column: data"):
                    await dataset.fetch("soja")
            case = "test_normalize_missing_unidade_column"
            with check(case), isolated_dataset_case(case):
                df = pd.DataFrame(
                    [
                        {
                            "data": pd.Timestamp("2025-01-15"),
                            "valor": 145.0,
                        },
                    ]
                )
                dataset = PrecoDiarioDataset()
                dataset.info.sources[0].fetch_fn = make_source(df)

                with levanta_exatamente(ValueError, match="Missing required column: unidade"):
                    await dataset.fetch("soja")
