"""Testes específicos para o dataset fertilizante (fetch com mock)."""

from unittest.mock import AsyncMock, patch

import pandas as pd

from agrobr.datasets.deterministic import deterministic
from agrobr.datasets.fertilizante import (
    FertilizanteDataset,
)
from tests.helpers import collect_failures, isolated_dataset_case

from .conftest import make_source, mock_source_meta


def _mock_df():
    return pd.DataFrame(
        [
            {
                "ano": 2024,
                "mes": 1,
                "uf": "MT",
                "produto_fertilizante": "total",
                "volume_ton": 150000.0,
            },
            {
                "ano": 2024,
                "mes": 1,
                "uf": "SP",
                "produto_fertilizante": "total",
                "volume_ton": 100000.0,
            },
        ]
    )


class TestFertilizanteFetch:
    async def test_fertilizante_fetch_casos_1(self):
        with collect_failures() as check:
            case = "test_snapshot_sets_ano"
            with check(case), isolated_dataset_case(case):
                dataset = FertilizanteDataset()
                mock_fn = make_source(_mock_df())
                dataset.info.sources[0].fetch_fn = mock_fn

                async with deterministic("2024-06-15"):
                    await dataset.fetch("total")

                _, kwargs = mock_fn.call_args
                assert kwargs["ano"] == 2024
            case = "test_snapshot_does_not_override_explicit_ano"
            with check(case), isolated_dataset_case(case):
                dataset = FertilizanteDataset()
                mock_fn = make_source(_mock_df())
                dataset.info.sources[0].fetch_fn = mock_fn

                async with deterministic("2024-06-15"):
                    await dataset.fetch("total", ano=2023)

                _, kwargs = mock_fn.call_args
                assert kwargs["ano"] == 2023


class TestFertilizanteFetchFunctions:
    async def test_fertilizante_fetch_functions_casos_1(self):
        with collect_failures() as check:
            case = "test_fetch_anda_forwards_params"
            with check(case), isolated_dataset_case(case):
                df = _mock_df()
                meta = mock_source_meta()
                with patch(
                    "agrobr.anda.entregas", new_callable=AsyncMock, return_value=(df, meta)
                ) as mock_fn:
                    from agrobr.datasets.fertilizante import _fetch_anda

                    await _fetch_anda("total", ano=2023)
                mock_fn.assert_called_once_with(2023, produto="total", return_meta=True)
            case = "test_fetch_anda_defaults_ano_to_current_year"
            with check(case), isolated_dataset_case(case):
                df = _mock_df()
                meta = mock_source_meta()
                with patch(
                    "agrobr.anda.entregas", new_callable=AsyncMock, return_value=(df, meta)
                ) as mock_fn:
                    from agrobr.datasets.fertilizante import _fetch_anda

                    await _fetch_anda("total")
                assert isinstance(mock_fn.call_args[0][0], int)
