import pandas as pd
import pytest

from agrobr.datasets.deterministic import deterministic
from agrobr.datasets.pecuaria_municipal import PecuariaMunicipalDataset

from .conftest import make_source


def _mock_df():
    return pd.DataFrame(
        [
            {
                "ano": 2022,
                "localidade": "Mato Grosso",
                "localidade_cod": 51,
                "especie": "bovino",
                "valor": 32000000.0,
                "unidade": "Cabeças",
                "fonte": "ibge_ppm",
            },
        ]
    )


class TestPecuariaMunicipalSnapshot:
    @pytest.mark.asyncio
    async def test_snapshot_sets_ano(self):
        dataset = PecuariaMunicipalDataset()
        mock_fn = make_source(_mock_df())
        dataset.info.sources[0].fetch_fn = mock_fn

        async with deterministic(snapshot="2023-06-15"):
            await dataset.fetch("bovino")

        _, call_kwargs = mock_fn.call_args
        assert call_kwargs["ano"] == 2022
