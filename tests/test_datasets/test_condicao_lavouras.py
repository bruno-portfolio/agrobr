import pandas as pd

from agrobr.datasets.condicao_lavouras import (
    CondicaoLavourasDataset,
)
from tests.helpers import collect_failures, isolated_dataset_case

from .conftest import make_source


def _make_df(**overrides):
    row = {
        "produto": "soja",
        "data": "01/03/2024",
        "condicao": "boa",
        "pct": 70.0,
        "plantio_pct": float("nan"),
        "colheita_pct": float("nan"),
    }
    row.update(overrides)
    return pd.DataFrame([row])


class TestCondicaoLavourasFetch:
    async def test_condicao_lavouras_fetch_casos_1(self):
        with collect_failures() as check:
            case = "test_fetch_all"
            with check(case), isolated_dataset_case(case):
                dataset = CondicaoLavourasDataset()
                dataset.info.sources[0].fetch_fn = make_source(_make_df())
                df = await dataset.fetch()

                assert len(df) == 1
                assert "produto" in df.columns
                assert "condicao" in df.columns
            case = "test_fetch_by_produto"
            with check(case), isolated_dataset_case(case):
                mock_fn = make_source(_make_df())
                dataset = CondicaoLavourasDataset()
                dataset.info.sources[0].fetch_fn = mock_fn
                await dataset.fetch(produto="soja")

                call_args = mock_fn.call_args
                assert call_args[0][0] == "soja"
