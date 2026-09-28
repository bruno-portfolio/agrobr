import pandas as pd

from agrobr.datasets.balanco import BalancoDataset
from tests.helpers import collect_failures, isolated_dataset_case

from .conftest import make_source


def _mock_df():
    return pd.DataFrame(
        [
            {
                "produto": "soja",
                "safra": "2023/24",
                "estoque_inicial": 1200.0,
                "producao": 15000.0,
                "importacao": 50.0,
                "suprimento": 16250.0,
                "consumo": 5300.0,
                "exportacao": 9700.0,
                "estoque_final": 1250.0,
            },
        ]
    )


class TestBalancoNormalize:
    async def test_balanco_normalize_casos_1(self):
        with collect_failures() as check:
            case = "test_normalize_adds_produto_fonte"
            with check(case), isolated_dataset_case(case):
                df = _mock_df().drop(columns=["produto"])
                dataset = BalancoDataset()
                dataset.info.sources[0].fetch_fn = make_source(df)

                result = await dataset.fetch("soja")

                assert result["produto"].iloc[0] == "soja"
                assert result["fonte"].iloc[0] == "conab"
            case = "test_normalize_empty_df"
            with check(case), isolated_dataset_case(case):
                df = _mock_df().iloc[:0].copy()
                df["fonte"] = pd.Series(dtype="str")
                dataset = BalancoDataset()
                dataset.info.sources[0].fetch_fn = make_source(df)

                result = await dataset.fetch("soja")

                assert len(result) == 0
