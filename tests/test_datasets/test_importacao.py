import pandas as pd

from agrobr.datasets.importacao import ImportacaoDataset
from tests.helpers import collect_failures, isolated_dataset_case


def _make_df(**overrides):
    row = {
        "ano": 2024,
        "mes": 1,
        "produto": "soja",
        "uf": "SP",
        "kg_liquido": 1000000.0,
        "valor_fob_usd": 500000.0,
    }
    row.update(overrides)
    return pd.DataFrame([row])


class TestImportacaoFetch:
    def test_importacao_fetch_casos_1(self):
        with collect_failures() as check:
            case = "test_normalize_adds_produto"
            with check(case), isolated_dataset_case(case):
                df_no_produto = _make_df()
                df_no_produto = df_no_produto.drop(columns=["produto"])

                dataset = ImportacaoDataset()
                result = dataset._normalize(df_no_produto, "milho")
                assert "produto" in result.columns
                assert result.iloc[0]["produto"] == "milho"
            case = "test_normalize_empty_df"
            with check(case), isolated_dataset_case(case):
                empty_df = pd.DataFrame(columns=["ano", "mes", "uf", "kg_liquido", "valor_fob_usd"])
                dataset = ImportacaoDataset()
                result = dataset._normalize(empty_df, "soja")
                assert "produto" in result.columns
                assert len(result) == 0
