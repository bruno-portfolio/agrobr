import pandas as pd

from agrobr.datasets.pib_agro import PibAgroDataset
from tests.helpers import collect_failures, isolated_dataset_case


def _make_df(**overrides):
    row = {
        "trimestre": "202401",
        "valor": 150000.0,
        "unidade": "R$ (milhões)",
        "setor": "agropecuaria",
        "precos": "corrente",
        "fonte": "ibge_pib",
    }
    row.update(overrides)
    return pd.DataFrame([row])


class TestPibAgroFetch:
    def test_pib_agro_fetch_casos_1(self):
        with collect_failures() as check:
            case = "test_normalize_adds_setor_precos"
            with check(case), isolated_dataset_case(case):
                df = pd.DataFrame(
                    [{"trimestre": "202401", "valor": 100.0, "unidade": "R$", "fonte": "ibge_pib"}]
                )
                dataset = PibAgroDataset()
                result = dataset._normalize(df, "industria", "real_1995")
                assert result.iloc[0]["setor"] == "industria"
                assert result.iloc[0]["precos"] == "real_1995"
            case = "test_normalize_empty_df"
            with check(case), isolated_dataset_case(case):
                empty_df = pd.DataFrame(columns=["trimestre", "valor", "unidade", "fonte"])
                dataset = PibAgroDataset()
                result = dataset._normalize(empty_df, "agropecuaria", "corrente")
                assert "setor" in result.columns
                assert "precos" in result.columns
                assert len(result) == 0
