from unittest.mock import patch

import pandas as pd

from agrobr import datasets
from agrobr.datasets.serie_historica_safra import (
    SerieHistoricaSafraDataset,
)
from tests import helpers
from tests.helpers import collect_failures, isolated_dataset_case

from .conftest import make_source


def _mock_df():
    return pd.DataFrame(
        {
            "produto": ["soja", "soja"],
            "safra": ["2023/24", "2023/24"],
            "regiao": ["CENTRO-OESTE", "SUL"],
            "uf": ["MT", "PR"],
            "area_plantada_mil_ha": [12000.0, 6000.0],
            "producao_mil_ton": [40000.0, 22000.0],
            "produtividade_kg_ha": [3333.0, 3667.0],
        }
    )


class TestSerieHistoricaSafraFetch:
    async def test_serie_historica_safra_fetch_casos_1(self):
        with collect_failures() as check:
            case = "test_params_passthrough"
            with check(case), isolated_dataset_case(case):
                dataset = SerieHistoricaSafraDataset()
                mock_fn = make_source(_mock_df())
                dataset.info.sources[0].fetch_fn = mock_fn

                await dataset.fetch("soja", ano_inicio=2020, ano_fim=2024, uf="MT")

                _, kwargs = mock_fn.call_args
                assert kwargs["ano_inicio"] == 2020
                assert kwargs["ano_fim"] == 2024
                assert kwargs["uf"] == "MT"
            case = "test_normalize_noop"
            with check(case), isolated_dataset_case(case):
                df = _mock_df()
                dataset = SerieHistoricaSafraDataset()
                dataset.info.sources[0].fetch_fn = make_source(df.copy())

                result = await dataset.fetch("soja")

                pd.testing.assert_frame_equal(result, df)
            case = "test_contract_validation_called"
            with check(case), isolated_dataset_case(case):
                dataset = SerieHistoricaSafraDataset()
                dataset.info.sources[0].fetch_fn = make_source(_mock_df())

                with patch.object(dataset, "_validate_contract") as mock_validate:
                    await dataset.fetch("soja")
                    mock_validate.assert_called_once()
            case = "test_deterministic_snapshot"
            with check(case), isolated_dataset_case(case):
                dataset = SerieHistoricaSafraDataset()
                mock_fn = make_source(_mock_df())
                dataset.info.sources[0].fetch_fn = mock_fn

                with patch(
                    "agrobr.datasets.serie_historica_safra.get_snapshot",
                    return_value="2024-01-01",
                ):
                    await dataset.fetch("soja")

                _, kwargs = mock_fn.call_args
                assert kwargs["ano_inicio"] == 2019


async def test_dataset_confere_o_oraculo_da_serie_historica_por_uf(monkeypatch):
    casos = helpers.load_serie_historica_manifest()["cases"]
    caso = next(item for item in casos if item["product"] == "soja")
    pedidos = helpers.install_serie_historica_http(monkeypatch, caso)
    try:
        frame = await datasets.serie_historica_safra("soja", uf="MT")
    except Exception as erro:
        raise AssertionError(f"a cola dataset → fonte quebrou: {erro!r}") from erro
    assert len(pedidos) == 1
    assert not frame.empty
    assert frame["uf"].eq("MT").all()
    helpers.assert_serie_historica_case(frame, caso)
