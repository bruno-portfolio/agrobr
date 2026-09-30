from datetime import date

import pandas as pd

from agrobr.contracts.datasets import POSICIONAMENTO_FUNDOS_COLUNAS_V2
from agrobr.datasets.posicionamento_fundos import (
    PosicionamentoFundosDataset,
)
from tests.helpers import collect_failures, isolated_dataset_case

from .conftest import make_source


def _make_df(**overrides):
    row = {
        "data": pd.Timestamp("2026-06-02"),
        "commodity": "soja",
        "contrato": "SOYBEANS - CHICAGO BOARD OF TRADE",
        "codigo_cftc": "005602",
        "open_interest": 1054882,
        "managed_money_long": 264854,
        "managed_money_short": 109074,
        "managed_money_spread": 71474,
        "managed_money_net": 155780,
        "producer_long": 226895,
        "producer_short": 477906,
        "swap_long": 137395,
        "swap_short": 27302,
        "other_long": 81696,
        "other_short": 52131,
        "nonreportable_long": 67713,
        "nonreportable_short": 48259,
        "change_managed_money_long": 1690,
        "change_managed_money_short": -8533,
        "change_open_interest": 6324,
    }
    row.update(overrides)
    df = pd.DataFrame([row])
    for col in df.columns:
        if col.startswith("change_"):
            df[col] = df[col].astype("Int64")
    return df


class TestPosicionamentoFundosFetch:
    async def test_posicionamento_fundos_fetch_casos_1(self):
        with collect_failures() as check:
            case = "test_snapshot_define_fim"
            with check(case), isolated_dataset_case(case):
                mock_fn = make_source(_make_df())
                dataset = PosicionamentoFundosDataset()
                dataset.info.sources[0].fetch_fn = mock_fn

                from agrobr.datasets.deterministic import deterministic

                async with deterministic("2020-06-15"):
                    await dataset.fetch("soja")

                call_kwargs = mock_fn.call_args[1]
                assert call_kwargs["fim"] == date(2020, 6, 15)
            case = "test_fim_explicito_vence_snapshot"
            with check(case), isolated_dataset_case(case):
                mock_fn = make_source(_make_df())
                dataset = PosicionamentoFundosDataset()
                dataset.info.sources[0].fetch_fn = mock_fn

                from agrobr.datasets.deterministic import deterministic

                async with deterministic("2020-06-15"):
                    await dataset.fetch("soja", fim="31/12/2019")

                call_kwargs = mock_fn.call_args[1]
                assert call_kwargs["fim"] == date(2019, 12, 31)

    async def test_colunas_saem_em_portugues_no_contrato_2_0(self):
        with isolated_dataset_case("colunas_pt"):
            mock_fn = make_source(_make_df())
            dataset = PosicionamentoFundosDataset()
            dataset.info.sources[0].fetch_fn = mock_fn
            df, meta = await dataset.fetch("soja", combinado=True, return_meta=True)
        assert list(df.columns) == [
            POSICIONAMENTO_FUNDOS_COLUNAS_V2.get(coluna, coluna) for coluna in _make_df().columns
        ]
        assert df.loc[0, "fundos_saldo"] == 155780
        assert df.loc[0, "variacao_posicoes"] == 6324
        assert meta.contract_version == "2.0"
        assert mock_fn.call_args[1]["combinado"] is True
