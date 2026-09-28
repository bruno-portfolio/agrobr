"""Testes específicos para o dataset credito_rural (fetch com mock)."""

import json
import re
from pathlib import Path
from unittest.mock import AsyncMock

import pandas as pd

from agrobr import datasets
from agrobr.bcb import client, models
from agrobr.bcb.api import _aggregate_credito_rural
from agrobr.bcb.parser import parse_credito_rural
from agrobr.datasets.credito_rural import (
    CreditoRuralDataset,
)
from agrobr.datasets.deterministic import deterministic
from agrobr.exceptions import InvalidParameterError
from tests.helpers import collect_failures, isolated_dataset_case, levanta_exatamente

from .conftest import make_source


def _golden_df() -> pd.DataFrame:
    path = Path(__file__).parents[1] / "golden_data" / "bcb" / "custeio_sample"
    records = json.loads(path.joinpath("response.json").read_text(encoding="utf-8"))
    parsed = parse_credito_rural(records)
    return _aggregate_credito_rural(parsed, "uf", "odata")


class TestCreditoRuralNormalize:
    async def test_credito_rural_normalize_casos_1(self):
        with collect_failures() as check:
            case = "test_normalize_adds_produto"
            with check(case), isolated_dataset_case(case):
                df = _golden_df().drop(columns=["produto"])
                dataset = CreditoRuralDataset()
                dataset.info.sources[0].fetch_fn = make_source(df)

                result = await dataset.fetch("soja")

                assert result["produto"].iloc[0] == "soja"
            case = "test_normalize_adds_finalidade"
            with check(case), isolated_dataset_case(case):
                df = _golden_df().drop(columns=["finalidade"])
                dataset = CreditoRuralDataset()
                dataset.info.sources[0].fetch_fn = make_source(df)

                result = await dataset.fetch("soja", finalidade="investimento")

                assert result["finalidade"].iloc[0] == "investimento"
            case = "test_normalize_keeps_existing_produto_finalidade"
            with check(case), isolated_dataset_case(case):
                dataset = CreditoRuralDataset()
                dataset.info.sources[0].fetch_fn = make_source(_golden_df())

                result = await dataset.fetch("soja")

                assert result["produto"].iloc[0] == "soja"
                assert result["finalidade"].iloc[0] == "custeio"


class TestCreditoRuralFetch:
    async def test_credito_rural_fetch_casos_1(self):
        with collect_failures() as check:
            case = "test_snapshot_generates_safra"
            with check(case), isolated_dataset_case(case):
                dataset = CreditoRuralDataset()
                mock_fn = make_source(_golden_df())
                dataset.info.sources[0].fetch_fn = mock_fn

                async with deterministic("2025-03-15"):
                    await dataset.fetch("soja")

                _, kwargs = mock_fn.call_args
                assert kwargs["safra"] == "2024/2025"
            case = "test_snapshot_does_not_override_explicit_safra"
            with check(case), isolated_dataset_case(case):
                dataset = CreditoRuralDataset()
                mock_fn = make_source(_golden_df())
                dataset.info.sources[0].fetch_fn = mock_fn

                async with deterministic("2025-03-15"):
                    await dataset.fetch("soja", safra="2022/23")

                _, kwargs = mock_fn.call_args
                assert kwargs["safra"] == "2022/23"


def test_dataset_de_credito_rural_oferece_todas_as_culturas_do_sicor():
    assert set(datasets.info("credito_rural")["products"]) == set(models.SICOR_PRODUTOS)


async def test_dataset_recusa_municipio_e_industrializacao_antes_da_rede(monkeypatch):
    fetch = AsyncMock()
    monkeypatch.setattr(client, "_fetch_odata", fetch)
    with collect_failures() as check:
        for argumentos, motivo in [
            (
                {"agregacao": "municipio"},
                "agregacao inválida: 'municipio'. Use agregacao='uf', 'programa' ou 'registro'. O SICOR publica "
                "município por produto (CusteioMunicipioProduto e InvestMunicipioProduto), que o "
                "agrobr ainda não lê; o extra agrobr[bigquery] traz dados municipais.",
            ),
            (
                {"finalidade": "industrializacao"},
                "O SICOR não publica a industrialização por produto; use "
                "bcb.credito_rural_total(finalidade='industrializacao'), com o total por UF",
            ),
        ]:
            with (
                check(argumentos),
                levanta_exatamente(InvalidParameterError, match=re.escape(motivo)),
            ):
                await datasets.credito_rural("soja", safra="2022/23", **argumentos)
    fetch.assert_not_awaited()
