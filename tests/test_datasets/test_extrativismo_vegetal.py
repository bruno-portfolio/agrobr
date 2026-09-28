import json
from pathlib import Path

import httpx
import pandas as pd
import pytest

from agrobr import datasets
from agrobr.datasets.deterministic import deterministic
from agrobr.datasets.extrativismo_vegetal import (
    ExtrativsmoVegetalDataset,
)
from agrobr.ibge import client
from tests.helpers import collect_failures, isolated_dataset_case

from .conftest import make_source

PEVS_VALOR = (
    Path(__file__).resolve().parents[1] / "golden_data/ibge/pevs_valor_oficial/extracao_valor.json"
)


@pytest.fixture(autouse=True)
def sem_periodos_ibge(monkeypatch: pytest.MonkeyPatch) -> None:
    """Os replays deste módulo não trazem o `/periodos`."""

    async def sem_metadado(_table_code: str, _df: object) -> dict[str, object]:
        return {}

    monkeypatch.setattr(client, "_periodos_modificacao", sem_metadado)


def _mock_df():
    return pd.DataFrame(
        [
            {
                "ano": 2022,
                "localidade": "Pará",
                "localidade_cod": 15,
                "produto": "acai",
                "valor": 1500000.0,
                "unidade": "Toneladas",
                "fonte": "ibge_extracao_vegetal",
            },
        ]
    )


class TestExtrativsmoVegetalFetch:
    async def test_extrativsmo_vegetal_fetch_casos_1(self):
        with collect_failures() as check:
            case = "test_snapshot_sets_ano_minus_1"
            with check(case), isolated_dataset_case(case):
                dataset = ExtrativsmoVegetalDataset()
                mock_fn = make_source(_mock_df())
                dataset.info.sources[0].fetch_fn = mock_fn

                async with deterministic("2024-06-15"):
                    await dataset.fetch("acai")

                _, kwargs = mock_fn.call_args
                assert kwargs["ano"] == 2023
            case = "test_snapshot_does_not_override_explicit_ano"
            with check(case), isolated_dataset_case(case):
                dataset = ExtrativsmoVegetalDataset()
                mock_fn = make_source(_mock_df())
                dataset.info.sources[0].fetch_fn = mock_fn

                async with deterministic("2024-06-15"):
                    await dataset.fetch("acai", ano=2021)

                _, kwargs = mock_fn.call_args
                assert kwargs["ano"] == 2021


async def test_valor_da_producao_confere_a_celula_oficial_em_mil_reais(monkeypatch):
    oficial = json.loads(PEVS_VALOR.read_text(encoding="utf-8"))[1]

    async def send(_client, request, **_kwargs):
        return httpx.Response(200, json=[oficial], request=request)

    monkeypatch.setattr(httpx.AsyncClient, "send", send)
    frame = await datasets.extrativismo_vegetal(
        "acai", ano=2023, uf="PA", variavel="valor_producao"
    )
    assert frame[["ano", "localidade_cod", "valor", "unidade"]].to_dict("records") == [
        {
            "ano": int(oficial["D2C"]),
            "localidade_cod": int(oficial["D1C"]),
            "valor": float(oficial["V"]),
            "unidade": oficial["MN"],
        }
    ]
    assert oficial["MN"] in datasets.info("extrativismo_vegetal")["unit"].split(" / ")
