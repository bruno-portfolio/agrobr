from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from agrobr import datasets, usda
from agrobr.datasets.oferta_demanda_global import (
    OfertaDemandaGlobalDataset,
)
from tests.helpers import collect_failures, isolated_dataset_case
from tests.test_usda.conftest import Gateway, corpo, simular_gateway

from .conftest import make_source, mock_source_meta


def _make_df(**overrides):
    row = {
        "commodity_code": "2222000",
        "commodity": "soja",
        "country_code": "BR",
        "country": "Brazil",
        "market_year": 2024,
        "attribute": "Production",
        "attribute_br": "producao",
        "value": 154000.0,
        "unit": "(1000 MT)",
        "attribute_id": 28,
        "unit_id": 8,
        "last_update_year": 2026,
        "last_update_month": 4,
    }
    row.update(overrides)
    return pd.DataFrame([row])


class TestOfertaDemandaGlobalFetch:
    @pytest.mark.asyncio
    async def test_snapshot_default_year(self):
        mock_fn = make_source(_make_df())
        dataset = OfertaDemandaGlobalDataset()
        dataset.info.sources[0].fetch_fn = mock_fn

        from agrobr.datasets.deterministic import deterministic

        async with deterministic("2023-06-15"):
            await dataset.fetch("soja")

        call_kwargs = mock_fn.call_args[1]
        assert call_kwargs["market_year"] == 2023


class TestOfertaDemandaGlobalFetchFunctions:
    async def test_oferta_demanda_global_fetch_functions_casos_1(self):
        with collect_failures() as check:
            case = "test_fetch_usda_psd_forwards_all_params"
            with check(case), isolated_dataset_case(case):
                df = _make_df()
                meta = mock_source_meta()
                with patch(
                    "agrobr.usda.psd", new_callable=AsyncMock, return_value=(df, meta)
                ) as mock_fn:
                    from agrobr.datasets.oferta_demanda_global import _fetch_usda_psd

                    await _fetch_usda_psd(
                        "soja",
                        country="US",
                        market_year=2023,
                        attributes=["Production"],
                        pivot=True,
                        api_key="key123",
                    )
                mock_fn.assert_called_once_with(
                    "soja",
                    country="US",
                    market_year=2023,
                    attributes=["Production"],
                    pivot=True,
                    api_key="key123",
                    return_meta=True,
                )
            case = "test_fetch_usda_psd_defaults"
            with check(case), isolated_dataset_case(case):
                df = _make_df()
                meta = mock_source_meta()
                with patch(
                    "agrobr.usda.psd", new_callable=AsyncMock, return_value=(df, meta)
                ) as mock_fn:
                    from agrobr.datasets.oferta_demanda_global import _fetch_usda_psd

                    await _fetch_usda_psd("soja")
                _, kwargs = mock_fn.call_args
                assert kwargs["country"] == "BR"
                assert kwargs["market_year"] is None
                assert kwargs["attributes"] is None
                assert kwargs["pivot"] is False
                assert kwargs["api_key"] is None


class TestOfertaDemandaGlobalValidation:
    async def test_oferta_demanda_global_validation_casos_1(self):
        with collect_failures() as check:
            case = "test_contract_skipped_when_pivot"
            with check(case), isolated_dataset_case(case):
                dataset = OfertaDemandaGlobalDataset()
                pivot_df = pd.DataFrame(
                    [{"commodity_code": "2222000", "commodity": "soja", "Production": 154000.0}]
                )
                dataset.info.sources[0].fetch_fn = make_source(pivot_df)

                with patch.object(dataset, "_validate_contract") as mock_validate:
                    await dataset.fetch("soja", pivot=True)
                    mock_validate.assert_not_called()
            case = "test_contract_called_when_not_pivot"
            with check(case), isolated_dataset_case(case):
                dataset = OfertaDemandaGlobalDataset()
                dataset.info.sources[0].fetch_fn = make_source(_make_df())

                with patch.object(dataset, "_validate_contract") as mock_validate:
                    await dataset.fetch("soja", pivot=False)
                    mock_validate.assert_called_once()


async def test_dataset_repassa_consulta_e_saida_da_fonte_sobre_o_golden(monkeypatch):
    servidor = Gateway()
    capturado = servidor.servir("soja_BR_2024.json")["url"]
    simular_gateway(monkeypatch, servidor)
    fonte = await usda.psd("soja", market_year=2024, api_key="chave")
    try:
        frame, meta = await datasets.oferta_demanda_global(
            "soja", market_year=2024, api_key="chave", return_meta=True
        )
        sem_meta = await datasets.oferta_demanda_global("soja", market_year=2024, api_key="chave")
    except Exception as erro:
        raise AssertionError(f"a cola dataset → fonte quebrou: {erro!r}") from erro
    assert [str(p.url) for p in servidor.pedidos] == [capturado] * 3
    assert servidor.pedidos[1].headers.get("X-Api-Key") == "chave"
    assert len(frame) == 13
    pd.testing.assert_frame_equal(frame, fonte)
    assert isinstance(sem_meta, pd.DataFrame)
    pd.testing.assert_frame_equal(sem_meta, fonte)
    assert meta.source_url == capturado
    assert (meta.raw_content_size, meta.contract_version) == (
        len(corpo("soja_BR_2024.json")),
        "1.1",
    )
    assert (meta.selected_source, meta.attempted_sources) == ("usda", ["usda"])
