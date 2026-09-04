from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

from agrobr.anec import parser
from agrobr.contracts import validate_dataset
from agrobr.datasets import registry
from agrobr.datasets.exportacao_anec import EmbarquesANECDataset, embarques_anec
from agrobr.exceptions import InvalidParameterError
from tests.test_datasets.conftest import make_source, mock_source_meta


def _mock_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "porto": "SANTOS",
                "produto": "soybean",
                "periodo": "last_week",
                "valor_ton": 275041.0,
            },
            {
                "porto": "PARANAGUÁ",
                "produto": "soybean",
                "periodo": "last_week",
                "valor_ton": 332376.0,
            },
        ]
    )


@pytest.fixture
def restore_dataset_source():
    original = EmbarquesANECDataset.info.sources[0].fetch_fn
    yield
    EmbarquesANECDataset.info.sources[0].fetch_fn = original


@pytest.mark.usefixtures("restore_dataset_source")
class TestEmbarquesANEC:
    @pytest.mark.asyncio
    async def test_fetch_returns_dataframe(self):
        dataset = EmbarquesANECDataset()
        dataset.info.sources[0].fetch_fn = make_source(_mock_df())

        df = await dataset.fetch(ano=2026)

        assert len(df) == 2
        assert "porto" in df.columns
        assert "valor_ton" in df.columns

    @pytest.mark.asyncio
    async def test_fetch_return_meta(self):
        dataset = EmbarquesANECDataset()
        dataset.info.sources[0].fetch_fn = make_source(_mock_df())

        df, meta = await dataset.fetch(ano=2026, return_meta=True)

        assert meta.dataset == "embarques_anec"
        assert meta.selected_source == "anec"
        assert meta.contract_version == "1.0"

    @pytest.mark.asyncio
    async def test_public_fn_propagates_source_error(self):
        with pytest.raises(InvalidParameterError):
            await embarques_anec(ano=2025)

    @pytest.mark.asyncio
    async def test_filters_propagate(self):
        captured: dict = {}

        async def _capturing(produto: str, **kwargs):  # noqa: ARG001
            captured.update(kwargs)
            return _mock_df(), mock_source_meta()

        dataset = EmbarquesANECDataset()
        dataset.info.sources[0].fetch_fn = _capturing

        await dataset.fetch(ano=2026, semana=4, porto="SANTOS", produto="soybean", tipo="efetivado")

        assert captured["ano"] == 2026
        assert captured["semana"] == 4
        assert captured["porto"] == "SANTOS"
        assert captured["produto_filtro"] == "soybean"
        assert captured["tipo"] == "efetivado"

    @pytest.mark.asyncio
    async def test_real_golden_has_unique_primary_key_and_valid_contract(self):
        golden = (
            Path(__file__).parents[1] / "golden_data" / "anec" / "weekly_w12_2026" / "response.pdf"
        )
        report = parser.parse_anec_pdf(golden.read_bytes())

        with patch(
            "agrobr.anec.api._fetch_and_parse",
            new_callable=AsyncMock,
            return_value=(report, "golden://anec/weekly_w12_2026", MagicMock()),
        ):
            df = await EmbarquesANECDataset().fetch(ano=2026, semana=12)

        assert not df.duplicated(["porto", "produto", "periodo"]).any()
        validate_dataset(df, "embarques_anec")


class TestRegistry:
    def test_registered_name(self):
        assert "embarques_anec" in registry.list_datasets()

    def test_describe_returns_anec_info(self):
        text = registry.describe("embarques_anec")
        assert "ANEC" in text
        assert "weekly" in text
        assert "zona_cinza" in text

    def test_get_dataset_returns_instance(self):
        ds = registry.get_dataset("embarques_anec")
        assert ds.info.name == "embarques_anec"
        assert ds.info.license == "zona_cinza"
        assert ds.info.update_frequency == "weekly"
