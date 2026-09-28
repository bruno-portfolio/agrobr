"""Testes específicos para o dataset exportacao (fetch com mock + prioridade)."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch

import httpx
import pandas as pd
import pytest

from agrobr.datasets.deterministic import deterministic
from agrobr.datasets.exportacao import (
    ExportacaoDataset,
    _fetch_abiove,
    _fetch_comexstat,
)
from agrobr.exceptions import InvalidParameterError, SourceUnavailableError
from tests.helpers import collect_failures, isolated_dataset_case

from .conftest import make_source, mock_source_meta


def _mock_export_df():
    return pd.DataFrame(
        [
            {
                "ano": 2024,
                "mes": 1,
                "produto": "soja",
                "uf": "MT",
                "kg_liquido": 5000000000,
                "valor_fob_usd": 2500000000,
            },
        ]
    )


class TestExportacaoFetchFunctions:
    @pytest.mark.asyncio
    async def test_soybean_oil_has_unique_contract_key(self):
        dataset = ExportacaoDataset()
        source_df = pd.DataFrame(
            {
                "ano": [2024, 2024],
                "mes": [1, 1],
                "ncm": ["15071000", "15079019"],
                "uf": ["MT", "MT"],
                "kg_liquido": [1000.0, 2000.0],
                "valor_fob_usd": [500.0, 1000.0],
                "volume_ton": [1.0, 2.0],
            }
        )

        with (
            patch.object(dataset.info.sources[0], "fetch_fn", _fetch_comexstat),
            patch(
                "agrobr.comexstat.exportacao",
                new_callable=AsyncMock,
                return_value=(source_df, mock_source_meta()),
            ),
        ):
            result_df = await dataset.fetch("oleo_soja", ano=2024)

        key = ["ano", "mes", "produto", "uf"]
        assert result_df["produto"].tolist() == ["oleo_soja"]
        assert not result_df.duplicated(subset=key).any()

    async def test_exportacao_fetch_functions_casos_1(self):
        with collect_failures() as check:
            case = "test_fetch_abiove_column_transform"
            with check(case), isolated_dataset_case(case):
                df = pd.DataFrame({"volume_ton": [100.0], "receita_usd_mil": [50.0]})
                meta = mock_source_meta()
                with patch(
                    "agrobr.abiove.exportacao", new_callable=AsyncMock, return_value=(df, meta)
                ):
                    from agrobr.datasets.exportacao import _fetch_abiove

                    result_df, _ = await _fetch_abiove("soja", ano=2024)
                assert result_df["kg_liquido"].iloc[0] == 100000.0
                assert result_df["valor_fob_usd"].iloc[0] == 50000.0
            case = "test_fetch_abiove_defaults_none_ano"
            with check(case), isolated_dataset_case(case):
                df = pd.DataFrame({"volume_ton": [100.0], "receita_usd_mil": [50.0]})
                meta = mock_source_meta()

                with (
                    patch(
                        "agrobr.datasets.exportacao.utcnow",
                        return_value=datetime(2026, 9, 2, tzinfo=UTC),
                    ),
                    patch(
                        "agrobr.abiove.exportacao",
                        new_callable=AsyncMock,
                        return_value=(df, meta),
                    ) as mock_fn,
                ):
                    await _fetch_abiove("milho", ano=None)

                mock_fn.assert_awaited_once_with(
                    ano=2025,
                    produto="milho",
                    return_meta=True,
                )
            case = "test_fetch_abiove_skip_transform_if_cols_exist"
            with check(case), isolated_dataset_case(case):
                df = pd.DataFrame(
                    {
                        "volume_ton": [100.0],
                        "kg_liquido": [999.0],
                        "receita_usd_mil": [50.0],
                        "valor_fob_usd": [888.0],
                    }
                )
                meta = mock_source_meta()
                with patch(
                    "agrobr.abiove.exportacao", new_callable=AsyncMock, return_value=(df, meta)
                ):
                    from agrobr.datasets.exportacao import _fetch_abiove

                    result_df, _ = await _fetch_abiove("soja", ano=2024)
                assert result_df["kg_liquido"].iloc[0] == 999.0
                assert result_df["valor_fob_usd"].iloc[0] == 888.0

    @pytest.mark.asyncio
    @pytest.mark.parametrize("produto", ["cafe", "algodao", "acucar"])
    async def test_fetch_abiove_rejects_unsupported_produto_before_source_call(
        self,
        produto: str,
    ):
        with (
            patch("agrobr.abiove.exportacao", new_callable=AsyncMock) as mock_fn,
            pytest.raises(SourceUnavailableError, match="não disponível"),
        ):
            await _fetch_abiove(produto, ano=2024)

        mock_fn.assert_not_awaited()


class TestExportacaoFallback:
    async def test_exportacao_fallback_casos_1(self):
        with collect_failures() as check:
            case = "test_all_sources_fail"
            with check(case), isolated_dataset_case(case):
                dataset = ExportacaoDataset()
                dataset.info.sources[0].fetch_fn = AsyncMock(side_effect=httpx.ConnectError("down"))
                dataset.info.sources[1].fetch_fn = AsyncMock(side_effect=httpx.ConnectError("down"))

                with pytest.raises(SourceUnavailableError):
                    await dataset.fetch("soja", ano=2024)
            case = "test_uf_does_not_fallback_to_national_abiove"
            with check(case), isolated_dataset_case(case):
                dataset = ExportacaoDataset()
                comexstat_fetch = AsyncMock(side_effect=httpx.ConnectError("down"))

                with (
                    patch.object(dataset.info.sources[0], "fetch_fn", comexstat_fetch),
                    patch.object(dataset.info.sources[1], "fetch_fn", _fetch_abiove),
                    patch("agrobr.abiove.exportacao", new_callable=AsyncMock) as abiove_exportacao,
                    pytest.raises(SourceUnavailableError) as exc_info,
                ):
                    await dataset.fetch("milho", ano=2024, uf="MT")

                assert exc_info.value.errors[1][0:2] == ("abiove", "unavailable")
                assert "apenas totais nacionais" in exc_info.value.errors[1][2]
                abiove_exportacao.assert_not_awaited()


class TestExportacaoFetch:
    async def test_exportacao_fetch_casos_1(self):
        with collect_failures() as check:
            case = "test_snapshot_rejected_before_source"
            with check(case), isolated_dataset_case(case):
                dataset = ExportacaoDataset()
                mock_fn = make_source(_mock_export_df())
                dataset.info.sources[0].fetch_fn = mock_fn

                async with deterministic("2024-06-15"):
                    with pytest.raises(InvalidParameterError, match="deterministic"):
                        await dataset.fetch("soja")

                mock_fn.assert_not_awaited()
            case = "test_snapshot_explicit_year_rejected_before_source"
            with check(case), isolated_dataset_case(case):
                dataset = ExportacaoDataset()
                mock_fn = make_source(_mock_export_df())
                dataset.info.sources[0].fetch_fn = mock_fn

                async with deterministic("2024-06-15"):
                    with pytest.raises(InvalidParameterError, match="deterministic"):
                        await dataset.fetch("soja", ano=2023)

                mock_fn.assert_not_awaited()
