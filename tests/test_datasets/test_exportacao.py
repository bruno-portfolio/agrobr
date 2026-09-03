"""Testes específicos para o dataset exportacao (fetch com mock + prioridade)."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch

import httpx
import pandas as pd
import pytest

from agrobr.datasets.deterministic import deterministic
from agrobr.datasets.exportacao import (
    EXPORTACAO_INFO,
    ExportacaoDataset,
    _fetch_abiove,
    _fetch_comexstat,
    exportacao,
)
from agrobr.exceptions import SourceFallbackWarning, SourceUnavailableError

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


class TestExportacaoSpecific:
    def test_info_comexstat_priority(self):
        comexstat = next(s for s in EXPORTACAO_INFO.sources if s.name == "comexstat")
        abiove = next(s for s in EXPORTACAO_INFO.sources if s.name == "abiove")
        assert comexstat.priority < abiove.priority


class TestExportacaoFetch:
    @pytest.mark.asyncio
    async def test_fetch_returns_dataframe(self):
        dataset = ExportacaoDataset()
        dataset.info.sources[0].fetch_fn = make_source(_mock_export_df())
        df = await dataset.fetch("soja", ano=2024)

        assert len(df) == 1
        assert "kg_liquido" in df.columns
        assert df.iloc[0]["valor_fob_usd"] == 2500000000

    @pytest.mark.asyncio
    async def test_fetch_return_meta(self):
        dataset = ExportacaoDataset()
        dataset.info.sources[0].fetch_fn = make_source(_mock_export_df())
        df, meta = await dataset.fetch("soja", ano=2024, return_meta=True)

        assert meta.dataset == "exportacao"
        assert meta.contract_version == "1.0"
        assert "comexstat" in meta.attempted_sources
        assert meta.records_count == len(df)

    @pytest.mark.asyncio
    async def test_fetch_invalid_produto(self):
        dataset = ExportacaoDataset()
        with pytest.raises(ValueError, match="não suportado"):
            await dataset.fetch("banana")

    @pytest.mark.asyncio
    async def test_fetch_forwards_uf(self):
        dataset = ExportacaoDataset()
        mock_fn = make_source(_mock_export_df())
        dataset.info.sources[0].fetch_fn = mock_fn

        await dataset.fetch("soja", ano=2024, uf="MT")

        _, kwargs = mock_fn.call_args
        assert kwargs["uf"] == "MT"

    @pytest.mark.asyncio
    async def test_snapshot_sets_ano(self):
        dataset = ExportacaoDataset()
        mock_fn = make_source(_mock_export_df())
        dataset.info.sources[0].fetch_fn = mock_fn

        async with deterministic("2024-06-15"):
            await dataset.fetch("soja")

        _, kwargs = mock_fn.call_args
        assert kwargs["ano"] == 2024

    @pytest.mark.asyncio
    async def test_snapshot_does_not_override_explicit_ano(self):
        dataset = ExportacaoDataset()
        mock_fn = make_source(_mock_export_df())
        dataset.info.sources[0].fetch_fn = mock_fn

        async with deterministic("2024-06-15"):
            await dataset.fetch("soja", ano=2023)

        _, kwargs = mock_fn.call_args
        assert kwargs["ano"] == 2023


class TestExportacaoNormalize:
    @pytest.mark.asyncio
    async def test_normalize_adds_produto(self):
        df = _mock_export_df().drop(columns=["produto"])
        dataset = ExportacaoDataset()
        dataset.info.sources[0].fetch_fn = make_source(df)

        result = await dataset.fetch("soja", ano=2024)

        assert result["produto"].iloc[0] == "soja"

    @pytest.mark.asyncio
    async def test_normalize_keeps_existing_produto(self):
        dataset = ExportacaoDataset()
        dataset.info.sources[0].fetch_fn = make_source(_mock_export_df())

        result = await dataset.fetch("soja", ano=2024)

        assert result["produto"].iloc[0] == "soja"


class TestExportacaoFallback:
    @pytest.mark.asyncio
    async def test_comexstat_fails_falls_back_to_abiove(self):
        dataset = ExportacaoDataset()
        dataset.info.sources[0].fetch_fn = AsyncMock(side_effect=httpx.ConnectError("down"))
        dataset.info.sources[1].fetch_fn = make_source(_mock_export_df())

        with pytest.warns(SourceFallbackWarning, match="abiove"):
            df, meta = await dataset.fetch("soja", ano=2024, return_meta=True)

        assert len(df) == 1
        assert meta.attempted_sources == ["comexstat", "abiove"]
        assert meta.selected_source == "abiove"

    @pytest.mark.asyncio
    async def test_all_sources_fail(self):
        dataset = ExportacaoDataset()
        dataset.info.sources[0].fetch_fn = AsyncMock(side_effect=httpx.ConnectError("down"))
        dataset.info.sources[1].fetch_fn = AsyncMock(side_effect=httpx.ConnectError("down"))

        with pytest.raises(SourceUnavailableError):
            await dataset.fetch("soja", ano=2024)

    @pytest.mark.asyncio
    async def test_uf_does_not_fallback_to_national_abiove(self):
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

    @pytest.mark.asyncio
    async def test_soja_fallback_normalizes_abiove_result(self):
        dataset = ExportacaoDataset()
        abiove_df = pd.DataFrame(
            {
                "ano": [2024],
                "mes": [1],
                "produto": ["grao"],
                "volume_ton": [100.0],
                "receita_usd_mil": [50.0],
            }
        )
        source_meta = mock_source_meta()

        with (
            patch.object(
                dataset.info.sources[0],
                "fetch_fn",
                AsyncMock(side_effect=httpx.ConnectError("down")),
            ),
            patch.object(dataset.info.sources[1], "fetch_fn", _fetch_abiove),
            patch(
                "agrobr.abiove.exportacao",
                new_callable=AsyncMock,
                return_value=(abiove_df, source_meta),
            ) as abiove_exportacao,
            pytest.warns(SourceFallbackWarning, match="abiove"),
        ):
            df, meta = await dataset.fetch("soja", ano=2024, return_meta=True)

        assert df["produto"].tolist() == ["soja"]
        assert df["uf"].isna().all()
        assert df["kg_liquido"].tolist() == [100000.0]
        assert meta.selected_source == "abiove"
        abiove_exportacao.assert_awaited_once_with(
            ano=2024,
            mes=None,
            produto="grao",
            return_meta=True,
        )


class TestExportacaoPublicAPI:
    @pytest.mark.asyncio
    async def test_public_function_delegates(self):
        with patch.object(ExportacaoDataset, "fetch", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = _mock_export_df()
            await exportacao("soja", ano=2024, uf="MT")

            mock_fetch.assert_called_once_with("soja", ano=2024, uf="MT", return_meta=False)

    @pytest.mark.asyncio
    async def test_public_function_return_meta(self):
        with patch.object(ExportacaoDataset, "fetch", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = (_mock_export_df(), mock_source_meta())
            result = await exportacao("soja", ano=2024, return_meta=True)

            assert isinstance(result, tuple)
            assert len(result) == 2
            assert isinstance(result[0], pd.DataFrame)


class TestExportacaoFetchFunctions:
    @pytest.mark.asyncio
    async def test_fetch_comexstat_forwards_params(self):
        meta = mock_source_meta()
        with patch(
            "agrobr.comexstat.exportacao",
            new_callable=AsyncMock,
            return_value=(_mock_export_df(), meta),
        ) as mock_fn:
            await _fetch_comexstat("soja", ano=2024, uf="PR")
        mock_fn.assert_called_once_with("soja", ano=2024, uf="PR", return_meta=True)

    @pytest.mark.asyncio
    async def test_fetch_comexstat_aggregates_soybean_oil_ncms(self):
        source_df = pd.DataFrame(
            {
                "ano": [2024, 2024, 2024, 2024],
                "mes": [1, 1, 1, 2],
                "ncm": ["15071000", "15079011", "15079090", "15071000"],
                "uf": ["MT", "MT", "PR", "MT"],
                "kg_liquido": [1000.0, 2500.0, 3000.0, 4000.0],
                "valor_fob_usd": [500.0, 1000.0, 1500.0, 2000.0],
                "volume_ton": [1.0, 2.5, 3.0, 4.0],
            }
        )

        with patch(
            "agrobr.comexstat.exportacao",
            new_callable=AsyncMock,
            return_value=(source_df, mock_source_meta()),
        ):
            result_df, _ = await _fetch_comexstat("oleo_soja", ano=2024)

        jan_mt = result_df[(result_df["mes"] == 1) & (result_df["uf"] == "MT")].iloc[0]
        assert len(result_df) == 3
        assert "ncm" not in result_df.columns
        assert jan_mt["kg_liquido"] == 3500.0
        assert jan_mt["valor_fob_usd"] == 1500.0
        assert jan_mt["volume_ton"] == 3.5

    @pytest.mark.asyncio
    async def test_fetch_comexstat_keeps_specific_product_rows(self):
        source_df = pd.DataFrame(
            {
                "ano": [2024],
                "mes": [1],
                "ncm": ["15071000"],
                "uf": ["MT"],
                "kg_liquido": [1000.0],
                "valor_fob_usd": [500.0],
                "volume_ton": [1.0],
            }
        )

        with patch(
            "agrobr.comexstat.exportacao",
            new_callable=AsyncMock,
            return_value=(source_df, mock_source_meta()),
        ):
            result_df, _ = await _fetch_comexstat("oleo_soja_bruto", ano=2024)

        pd.testing.assert_frame_equal(result_df, source_df)

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

    @pytest.mark.asyncio
    async def test_fetch_abiove_column_transform(self):
        df = pd.DataFrame({"volume_ton": [100.0], "receita_usd_mil": [50.0]})
        meta = mock_source_meta()
        with patch("agrobr.abiove.exportacao", new_callable=AsyncMock, return_value=(df, meta)):
            from agrobr.datasets.exportacao import _fetch_abiove

            result_df, _ = await _fetch_abiove("soja", ano=2024)
        assert result_df["kg_liquido"].iloc[0] == 100000.0
        assert result_df["valor_fob_usd"].iloc[0] == 50000.0

    @pytest.mark.asyncio
    async def test_fetch_abiove_defaults_none_ano(self):
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
            mes=None,
            produto="milho",
            return_meta=True,
        )

    @pytest.mark.asyncio
    async def test_fetch_abiove_rejects_uf_before_source_call(self):
        with (
            patch("agrobr.abiove.exportacao", new_callable=AsyncMock) as mock_fn,
            pytest.raises(SourceUnavailableError, match="apenas totais nacionais"),
        ):
            await _fetch_abiove("milho", ano=2024, uf="MT")

        mock_fn.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("produto", "produto_abiove"),
        [
            ("soja", "grao"),
            ("farelo_soja", "farelo"),
            ("oleo_soja", "oleo"),
            ("milho", "milho"),
        ],
    )
    async def test_fetch_abiove_maps_produto_and_normalizes_output(
        self,
        produto: str,
        produto_abiove: str,
    ):
        source_df = pd.DataFrame(
            {
                "ano": [2024],
                "mes": [1],
                "produto": [produto_abiove],
                "volume_ton": [100.0],
                "receita_usd_mil": [50.0],
            }
        )

        with patch(
            "agrobr.abiove.exportacao",
            new_callable=AsyncMock,
            return_value=(source_df, mock_source_meta()),
        ) as mock_fn:
            result_df, _ = await _fetch_abiove(produto, ano=2024)

        assert result_df["produto"].tolist() == [produto]
        assert result_df["uf"].isna().all()
        mock_fn.assert_awaited_once_with(
            ano=2024,
            mes=None,
            produto=produto_abiove,
            return_meta=True,
        )

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

    @pytest.mark.asyncio
    async def test_fetch_abiove_skip_transform_if_cols_exist(self):
        df = pd.DataFrame(
            {
                "volume_ton": [100.0],
                "kg_liquido": [999.0],
                "receita_usd_mil": [50.0],
                "valor_fob_usd": [888.0],
            }
        )
        meta = mock_source_meta()
        with patch("agrobr.abiove.exportacao", new_callable=AsyncMock, return_value=(df, meta)):
            from agrobr.datasets.exportacao import _fetch_abiove

            result_df, _ = await _fetch_abiove("soja", ano=2024)
        assert result_df["kg_liquido"].iloc[0] == 999.0
        assert result_df["valor_fob_usd"].iloc[0] == 888.0
