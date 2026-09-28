from __future__ import annotations

from datetime import date
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from agrobr.b3 import api, client, parser
from agrobr.models import MetaInfo

GOLDEN_OI_DIR = Path(__file__).parent.parent / "golden_data" / "b3" / "posicoes_sample"


def _make_zip_fixture():
    from tests.test_b3.test_parser import _make_bvmf_xml, _make_nested_zip, _make_pric_rpt

    xml = _make_bvmf_xml(
        _make_pric_rpt(
            "BGIG25",
            trade_dt="2025-02-13",
            prev_adj="310.00",
            adj="311.00",
            var="1.00",
            adj_val="330.00",
        ),
        _make_pric_rpt(
            "BGIH25",
            trade_dt="2025-02-13",
            prev_adj="315.00",
            adj="316.00",
            var="1.00",
            adj_val="330.00",
        ),
        _make_pric_rpt(
            "CCMH25",
            trade_dt="2025-02-13",
            prev_adj="70.50",
            adj="71.00",
            var="0.50",
            adj_val="135.00",
        ),
        _make_pric_rpt(
            "CCMK25",
            trade_dt="2025-02-13",
            prev_adj="72.00",
            adj="72.50",
            var="0.50",
            adj_val="135.00",
        ),
        _make_pric_rpt(
            "ICFH25",
            trade_dt="2025-02-13",
            prev_adj="450.00",
            adj="455.00",
            var="5.00",
            adj_val="100.00",
        ),
        _make_pric_rpt(
            "SJCK25",
            trade_dt="2025-02-13",
            prev_adj="25.00",
            adj="25.50",
            var="0.50",
            adj_val="115.00",
        ),
        _make_pric_rpt(
            "SOYF25",
            trade_dt="2025-02-13",
            prev_adj="380.00",
            adj="381.00",
            var="1.00",
            adj_val="450.00",
        ),
        _make_pric_rpt(
            "ETHH25",
            trade_dt="2025-02-13",
            prev_adj="2800.00",
            adj="2810.00",
            var="10.00",
            adj_val="84.30",
        ),
        _make_pric_rpt(
            "CNLH25",
            trade_dt="2025-02-13",
            prev_adj="4200.00",
            adj="4220.00",
            var="20.00",
            adj_val="100.00",
        ),
    )
    return _make_nested_zip(xml)


def _make_empty_zip_fixture():
    from tests.test_b3.test_parser import _make_bvmf_xml, _make_nested_zip

    xml = _make_bvmf_xml()
    return _make_nested_zip(xml)


class TestAjustes:
    @pytest.fixture
    def mock_fetch_zip(self):
        zip_bytes = _make_zip_fixture()
        with patch.object(
            client,
            "fetch_ajustes_zip",
            new_callable=AsyncMock,
            return_value=(
                zip_bytes,
                "https://www.b3.com.br/pesquisapregao/download?filelist=PR250213.zip",
            ),
        ) as mock:
            yield mock

    @pytest.fixture
    def mock_fetch_zip_empty(self):
        zip_bytes = _make_empty_zip_fixture()
        with patch.object(
            client,
            "fetch_ajustes_zip",
            new_callable=AsyncMock,
            return_value=(
                zip_bytes,
                "https://www.b3.com.br/pesquisapregao/download?filelist=PR250215.zip",
            ),
        ) as mock:
            yield mock

    @pytest.mark.asyncio
    async def test_filter_contrato_unknown_returns_empty(self, mock_fetch_zip):  # noqa: ARG002
        df = await api.ajustes(data="13/02/2025", contrato="XYZ")
        assert len(df) == 0

    @pytest.mark.asyncio
    async def test_empty_meta_zero_records(self, mock_fetch_zip_empty):  # noqa: ARG002
        _, meta = await api.ajustes(data="15/02/2025", return_meta=True)
        assert meta.records_count == 0


class TestContratos:
    def test_returns_sorted_list(self):
        result = api.contratos()
        assert isinstance(result, list)
        assert result == sorted(result)


def _golden_oi_csv() -> bytes:
    return GOLDEN_OI_DIR.joinpath("response.csv").read_bytes()


class TestPosicoesAbertas:
    @pytest.fixture
    def mock_fetch_oi(self):
        csv_bytes = _golden_oi_csv()
        with patch.object(
            client,
            "fetch_posicoes_abertas",
            new_callable=AsyncMock,
            return_value=(csv_bytes, "https://arquivos.b3.com.br/test"),
        ) as mock:
            yield mock

    @pytest.mark.asyncio
    async def test_filter_contrato_unknown_returns_empty(self, mock_fetch_oi):  # noqa: ARG002
        df = await api.posicoes_abertas(data="2025-12-19", contrato="XYZ")
        assert len(df) == 0

    @pytest.mark.asyncio
    async def test_return_meta(self, mock_fetch_oi):  # noqa: ARG002
        result = await api.posicoes_abertas(data="2025-12-19", return_meta=True)
        assert isinstance(result, tuple)
        df, meta = result
        assert isinstance(df, pd.DataFrame)
        assert isinstance(meta, MetaInfo)
        assert meta.source == "b3"
        assert meta.records_count == len(df)
        assert meta.parser_version == parser.PARSER_VERSION_OI
        assert meta.source_method == "httpx+csv"


class TestOiHistorico:
    @pytest.fixture
    def mock_fetch_oi(self):
        csv_bytes = _golden_oi_csv()
        with patch.object(
            client,
            "fetch_posicoes_abertas",
            new_callable=AsyncMock,
            return_value=(csv_bytes, "https://arquivos.b3.com.br/test"),
        ) as mock:
            yield mock

    @pytest.mark.asyncio
    async def test_filter_vencimento(self, mock_fetch_oi):  # noqa: ARG002
        df = await api.oi_historico(
            contrato="boi",
            inicio=date(2025, 12, 19),
            fim=date(2025, 12, 19),
            vencimento="F26",
        )
        assert len(df) > 0
        assert set(zip(df["vencimento_ano"], df["vencimento_mes"], strict=True)) == {(2026, 1)}
        assert set(df["tipo"]) == {"futuro", "opcao"}


class TestAjustesAsPolars:
    @pytest.mark.asyncio
    async def test_as_polars(self):
        pl = pytest.importorskip("polars")
        zip_bytes = _make_zip_fixture()
        with patch.object(
            client,
            "fetch_ajustes_zip",
            new_callable=AsyncMock,
            return_value=(
                zip_bytes,
                "https://www.b3.com.br/pesquisapregao/download?filelist=PR250213.zip",
            ),
        ):
            result = await api.ajustes(data="13/02/2025", as_polars=True)
        assert isinstance(result, pl.DataFrame)
