from __future__ import annotations

from datetime import date
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from agrobr.b3 import api, client, parser
from agrobr.b3.models import B3_CONTRATOS_AGRO, COLUNAS_OI_SAIDA, COLUNAS_SAIDA
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
    async def test_returns_dataframe(self, mock_fetch_zip):  # noqa: ARG002
        df = await api.ajustes(data="13/02/2025")
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0

    @pytest.mark.asyncio
    async def test_columns_match_schema(self, mock_fetch_zip):  # noqa: ARG002
        df = await api.ajustes(data="13/02/2025")
        for col in COLUNAS_SAIDA:
            assert col in df.columns

    @pytest.mark.asyncio
    async def test_accepts_date_object(self, mock_fetch_zip):
        df = await api.ajustes(data=date(2025, 2, 13))
        assert isinstance(df, pd.DataFrame)
        mock_fetch_zip.assert_called_once_with("13/02/2025")

    @pytest.mark.asyncio
    async def test_filter_contrato_by_name(self, mock_fetch_zip):  # noqa: ARG002
        df = await api.ajustes(data="13/02/2025", contrato="boi")
        assert len(df) > 0
        assert (df["ticker"] == "BGI").all()

    @pytest.mark.asyncio
    async def test_filter_contrato_by_ticker(self, mock_fetch_zip):  # noqa: ARG002
        df = await api.ajustes(data="13/02/2025", contrato="CCM")
        assert len(df) > 0
        assert (df["ticker"] == "CCM").all()

    @pytest.mark.asyncio
    async def test_filter_contrato_unknown_returns_empty(self, mock_fetch_zip):  # noqa: ARG002
        df = await api.ajustes(data="13/02/2025", contrato="XYZ")
        assert len(df) == 0

    @pytest.mark.asyncio
    async def test_return_meta(self, mock_fetch_zip):  # noqa: ARG002
        result = await api.ajustes(data="13/02/2025", return_meta=True)
        assert isinstance(result, tuple)
        df, meta = result
        assert isinstance(df, pd.DataFrame)
        assert isinstance(meta, MetaInfo)
        assert meta.source == "b3"
        assert meta.records_count == len(df)
        assert meta.parser_version == parser.PARSER_VERSION_ZIP
        assert meta.source_method == "httpx+zip+xml"
        assert "b3.com.br" in meta.source_url

    @pytest.mark.asyncio
    async def test_meta_fetch_duration(self, mock_fetch_zip):  # noqa: ARG002
        _, meta = await api.ajustes(data="13/02/2025", return_meta=True)
        assert meta.fetch_duration_ms >= 0
        assert meta.parse_duration_ms >= 0

    @pytest.mark.asyncio
    async def test_empty_returns_empty(self, mock_fetch_zip_empty):  # noqa: ARG002
        df = await api.ajustes(data="15/02/2025")
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0

    @pytest.mark.asyncio
    async def test_empty_meta_zero_records(self, mock_fetch_zip_empty):  # noqa: ARG002
        _, meta = await api.ajustes(data="15/02/2025", return_meta=True)
        assert meta.records_count == 0


class TestHistorico:
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

    @pytest.mark.asyncio
    async def test_returns_dataframe(self, mock_fetch_zip):  # noqa: ARG002
        df = await api.historico(
            contrato="boi",
            inicio=date(2025, 2, 13),
            fim=date(2025, 2, 13),
        )
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0

    @pytest.mark.asyncio
    async def test_accepts_string_dates(self, mock_fetch_zip):  # noqa: ARG002
        df = await api.historico(
            contrato="boi",
            inicio="2025-02-13",
            fim="2025-02-13",
        )
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0

    @pytest.mark.asyncio
    async def test_skips_weekends(self, mock_fetch_zip):
        await api.historico(
            contrato="boi",
            inicio=date(2025, 2, 8),
            fim=date(2025, 2, 9),
        )
        assert mock_fetch_zip.call_count == 0

    @pytest.mark.asyncio
    async def test_multiple_days(self, mock_fetch_zip):
        df = await api.historico(
            contrato="boi",
            inicio=date(2025, 2, 10),
            fim=date(2025, 2, 14),
        )
        assert mock_fetch_zip.call_count == 5
        assert len(df) > 0

    @pytest.mark.asyncio
    async def test_filter_vencimento(self, mock_fetch_zip):  # noqa: ARG002
        df = await api.historico(
            contrato="boi",
            inicio=date(2025, 2, 13),
            fim=date(2025, 2, 13),
            vencimento="G25",
        )
        assert len(df) > 0
        assert (df["vencimento_codigo"] == "G25").all()

    @pytest.mark.asyncio
    async def test_return_meta(self, mock_fetch_zip):  # noqa: ARG002
        result = await api.historico(
            contrato="boi",
            inicio=date(2025, 2, 13),
            fim=date(2025, 2, 13),
            return_meta=True,
        )
        assert isinstance(result, tuple)
        df, meta = result
        assert isinstance(meta, MetaInfo)
        assert meta.source == "b3"
        assert meta.source_method == "httpx+zip+xml"

    @pytest.mark.asyncio
    async def test_empty_range_returns_empty(self, mock_fetch_zip):  # noqa: ARG002
        df = await api.historico(
            contrato="boi",
            inicio=date(2025, 2, 15),
            fim=date(2025, 2, 14),
        )
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0


class TestContratos:
    def test_returns_sorted_list(self):
        result = api.contratos()
        assert isinstance(result, list)
        assert result == sorted(result)

    def test_all_contratos_present(self):
        result = api.contratos()
        for nome in B3_CONTRATOS_AGRO:
            assert nome in result

    def test_count_matches(self):
        result = api.contratos()
        assert len(result) == len(B3_CONTRATOS_AGRO)


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
    async def test_returns_dataframe(self, mock_fetch_oi):  # noqa: ARG002
        df = await api.posicoes_abertas(data="2025-12-19")
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0

    @pytest.mark.asyncio
    async def test_columns_match_schema(self, mock_fetch_oi):  # noqa: ARG002
        df = await api.posicoes_abertas(data="2025-12-19")
        for col in COLUNAS_OI_SAIDA:
            assert col in df.columns

    @pytest.mark.asyncio
    async def test_accepts_date_object(self, mock_fetch_oi):
        df = await api.posicoes_abertas(data=date(2025, 12, 19))
        assert isinstance(df, pd.DataFrame)
        mock_fetch_oi.assert_called_once_with("2025-12-19")

    @pytest.mark.asyncio
    async def test_filter_contrato_by_name(self, mock_fetch_oi):  # noqa: ARG002
        df = await api.posicoes_abertas(data="2025-12-19", contrato="boi")
        assert len(df) > 0
        assert (df["ticker"] == "BGI").all()

    @pytest.mark.asyncio
    async def test_filter_contrato_by_ticker(self, mock_fetch_oi):  # noqa: ARG002
        df = await api.posicoes_abertas(data="2025-12-19", contrato="CCM")
        assert len(df) > 0
        assert (df["ticker"] == "CCM").all()

    @pytest.mark.asyncio
    async def test_filter_contrato_unknown_returns_empty(self, mock_fetch_oi):  # noqa: ARG002
        df = await api.posicoes_abertas(data="2025-12-19", contrato="XYZ")
        assert len(df) == 0

    @pytest.mark.asyncio
    async def test_filter_tipo_futuro(self, mock_fetch_oi):  # noqa: ARG002
        df = await api.posicoes_abertas(data="2025-12-19", tipo="futuro")
        assert len(df) > 0
        assert (df["tipo"] == "futuro").all()

    @pytest.mark.asyncio
    async def test_filter_tipo_opcao(self, mock_fetch_oi):  # noqa: ARG002
        df = await api.posicoes_abertas(data="2025-12-19", tipo="opcao")
        assert len(df) > 0
        assert (df["tipo"] == "opcao").all()

    @pytest.mark.asyncio
    async def test_filter_contrato_and_tipo(self, mock_fetch_oi):  # noqa: ARG002
        df = await api.posicoes_abertas(data="2025-12-19", contrato="boi", tipo="futuro")
        assert len(df) > 0
        assert (df["ticker"] == "BGI").all()
        assert (df["tipo"] == "futuro").all()

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

    @pytest.mark.asyncio
    async def test_meta_fetch_duration(self, mock_fetch_oi):  # noqa: ARG002
        _, meta = await api.posicoes_abertas(data="2025-12-19", return_meta=True)
        assert meta.fetch_duration_ms >= 0
        assert meta.parse_duration_ms >= 0


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
    async def test_returns_dataframe(self, mock_fetch_oi):  # noqa: ARG002
        df = await api.oi_historico(
            contrato="boi",
            inicio=date(2025, 12, 19),
            fim=date(2025, 12, 19),
        )
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0

    @pytest.mark.asyncio
    async def test_accepts_string_dates(self, mock_fetch_oi):  # noqa: ARG002
        df = await api.oi_historico(
            contrato="boi",
            inicio="2025-12-19",
            fim="2025-12-19",
        )
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0

    @pytest.mark.asyncio
    async def test_skips_weekends(self, mock_fetch_oi):
        await api.oi_historico(
            contrato="boi",
            inicio=date(2025, 12, 20),
            fim=date(2025, 12, 21),
        )
        assert mock_fetch_oi.call_count == 0

    @pytest.mark.asyncio
    async def test_multiple_days(self, mock_fetch_oi):
        df = await api.oi_historico(
            contrato="boi",
            inicio=date(2025, 12, 15),
            fim=date(2025, 12, 19),
        )
        assert mock_fetch_oi.call_count == 5
        assert len(df) > 0

    @pytest.mark.asyncio
    async def test_filter_vencimento(self, mock_fetch_oi):  # noqa: ARG002
        df = await api.oi_historico(
            contrato="boi",
            inicio=date(2025, 12, 19),
            fim=date(2025, 12, 19),
            vencimento="F26",
        )
        assert len(df) > 0
        assert (df["vencimento_codigo"] == "F26").all()

    @pytest.mark.asyncio
    async def test_filter_tipo(self, mock_fetch_oi):  # noqa: ARG002
        df = await api.oi_historico(
            contrato="boi",
            inicio=date(2025, 12, 19),
            fim=date(2025, 12, 19),
            tipo="futuro",
        )
        assert len(df) > 0
        assert (df["tipo"] == "futuro").all()

    @pytest.mark.asyncio
    async def test_return_meta(self, mock_fetch_oi):  # noqa: ARG002
        result = await api.oi_historico(
            contrato="boi",
            inicio=date(2025, 12, 19),
            fim=date(2025, 12, 19),
            return_meta=True,
        )
        assert isinstance(result, tuple)
        df, meta = result
        assert isinstance(meta, MetaInfo)
        assert meta.source == "b3"

    @pytest.mark.asyncio
    async def test_empty_range_returns_empty(self, mock_fetch_oi):  # noqa: ARG002
        df = await api.oi_historico(
            contrato="boi",
            inicio=date(2025, 12, 20),
            fim=date(2025, 12, 19),
        )
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0


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


class TestAjustesZipDirect:
    @pytest.mark.asyncio
    async def test_zip_success(self):
        from tests.test_b3.test_parser import _make_bvmf_xml, _make_nested_zip, _make_pric_rpt

        xml = _make_bvmf_xml(_make_pric_rpt("BGIH27"))
        zip_bytes = _make_nested_zip(xml)

        with patch.object(
            client,
            "fetch_ajustes_zip",
            new_callable=AsyncMock,
            return_value=(
                zip_bytes,
                "https://www.b3.com.br/pesquisapregao/download?filelist=PR260227.zip",
            ),
        ):
            result = await api.ajustes(data="27/02/2026", return_meta=True)

        df, meta = result
        assert len(df) == 1
        assert meta.source_method == "httpx+zip+xml"
        assert meta.parser_version == parser.PARSER_VERSION_ZIP
