"""Testes para a API publica de serie historica CONAB."""

from io import BytesIO
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from agrobr.conab.serie_historica import api
from agrobr.conab.serie_historica.api import produtos_disponiveis


def _make_sample_xls() -> BytesIO:
    """Cria arquivo Excel de exemplo com 3 abas para mocks."""
    area_rows = [
        ["CONAB - Soja - Área Plantada (mil ha)", None, None],
        [None, None, None],
        ["Região/UF", "2022/23", "2023/24"],
        ["CENTRO-OESTE", None, None],
        ["MT", 11400.0, 12000.0],
        ["GO", 4300.0, 4500.0],
        ["SUL", None, None],
        ["PR", 5700.0, 5900.0],
        ["BRASIL", 44000.0, 46000.0],
    ]

    producao_rows = [
        ["CONAB - Soja - Produção (mil ton)", None, None],
        [None, None, None],
        ["Região/UF", "2022/23", "2023/24"],
        ["CENTRO-OESTE", None, None],
        ["MT", 39000.0, 41000.0],
        ["GO", 15000.0, 15800.0],
        ["SUL", None, None],
        ["PR", 21000.0, 22000.0],
        ["BRASIL", 154600.0, 160000.0],
    ]

    produtividade_rows = [
        ["CONAB - Soja - Produtividade (kg/ha)", None, None],
        [None, None, None],
        ["Região/UF", "2022/23", "2023/24"],
        ["CENTRO-OESTE", None, None],
        ["MT", 3421.0, 3417.0],
        ["GO", 3488.0, 3511.0],
        ["SUL", None, None],
        ["PR", 3684.0, 3729.0],
        ["BRASIL", 3514.0, 3478.0],
    ]

    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        for name, rows in [
            ("Area", area_rows),
            ("Producao", producao_rows),
            ("Produtividade", produtividade_rows),
        ]:
            df = pd.DataFrame(rows)
            df.to_excel(writer, sheet_name=name, index=False, header=False)
    buf.seek(0)
    return buf


class TestSerieHistorica:
    @pytest.mark.asyncio
    async def test_returns_dataframe(self):
        xls = _make_sample_xls()
        metadata = {
            "url": "https://test.com/sojaseriehist.xls",
            "produto": "soja",
            "categoria": "graos",
            "size_bytes": 1024,
            "content_type": "application/vnd.ms-excel",
        }

        with patch.object(
            api.client,
            "download_xls",
            new_callable=AsyncMock,
            return_value=(xls, metadata),
        ):
            df = await api.serie_historica("soja")

        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0
        assert "produto" in df.columns
        assert "safra" in df.columns
        assert "uf" in df.columns
        assert "area_plantada_mil_ha" in df.columns
        assert all(df["produto"] == "soja")

    @pytest.mark.asyncio
    async def test_filter_uf(self):
        xls = _make_sample_xls()
        metadata = {
            "url": "https://test.com/sojaseriehist.xls",
            "produto": "soja",
            "categoria": "graos",
            "size_bytes": 1024,
            "content_type": "application/vnd.ms-excel",
        }

        with patch.object(
            api.client,
            "download_xls",
            new_callable=AsyncMock,
            return_value=(xls, metadata),
        ):
            df = await api.serie_historica("soja", uf="MT")

        assert all(df["uf"] == "MT")

    @pytest.mark.asyncio
    async def test_filter_year_range(self):
        xls = _make_sample_xls()
        metadata = {
            "url": "https://test.com/sojaseriehist.xls",
            "produto": "soja",
            "categoria": "graos",
            "size_bytes": 1024,
            "content_type": "application/vnd.ms-excel",
        }

        with patch.object(
            api.client,
            "download_xls",
            new_callable=AsyncMock,
            return_value=(xls, metadata),
        ):
            df = await api.serie_historica("soja", inicio=2023, fim=2023)

        assert all(df["safra"] == "2023/24")

    @pytest.mark.asyncio
    async def test_return_meta(self):
        xls = _make_sample_xls()
        metadata = {
            "url": "https://test.com/sojaseriehist.xls",
            "produto": "soja",
            "categoria": "graos",
            "size_bytes": 1024,
            "content_type": "application/vnd.ms-excel",
        }

        with patch.object(
            api.client,
            "download_xls",
            new_callable=AsyncMock,
            return_value=(xls, metadata),
        ):
            df, meta = await api.serie_historica("soja", return_meta=True)

        assert meta.source == "conab_serie_historica"
        assert meta.attempted_sources == ["conab_serie_historica"]
        assert meta.selected_source == "conab_serie_historica"
        assert meta.fetch_timestamp is not None
        assert meta.records_count == len(df)
        assert meta.parser_version >= 1

    @pytest.mark.asyncio
    async def test_metrics_merged(self):
        xls = _make_sample_xls()
        metadata = {
            "url": "https://test.com/sojaseriehist.xls",
            "produto": "soja",
            "categoria": "graos",
            "size_bytes": 1024,
            "content_type": "application/vnd.ms-excel",
        }

        with patch.object(
            api.client,
            "download_xls",
            new_callable=AsyncMock,
            return_value=(xls, metadata),
        ):
            df = await api.serie_historica("soja", uf="MT")

        mt_2022 = df[df["safra"] == "2022/23"]
        assert len(mt_2022) == 1
        row = mt_2022.iloc[0]
        assert row["area_plantada_mil_ha"] == pytest.approx(11400.0)
        assert row["producao_mil_ton"] == pytest.approx(39000.0)
        assert row["produtividade_kg_ha"] == pytest.approx(3421.0)


class TestProdutosDisponiveis:
    def test_returns_list(self):
        result = produtos_disponiveis()
        assert isinstance(result, list)
        assert len(result) > 0

    def test_has_required_keys(self):
        result = produtos_disponiveis()
        for item in result:
            assert "produto" in item
            assert "categoria" in item
            assert "url" in item

    def test_contains_main_products(self):
        result = produtos_disponiveis()
        products = {item["produto"] for item in result}
        assert "soja" in products
        assert "milho" in products
        assert "arroz" in products
        assert "cafe" in products
        assert "cana" in products

    def test_urls_contain_gov_br(self):
        result = produtos_disponiveis()
        for item in result:
            assert "gov.br" in item["url"]
            assert "/view" in item["url"]
