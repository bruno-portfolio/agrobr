"""Testes para a API pública de custo de produção CONAB."""

from io import BytesIO
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from agrobr.conab.custo_producao import api


def _make_sample_xlsx() -> BytesIO:
    """Cria planilha Excel de exemplo para mocks."""
    rows = [
        ["CUSTO DE PRODUÇÃO - SOJA - MT - ALTA TECNOLOGIA", None, None, None, None, None],
        [None, None, None, None, None, None],
        ["Item / Especificação", "Unidade", "Qtd./ha", "Preço Unitário (R$)", "Valor Total/ha (R$)", "Participação (%)"],
        ["Sementes", "kg", 60.0, 8.50, 510.00, 17.72],
        ["Fertilizantes", "kg", 200.0, 4.20, 840.00, 29.18],
        ["Herbicidas", "L", 3.0, 25.00, 75.00, 2.60],
        ["Preparo do solo", "h/m", 1.5, 180.00, 270.00, 9.38],
        ["Colheita mecânica", "h/m", 1.0, 350.00, 350.00, 12.16],
        ["Mão de obra temporária", "d/h", 3.0, 80.00, 240.00, 8.34],
        ["CUSTO OPERACIONAL EFETIVO (COE)", None, None, None, 2879.00, 100.00],
    ]

    df = pd.DataFrame(rows)
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Plan1", index=False, header=False)
    buf.seek(0)
    return buf


class TestCustoProducao:
    @pytest.mark.asyncio
    async def test_returns_dataframe(self):
        xlsx = _make_sample_xlsx()
        metadata = {"url": "https://test.com/soja_mt.xlsx", "titulo": "Soja MT", "cultura": "soja", "uf": "MT", "safra": "2023/24"}

        with patch.object(api.client, "fetch_xlsx_for_cultura", new_callable=AsyncMock, return_value=(xlsx, metadata)):
            df = await api.custo_producao("soja", uf="MT", safra="2023/24")

        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0
        assert "cultura" in df.columns
        assert "valor_ha" in df.columns
        assert all(df["cultura"] == "soja")

    @pytest.mark.asyncio
    async def test_return_meta(self):
        xlsx = _make_sample_xlsx()
        metadata = {"url": "https://test.com/soja_mt.xlsx", "titulo": "Soja MT", "cultura": "soja", "uf": "MT", "safra": "2023/24"}

        with patch.object(api.client, "fetch_xlsx_for_cultura", new_callable=AsyncMock, return_value=(xlsx, metadata)):
            df, meta = await api.custo_producao("soja", uf="MT", safra="2023/24", return_meta=True)

        assert meta.source == "conab_custo"
        assert meta.attempted_sources == ["conab_custo"]
        assert meta.selected_source == "conab_custo"
        assert meta.fetch_timestamp is not None
        assert meta.records_count == len(df)
        assert meta.parser_version >= 1


class TestCustoProducaoTotal:
    @pytest.mark.asyncio
    async def test_returns_dict(self):
        xlsx = _make_sample_xlsx()
        metadata = {"url": "https://test.com/soja_mt.xlsx", "titulo": "Soja MT", "cultura": "soja", "uf": "MT", "safra": "2023/24"}

        with patch.object(api.client, "fetch_xlsx_for_cultura", new_callable=AsyncMock, return_value=(xlsx, metadata)):
            result = await api.custo_producao_total("soja", uf="MT", safra="2023/24")

        assert isinstance(result, dict)
        assert "coe_ha" in result
        assert result["coe_ha"] == pytest.approx(2879.0)
        assert result["cultura"] == "soja"
        assert result["uf"] == "MT"

    @pytest.mark.asyncio
    async def test_return_meta(self):
        xlsx = _make_sample_xlsx()
        metadata = {"url": "https://test.com/soja_mt.xlsx", "titulo": "Soja MT", "cultura": "soja", "uf": "MT", "safra": "2023/24"}

        with patch.object(api.client, "fetch_xlsx_for_cultura", new_callable=AsyncMock, return_value=(xlsx, metadata)):
            result, meta = await api.custo_producao_total("soja", uf="MT", safra="2023/24", return_meta=True)

        assert meta.source == "conab_custo"
        assert meta.records_count == 1
        assert result["coe_ha"] > 0
