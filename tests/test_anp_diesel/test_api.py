"""Testes para agrobr.alt.anp_diesel.api."""

from __future__ import annotations

import io
from datetime import date
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from agrobr.alt.anp_diesel import api
from agrobr.models import MetaInfo


def _make_precos_xlsx_bytes(**kwargs) -> bytes:
    """Gera XLSX sintetico de precos."""
    rows = kwargs.get(
        "rows",
        [
            {
                "ESTADO - SIGLA": "SP",
                "MUNICÍPIO": "SAO PAULO",
                "PRODUTO": "DIESEL S10",
                "DATA INICIAL": "15/01/2024",
                "DATA FINAL": "21/01/2024",
                "PREÇO MÉDIO REVENDA": "6.45",
                "PREÇO MÉDIO DISTRIBUIÇÃO": "5.80",
                "NÚMERO DE POSTOS PESQUISADOS": "150",
            },
            {
                "ESTADO - SIGLA": "MT",
                "MUNICÍPIO": "CUIABA",
                "PRODUTO": "DIESEL S10",
                "DATA INICIAL": "15/01/2024",
                "DATA FINAL": "21/01/2024",
                "PREÇO MÉDIO REVENDA": "6.20",
                "PREÇO MÉDIO DISTRIBUIÇÃO": "5.60",
                "NÚMERO DE POSTOS PESQUISADOS": "80",
            },
            {
                "ESTADO - SIGLA": "SP",
                "MUNICÍPIO": "SAO PAULO",
                "PRODUTO": "DIESEL",
                "DATA INICIAL": "15/01/2024",
                "DATA FINAL": "21/01/2024",
                "PREÇO MÉDIO REVENDA": "5.95",
                "PREÇO MÉDIO DISTRIBUIÇÃO": "5.30",
                "NÚMERO DE POSTOS PESQUISADOS": "120",
            },
            {
                "ESTADO - SIGLA": "SP",
                "MUNICÍPIO": "SAO PAULO",
                "PRODUTO": "DIESEL S10",
                "DATA INICIAL": "22/01/2024",
                "DATA FINAL": "28/01/2024",
                "PREÇO MÉDIO REVENDA": "6.50",
                "PREÇO MÉDIO DISTRIBUIÇÃO": "5.85",
                "NÚMERO DE POSTOS PESQUISADOS": "155",
            },
            {
                "ESTADO - SIGLA": "MT",
                "MUNICÍPIO": "CUIABA",
                "PRODUTO": "DIESEL S10",
                "DATA INICIAL": "01/02/2024",
                "DATA FINAL": "07/02/2024",
                "PREÇO MÉDIO REVENDA": "6.25",
                "PREÇO MÉDIO DISTRIBUIÇÃO": "5.62",
                "NÚMERO DE POSTOS PESQUISADOS": "81",
            },
        ],
    )
    df = pd.DataFrame(rows)
    buf = io.BytesIO()
    df.to_excel(buf, index=False, engine="openpyxl")
    return buf.getvalue()


def _make_vendas_csv_bytes() -> bytes:
    """Gera CSV sintetico de vendas diesel (formato dados abertos ANP)."""
    rows = [
        {
            "ANO": "2024",
            "MES": "JAN",
            "GRANDE REGIAO": "REGIAO CENTRO-OESTE",
            "UNIDADE DA FEDERACAO": "MATO GROSSO",
            "PRODUTO": "OLEO DIESEL",
            "VENDAS": "500000",
        },
        {
            "ANO": "2024",
            "MES": "JAN",
            "GRANDE REGIAO": "REGIAO SUDESTE",
            "UNIDADE DA FEDERACAO": "SAO PAULO",
            "PRODUTO": "OLEO DIESEL",
            "VENDAS": "800000",
        },
        {
            "ANO": "2024",
            "MES": "FEV",
            "GRANDE REGIAO": "REGIAO CENTRO-OESTE",
            "UNIDADE DA FEDERACAO": "MATO GROSSO",
            "PRODUTO": "OLEO DIESEL S-10",
            "VENDAS": "520000",
        },
    ]
    df = pd.DataFrame(rows)
    return df.to_csv(index=False, sep=";").encode("utf-8")


class TestPrecosDiesel:
    @pytest.mark.asyncio
    async def test_basico_nivel_uf(self):
        xlsx = _make_precos_xlsx_bytes()
        with patch.object(api.client, "fetch_precos_estados", new_callable=AsyncMock) as mock:
            mock.return_value = xlsx
            df = await api.precos_diesel(nivel="uf")
            assert not df.empty
            assert "preco_venda" in df.columns

    @pytest.mark.asyncio
    async def test_basico_nivel_brasil(self):
        xlsx = _make_precos_xlsx_bytes()
        with patch.object(api.client, "fetch_precos_brasil", new_callable=AsyncMock) as mock:
            mock.return_value = xlsx
            df = await api.precos_diesel(nivel="brasil")
            assert not df.empty

    @pytest.mark.asyncio
    async def test_basico_nivel_municipio(self):
        xlsx = _make_precos_xlsx_bytes()
        with patch.object(api.client, "fetch_precos_municipios", new_callable=AsyncMock) as mock:
            mock.return_value = xlsx
            df = await api.precos_diesel(nivel="municipio", inicio="2024-01-01", fim="2024-12-31")
            assert not df.empty

    @pytest.mark.asyncio
    async def test_filtro_uf(self):
        xlsx = _make_precos_xlsx_bytes()
        with patch.object(api.client, "fetch_precos_estados", new_callable=AsyncMock) as mock:
            mock.return_value = xlsx
            df = await api.precos_diesel(uf="MT", nivel="uf")
            assert all(df["uf"] == "MT")

    @pytest.mark.asyncio
    async def test_filtro_produto(self):
        xlsx = _make_precos_xlsx_bytes()
        with patch.object(api.client, "fetch_precos_estados", new_callable=AsyncMock) as mock:
            mock.return_value = xlsx
            df = await api.precos_diesel(produto="DIESEL", nivel="uf")
            assert all(df["produto"] == "DIESEL")

    @pytest.mark.asyncio
    async def test_filtro_data_inicio_fim(self):
        xlsx = _make_precos_xlsx_bytes()
        with patch.object(api.client, "fetch_precos_estados", new_callable=AsyncMock) as mock:
            mock.return_value = xlsx
            df = await api.precos_diesel(
                nivel="uf",
                inicio="2024-01-20",
                fim="2024-01-31",
            )
            assert all(df["data"] >= pd.Timestamp("2024-01-20"))
            assert all(df["data"] <= pd.Timestamp("2024-01-31"))

    @pytest.mark.asyncio
    async def test_agregacao_mensal(self):
        xlsx = _make_precos_xlsx_bytes()
        with patch.object(api.client, "fetch_precos_estados", new_callable=AsyncMock) as mock:
            mock.return_value = xlsx
            df_semanal = await api.precos_diesel(nivel="uf", agregacao="semanal")
            df_mensal = await api.precos_diesel(nivel="uf", agregacao="mensal")
            assert len(df_mensal) <= len(df_semanal)

    @pytest.mark.asyncio
    async def test_return_meta(self):
        xlsx = _make_precos_xlsx_bytes()
        with patch.object(api.client, "fetch_precos_estados", new_callable=AsyncMock) as mock:
            mock.return_value = xlsx
            result = await api.precos_diesel(nivel="uf", return_meta=True)
            assert isinstance(result, tuple)
            df, meta = result
            assert isinstance(df, pd.DataFrame)
            assert isinstance(meta, MetaInfo)
            assert meta.source == "anp_diesel"
            assert meta.records_count == len(df)

    @pytest.mark.asyncio
    async def test_nivel_invalido(self):
        with pytest.raises(ValueError, match="invalido"):
            await api.precos_diesel(nivel="bairro")

    @pytest.mark.asyncio
    async def test_agregacao_invalida(self):
        with pytest.raises(ValueError, match="invalida"):
            await api.precos_diesel(agregacao="diario")

    @pytest.mark.asyncio
    async def test_produto_invalido(self):
        with pytest.raises(ValueError, match="invalido"):
            await api.precos_diesel(produto="GASOLINA")

    @pytest.mark.asyncio
    async def test_uf_invalida(self):
        with pytest.raises(ValueError, match="invalida"):
            await api.precos_diesel(uf="XX")

    @pytest.mark.asyncio
    async def test_inicio_string_iso(self):
        xlsx = _make_precos_xlsx_bytes()
        with patch.object(api.client, "fetch_precos_estados", new_callable=AsyncMock) as mock:
            mock.return_value = xlsx
            df = await api.precos_diesel(nivel="uf", inicio="2024-01-01")
            assert not df.empty

    @pytest.mark.asyncio
    async def test_inicio_date_object(self):
        xlsx = _make_precos_xlsx_bytes()
        with patch.object(api.client, "fetch_precos_estados", new_callable=AsyncMock) as mock:
            mock.return_value = xlsx
            df = await api.precos_diesel(nivel="uf", inicio=date(2024, 1, 1))
            assert not df.empty


class TestVendasDiesel:
    @pytest.mark.asyncio
    async def test_basico(self):
        csv_bytes = _make_vendas_csv_bytes()
        with patch.object(api.client, "fetch_vendas_m3", new_callable=AsyncMock) as mock:
            mock.return_value = csv_bytes
            df = await api.vendas_diesel()
            assert not df.empty
            assert "volume_m3" in df.columns

    @pytest.mark.asyncio
    async def test_filtro_uf(self):
        csv_bytes = _make_vendas_csv_bytes()
        with patch.object(api.client, "fetch_vendas_m3", new_callable=AsyncMock) as mock:
            mock.return_value = csv_bytes
            df = await api.vendas_diesel(uf="MT")
            assert all(df["uf"] == "MT")

    @pytest.mark.asyncio
    async def test_filtro_data(self):
        csv_bytes = _make_vendas_csv_bytes()
        with patch.object(api.client, "fetch_vendas_m3", new_callable=AsyncMock) as mock:
            mock.return_value = csv_bytes
            df = await api.vendas_diesel(
                inicio="2024-02-01",
                fim="2024-12-31",
            )
            assert all(df["data"] >= pd.Timestamp("2024-02-01"))

    @pytest.mark.asyncio
    async def test_return_meta(self):
        csv_bytes = _make_vendas_csv_bytes()
        with patch.object(api.client, "fetch_vendas_m3", new_callable=AsyncMock) as mock:
            mock.return_value = csv_bytes
            result = await api.vendas_diesel(return_meta=True)
            assert isinstance(result, tuple)
            df, meta = result
            assert isinstance(meta, MetaInfo)
            assert meta.source == "anp_diesel"

    @pytest.mark.asyncio
    async def test_uf_invalida(self):
        with pytest.raises(ValueError, match="invalida"):
            await api.vendas_diesel(uf="XX")

    @pytest.mark.asyncio
    async def test_inicio_string_iso(self):
        csv_bytes = _make_vendas_csv_bytes()
        with patch.object(api.client, "fetch_vendas_m3", new_callable=AsyncMock) as mock:
            mock.return_value = csv_bytes
            df = await api.vendas_diesel(inicio="2024-01-01")
            assert not df.empty


class TestSyncWrapper:
    def test_sync_anp_diesel_accessible(self):
        from agrobr import sync

        alt = sync.alt
        assert hasattr(alt, "anp_diesel")

    def test_sync_has_precos_vendas(self):
        from agrobr import sync

        ad = sync.alt.anp_diesel
        assert hasattr(ad, "precos_diesel")
        assert hasattr(ad, "vendas_diesel")
