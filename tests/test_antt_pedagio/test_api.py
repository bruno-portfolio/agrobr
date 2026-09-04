"""Testes para agrobr.alt.antt_pedagio.api."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import httpx
import pandas as pd
import pytest

from agrobr.alt.antt_pedagio.api import (
    _filtrar_fluxo,
    fluxo_pedagio,
    pracas_pedagio,
)
from agrobr.models import MetaInfo

# Sample CSV bytes for mocking
V1_CSV = (
    b"concessionaria;praca;mes_ano;categoria;tipo_cobranca;sentido;quantidade\n"
    b"CCR AutoBAn;Campinas;01/01/2023;Categoria 1;Automatica;Crescente;50000\n"
    b"CCR AutoBAn;Campinas;01/01/2023;Categoria 4;Automatica;Crescente;12000\n"
    b"CCR AutoBAn;Campinas;01/01/2023;Categoria 8;Automatica;Decrescente;8000\n"
    b"Arteris;Jacarezinho;01/01/2023;Categoria 1;Automatica;Crescente;20000\n"
    b"Arteris;Jacarezinho;01/01/2023;Categoria 7;Automatica;Crescente;5000\n"
)

PRACAS_CSV = (
    b"concessionaria;praca_de_pedagio;rodovia;uf;km_m;municipio;lat;lon;situacao\n"
    b"CCR AutoBAn;Campinas;SP-348;SP;87+500;Campinas;-22.9;-47.0;Ativa\n"
    b"Arteris;Jacarezinho;BR-153;PR;10+000;Jacarezinho;-23.1;-49.9;Ativa\n"
)


# ============================================================================
# fluxo_pedagio
# ============================================================================


class TestFluxoPedagio:
    def test_filter_concessionaria_literal(self):
        source = pd.DataFrame({"concessionaria": ["CCR (Sul)", "CCR Sul"], "n_eixos": [2, 2]})

        df = _filtrar_fluxo(
            source,
            concessionaria="CCR (Sul)",
            praca=None,
            rodovia=None,
            uf=None,
            tipo_veiculo=None,
            apenas_pesados=False,
        )

        assert df["concessionaria"].tolist() == ["CCR (Sul)"]

    @pytest.mark.asyncio
    async def test_basic_call(self):
        with patch("agrobr.alt.antt_pedagio.api.client") as mock_client:
            mock_client.fetch_trafego_anos = AsyncMock(return_value=[(2023, V1_CSV)])
            mock_client.fetch_pracas = AsyncMock(return_value=PRACAS_CSV)
            mock_client.DATASET_TRAFEGO_SLUG = "test-slug"

            df = await fluxo_pedagio(ano=2023)

        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0
        assert "data" in df.columns
        assert "volume" in df.columns

    @pytest.mark.asyncio
    async def test_filter_uf(self):
        with patch("agrobr.alt.antt_pedagio.api.client") as mock_client:
            mock_client.fetch_trafego_anos = AsyncMock(return_value=[(2023, V1_CSV)])
            mock_client.fetch_pracas = AsyncMock(return_value=PRACAS_CSV)
            mock_client.DATASET_TRAFEGO_SLUG = "test-slug"

            df = await fluxo_pedagio(ano=2023, uf="SP")

        if len(df) > 0 and "uf" in df.columns:
            assert (df["uf"].dropna() == "SP").all()

    @pytest.mark.asyncio
    async def test_filter_concessionaria(self):
        with patch("agrobr.alt.antt_pedagio.api.client") as mock_client:
            mock_client.fetch_trafego_anos = AsyncMock(return_value=[(2023, V1_CSV)])
            mock_client.fetch_pracas = AsyncMock(return_value=PRACAS_CSV)
            mock_client.DATASET_TRAFEGO_SLUG = "test-slug"

            df = await fluxo_pedagio(ano=2023, concessionaria="CCR")

        assert len(df) > 0
        assert all("CCR" in c for c in df["concessionaria"] if c)

    @pytest.mark.asyncio
    async def test_filter_praca(self):
        with patch("agrobr.alt.antt_pedagio.api.client") as mock_client:
            mock_client.fetch_trafego_anos = AsyncMock(return_value=[(2023, V1_CSV)])
            mock_client.fetch_pracas = AsyncMock(return_value=PRACAS_CSV)
            mock_client.DATASET_TRAFEGO_SLUG = "test-slug"

            df = await fluxo_pedagio(ano=2023, praca="Campinas")

        assert len(df) > 0
        assert all("Campinas" in p for p in df["praca"] if p)

    @pytest.mark.asyncio
    async def test_apenas_pesados(self):
        with patch("agrobr.alt.antt_pedagio.api.client") as mock_client:
            mock_client.fetch_trafego_anos = AsyncMock(return_value=[(2023, V1_CSV)])
            mock_client.fetch_pracas = AsyncMock(return_value=PRACAS_CSV)
            mock_client.DATASET_TRAFEGO_SLUG = "test-slug"

            df = await fluxo_pedagio(ano=2023, apenas_pesados=True)

        assert len(df) > 0
        assert (df["n_eixos"] >= 3).all()
        assert (df["tipo_veiculo"] == "Comercial").all()

    @pytest.mark.asyncio
    async def test_filter_tipo_veiculo(self):
        with patch("agrobr.alt.antt_pedagio.api.client") as mock_client:
            mock_client.fetch_trafego_anos = AsyncMock(return_value=[(2023, V1_CSV)])
            mock_client.fetch_pracas = AsyncMock(return_value=PRACAS_CSV)
            mock_client.DATASET_TRAFEGO_SLUG = "test-slug"

            df = await fluxo_pedagio(ano=2023, tipo_veiculo="Passeio")

        if len(df) > 0:
            assert (df["tipo_veiculo"] == "Passeio").all()

    @pytest.mark.asyncio
    async def test_return_meta(self):
        with patch("agrobr.alt.antt_pedagio.api.client") as mock_client:
            mock_client.fetch_trafego_anos = AsyncMock(return_value=[(2023, V1_CSV)])
            mock_client.fetch_pracas = AsyncMock(return_value=PRACAS_CSV)
            mock_client.DATASET_TRAFEGO_SLUG = "test-slug"

            result = await fluxo_pedagio(ano=2023, return_meta=True)

        assert isinstance(result, tuple)
        df, meta = result
        assert isinstance(df, pd.DataFrame)
        assert isinstance(meta, MetaInfo)
        assert meta.source == "antt_pedagio"
        assert meta.records_count == len(df)

    @pytest.mark.asyncio
    async def test_empty_result(self):
        with patch("agrobr.alt.antt_pedagio.api.client") as mock_client:
            mock_client.fetch_trafego_anos = AsyncMock(return_value=[])
            mock_client.fetch_pracas = AsyncMock(return_value=b"")
            mock_client.DATASET_TRAFEGO_SLUG = "test-slug"

            df = await fluxo_pedagio(ano=2023)

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0

    @pytest.mark.asyncio
    async def test_pracas_fetch_failure_graceful(self):
        with patch("agrobr.alt.antt_pedagio.api.client") as mock_client:
            mock_client.fetch_trafego_anos = AsyncMock(return_value=[(2023, V1_CSV)])
            mock_client.fetch_pracas = AsyncMock(side_effect=httpx.ConnectError("network error"))
            mock_client.DATASET_TRAFEGO_SLUG = "test-slug"

            df = await fluxo_pedagio(ano=2023)

        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0  # Should work even without pracas

    @pytest.mark.asyncio
    async def test_invalid_uf_raises(self):
        with pytest.raises(ValueError, match="UF"):
            await fluxo_pedagio(uf="XX")

    @pytest.mark.asyncio
    async def test_sorted_output(self):
        with patch("agrobr.alt.antt_pedagio.api.client") as mock_client:
            mock_client.fetch_trafego_anos = AsyncMock(return_value=[(2023, V1_CSV)])
            mock_client.fetch_pracas = AsyncMock(return_value=PRACAS_CSV)
            mock_client.DATASET_TRAFEGO_SLUG = "test-slug"

            df = await fluxo_pedagio(ano=2023)

        if len(df) > 1:
            dates = df["data"].tolist()
            assert dates == sorted(dates)


# ============================================================================
# pracas_pedagio
# ============================================================================


class TestPracasPedagio:
    @pytest.mark.asyncio
    async def test_basic_call(self):
        with patch("agrobr.alt.antt_pedagio.api.client") as mock_client:
            mock_client.fetch_pracas = AsyncMock(return_value=PRACAS_CSV)
            mock_client.DATASET_PRACAS_SLUG = "test-slug"

            df = await pracas_pedagio()

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2

    @pytest.mark.asyncio
    async def test_filter_uf(self):
        with patch("agrobr.alt.antt_pedagio.api.client") as mock_client:
            mock_client.fetch_pracas = AsyncMock(return_value=PRACAS_CSV)
            mock_client.DATASET_PRACAS_SLUG = "test-slug"

            df = await pracas_pedagio(uf="SP")

        assert len(df) == 1
        assert df.iloc[0]["uf"] == "SP"

    @pytest.mark.asyncio
    async def test_filter_rodovia(self):
        with patch("agrobr.alt.antt_pedagio.api.client") as mock_client:
            mock_client.fetch_pracas = AsyncMock(return_value=PRACAS_CSV)
            mock_client.DATASET_PRACAS_SLUG = "test-slug"

            df = await pracas_pedagio(rodovia="BR-153")

        assert len(df) == 1

    @pytest.mark.asyncio
    async def test_return_meta(self):
        with patch("agrobr.alt.antt_pedagio.api.client") as mock_client:
            mock_client.fetch_pracas = AsyncMock(return_value=PRACAS_CSV)
            mock_client.DATASET_PRACAS_SLUG = "test-slug"

            result = await pracas_pedagio(return_meta=True)

        assert isinstance(result, tuple)
        df, meta = result
        assert isinstance(meta, MetaInfo)
        assert meta.source == "antt_pedagio"

    @pytest.mark.asyncio
    async def test_invalid_uf_raises(self):
        with pytest.raises(ValueError, match="UF"):
            await pracas_pedagio(uf="XX")
