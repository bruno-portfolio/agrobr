"""Testes para agrobr.antaq.api."""

from __future__ import annotations

import io
import zipfile
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from agrobr.antaq import api


def _make_zip(files: dict[str, str]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, content in files.items():
            zf.writestr(name, content.encode("utf-8-sig"))
    return buf.getvalue()


ATRACACAO_TXT = (
    "IDAtracacao;Porto Atracação;Complexo Portuário;Tipo da Autoridade Portuária;"
    "Data Atracação;Data Desatracação;Ano;Mes;"
    "Tipo de Navegação da Atracação;Terminal;Município;UF;SGUF;Região Geográfica\n"
    "1;Santos;Complexo Santos;AP;01/01/2024;02/01/2024;"
    "2024;1;Longo Curso;T1;Santos;SP;SP;Sudeste\n"
    "2;Paranaguá;Complexo Paranaguá;AP;15/01/2024;16/01/2024;"
    "2024;1;Cabotagem;T2;Paranaguá;PR;PR;Sul\n"
    "3;Salvador;Complexo Salvador;AP;20/02/2024;21/02/2024;"
    "2024;2;Longo Curso;T3;Salvador;BA;BA;Nordeste\n"
)

CARGA_TXT = (
    "IDCarga;IDAtracacao;Origem;Destino;CDMercadoria;Tipo Operação da Carga;"
    "Tipo Navegação;Natureza da Carga;Sentido;TEU;QTCarga;VLPesoCargaBruta\n"
    '1;1;Brasil;China;1201;Exportação;Longo Curso;Granel Sólido;Embarcados;0;1;"34.452,28"\n'
    '2;1;Brasil;EUA;1507;Exportação;Longo Curso;Granel Líquido e Gasoso;Embarcados;0;1;"12.100,50"\n'
    '3;2;Bahia;São Paulo;2304;Cabotagem;Cabotagem;Granel Sólido;Desembarcados;0;1;"5.000,00"\n'
    '4;3;São Paulo;Bahia;1005;Exportação;Longo Curso;Carga Geral;Embarcados;2;1;"800,75"\n'
)

MERCADORIA_TXT = (
    "CDMercadoria;Grupo de Mercadoria;Mercadoria;Nomenclatura Simplificada Mercadoria\n"
    "1201;Grãos;Soja em grãos;SOJA EM GRAOS\n"
    "1507;Óleos;Óleo de soja;OLEO DE SOJA\n"
    "2304;Farelos;Farelo de soja;FARELO DE SOJA\n"
    "1005;Grãos;Milho;MILHO\n"
)

ANO_ZIP = _make_zip(
    {
        "2024Atracacao.txt": ATRACACAO_TXT,
        "2024Carga.txt": CARGA_TXT,
    }
)

MERC_ZIP = _make_zip(
    {
        "Mercadoria.txt": MERCADORIA_TXT,
    }
)


def _patch_client():
    return (
        patch.object(api.client, "fetch_ano_zip", new_callable=AsyncMock, return_value=ANO_ZIP),
        patch.object(
            api.client, "fetch_mercadoria_zip", new_callable=AsyncMock, return_value=MERC_ZIP
        ),
    )


class TestMovimentacao:
    @pytest.mark.asyncio
    async def test_returns_dataframe(self):
        p1, p2 = _patch_client()
        with p1, p2:
            df = await api.movimentacao(2024)

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 4
        assert "porto" in df.columns
        assert "peso_bruto_ton" in df.columns

    @pytest.mark.asyncio
    async def test_filter_tipo_navegacao(self):
        p1, p2 = _patch_client()
        with p1, p2:
            df = await api.movimentacao(2024, tipo_navegacao="cabotagem")

        assert len(df) == 1
        assert (df["tipo_navegacao"] == "Cabotagem").all()

    @pytest.mark.asyncio
    async def test_filter_natureza_carga(self):
        p1, p2 = _patch_client()
        with p1, p2:
            df = await api.movimentacao(2024, natureza_carga="granel_solido")

        assert len(df) == 2
        assert (df["natureza_carga"] == "Granel Sólido").all()

    @pytest.mark.asyncio
    async def test_filter_mercadoria(self):
        p1, p2 = _patch_client()
        with p1, p2:
            df = await api.movimentacao(2024, mercadoria="SOJA")

        assert len(df) >= 1
        assert df["mercadoria"].str.contains("SOJA", case=False, na=False).all()

    @pytest.mark.asyncio
    async def test_filter_porto(self):
        p1, p2 = _patch_client()
        with p1, p2:
            df = await api.movimentacao(2024, porto="Santos")

        assert len(df) >= 1
        assert df["porto"].str.contains("Santos", case=False, na=False).all()

    @pytest.mark.asyncio
    async def test_filter_uf(self):
        p1, p2 = _patch_client()
        with p1, p2:
            df = await api.movimentacao(2024, uf="SP")

        assert len(df) >= 1
        assert (df["uf"].str.upper() == "SP").all()

    @pytest.mark.asyncio
    async def test_filter_sentido_embarque(self):
        p1, p2 = _patch_client()
        with p1, p2:
            df = await api.movimentacao(2024, sentido="embarque")

        assert len(df) >= 1
        assert (df["sentido"] == "Embarcados").all()

    @pytest.mark.asyncio
    async def test_filter_sentido_desembarque(self):
        p1, p2 = _patch_client()
        with p1, p2:
            df = await api.movimentacao(2024, sentido="desembarque")

        assert len(df) == 1
        assert (df["sentido"] == "Desembarcados").all()

    @pytest.mark.asyncio
    async def test_multiple_filters(self):
        p1, p2 = _patch_client()
        with p1, p2:
            df = await api.movimentacao(
                2024,
                tipo_navegacao="longo_curso",
                natureza_carga="granel_solido",
                sentido="embarque",
            )

        assert len(df) == 1
        assert df.iloc[0]["porto"] == "Santos"

    @pytest.mark.asyncio
    async def test_return_meta(self):
        p1, p2 = _patch_client()
        with p1, p2:
            df, meta = await api.movimentacao(2024, return_meta=True)

        assert meta.source == "antaq"
        assert meta.source_url == "https://web3.antaq.gov.br/ea/txt/2024.zip"
        assert meta.source_method == "httpx"
        assert meta.attempted_sources == ["antaq_ea"]
        assert meta.selected_source == "antaq_ea"
        assert meta.records_count == len(df)
        assert meta.parser_version == 1
        assert meta.fetch_timestamp is not None

    @pytest.mark.asyncio
    async def test_ano_below_min_raises(self):
        with pytest.raises(ValueError, match="2010"):
            await api.movimentacao(2009)

    @pytest.mark.asyncio
    async def test_ano_above_max_raises(self):
        with pytest.raises(ValueError, match="2025"):
            await api.movimentacao(2026)

    @pytest.mark.asyncio
    async def test_invalid_tipo_navegacao_raises(self):
        with pytest.raises(ValueError, match="Tipo de navegação desconhecido"):
            await api.movimentacao(2024, tipo_navegacao="invalido")

    @pytest.mark.asyncio
    async def test_invalid_natureza_carga_raises(self):
        with pytest.raises(ValueError, match="Natureza da carga desconhecida"):
            await api.movimentacao(2024, natureza_carga="invalido")

    @pytest.mark.asyncio
    async def test_none_filters_return_all(self):
        p1, p2 = _patch_client()
        with p1, p2:
            df = await api.movimentacao(
                2024,
                tipo_navegacao=None,
                natureza_carga=None,
                mercadoria=None,
                porto=None,
                uf=None,
                sentido=None,
            )

        assert len(df) == 4

    @pytest.mark.asyncio
    async def test_columns_present(self):
        p1, p2 = _patch_client()
        with p1, p2:
            df = await api.movimentacao(2024)

        expected_cols = [
            "ano",
            "mes",
            "data_atracacao",
            "tipo_navegacao",
            "natureza_carga",
            "sentido",
            "porto",
            "uf",
            "cd_mercadoria",
            "mercadoria",
            "peso_bruto_ton",
        ]
        for col in expected_cols:
            assert col in df.columns, f"Missing column: {col}"
