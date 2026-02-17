"""Testes para agrobr.alt.mapa_psr.api."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from agrobr.alt.mapa_psr import api
from agrobr.models import MetaInfo


def _make_csv_bytes(
    rows: list[dict[str, str]] | None = None,
    sep: str = ";",
) -> bytes:
    """Gera CSV sintetico para mocks."""
    if rows is None:
        rows = [
            {
                "ANO_APOLICE": "2023",
                "NR_APOLICE": "AP001",
                "SG_UF_PROPRIEDADE": "MT",
                "NM_MUNICIPIO_PROPRIEDADE": "SORRISO",
                "CD_GEOCMU": "5107925",
                "NM_CULTURA_GLOBAL": "SOJA",
                "NM_CLASSIF_PRODUTO": "AGRICOLA",
                "NR_AREA_TOTAL": "500",
                "VL_PREMIO_LIQUIDO": "15000",
                "VL_SUBVENCAO_FEDERAL": "6000",
                "VL_LIMITE_GARANTIA": "250000",
                "VALOR_INDENIZACAO": "120000",
                "EVENTO_PREPONDERANTE": "SECA",
                "NR_PRODUTIVIDADE_ESTIMADA": "60",
                "NR_PRODUTIVIDADE_SEGURADA": "48",
                "NivelDeCobertura": "80",
                "PE_TAXA": "7.5",
                "NM_RAZAO_SOCIAL": "Seguradora ABC",
            },
            {
                "ANO_APOLICE": "2023",
                "NR_APOLICE": "AP002",
                "SG_UF_PROPRIEDADE": "PR",
                "NM_MUNICIPIO_PROPRIEDADE": "LONDRINA",
                "CD_GEOCMU": "4113700",
                "NM_CULTURA_GLOBAL": "MILHO",
                "NM_CLASSIF_PRODUTO": "AGRICOLA",
                "NR_AREA_TOTAL": "200",
                "VL_PREMIO_LIQUIDO": "8000",
                "VL_SUBVENCAO_FEDERAL": "3200",
                "VL_LIMITE_GARANTIA": "100000",
                "VALOR_INDENIZACAO": "0",
                "EVENTO_PREPONDERANTE": "",
                "NR_PRODUTIVIDADE_ESTIMADA": "120",
                "NR_PRODUTIVIDADE_SEGURADA": "96",
                "NivelDeCobertura": "80",
                "PE_TAXA": "6",
                "NM_RAZAO_SOCIAL": "Seguradora XYZ",
            },
            {
                "ANO_APOLICE": "2022",
                "NR_APOLICE": "AP003",
                "SG_UF_PROPRIEDADE": "GO",
                "NM_MUNICIPIO_PROPRIEDADE": "RIO VERDE",
                "CD_GEOCMU": "5218805",
                "NM_CULTURA_GLOBAL": "SOJA",
                "NM_CLASSIF_PRODUTO": "AGRICOLA",
                "NR_AREA_TOTAL": "1000",
                "VL_PREMIO_LIQUIDO": "30000",
                "VL_SUBVENCAO_FEDERAL": "12000",
                "VL_LIMITE_GARANTIA": "500000",
                "VALOR_INDENIZACAO": "350000",
                "EVENTO_PREPONDERANTE": "GEADA",
                "NR_PRODUTIVIDADE_ESTIMADA": "55",
                "NR_PRODUTIVIDADE_SEGURADA": "44",
                "NivelDeCobertura": "80",
                "PE_TAXA": "8",
                "NM_RAZAO_SOCIAL": "Seguradora ABC",
            },
        ]
    headers = list(rows[0].keys())
    lines = [sep.join(headers)]
    for row in rows:
        lines.append(sep.join(row.get(h, "") for h in headers))
    return "\n".join(lines).encode("utf-8")


class TestSinistros:
    @pytest.mark.asyncio
    @patch.object(api.client, "fetch_periodos", new_callable=AsyncMock)
    async def test_basico(self, mock_fetch):
        mock_fetch.return_value = [_make_csv_bytes()]
        df = await api.sinistros()
        assert not df.empty
        assert all(df["valor_indenizacao"] > 0)

    @pytest.mark.asyncio
    @patch.object(api.client, "fetch_periodos", new_callable=AsyncMock)
    async def test_filtro_cultura(self, mock_fetch):
        mock_fetch.return_value = [_make_csv_bytes()]
        df = await api.sinistros(cultura="SOJA")
        assert all(df["cultura"] == "SOJA")

    @pytest.mark.asyncio
    @patch.object(api.client, "fetch_periodos", new_callable=AsyncMock)
    async def test_filtro_uf(self, mock_fetch):
        mock_fetch.return_value = [_make_csv_bytes()]
        df = await api.sinistros(uf="MT")
        assert all(df["uf"] == "MT")

    @pytest.mark.asyncio
    @patch.object(api.client, "fetch_periodos", new_callable=AsyncMock)
    async def test_filtro_ano(self, mock_fetch):
        mock_fetch.return_value = [_make_csv_bytes()]
        df = await api.sinistros(ano=2023)
        assert all(df["ano_apolice"] == 2023)

    @pytest.mark.asyncio
    @patch.object(api.client, "fetch_periodos", new_callable=AsyncMock)
    async def test_filtro_range_ano(self, mock_fetch):
        mock_fetch.return_value = [_make_csv_bytes()]
        df = await api.sinistros(ano_inicio=2022, ano_fim=2023)
        assert all(df["ano_apolice"].between(2022, 2023))

    @pytest.mark.asyncio
    @patch.object(api.client, "fetch_periodos", new_callable=AsyncMock)
    async def test_filtro_evento(self, mock_fetch):
        mock_fetch.return_value = [_make_csv_bytes()]
        df = await api.sinistros(evento="seca")
        assert len(df) >= 1
        assert all("seca" in e for e in df["evento"])

    @pytest.mark.asyncio
    @patch.object(api.client, "fetch_periodos", new_callable=AsyncMock)
    async def test_return_meta(self, mock_fetch):
        mock_fetch.return_value = [_make_csv_bytes()]
        result = await api.sinistros(return_meta=True)
        assert isinstance(result, tuple)
        df, meta = result
        assert isinstance(meta, MetaInfo)
        assert meta.source == "mapa_psr"
        assert meta.records_count == len(df)

    @pytest.mark.asyncio
    async def test_uf_invalida_raise(self):
        with pytest.raises(ValueError, match="invalida"):
            await api.sinistros(uf="XX")

    @pytest.mark.asyncio
    async def test_ano_futuro_raise(self):
        with pytest.raises(ValueError):
            await api.sinistros(ano=2050)

    @pytest.mark.asyncio
    async def test_ano_anterior_psr_raise(self):
        with pytest.raises(ValueError):
            await api.sinistros(ano=2000)

    @pytest.mark.asyncio
    async def test_range_invertido_raise(self):
        with pytest.raises(ValueError, match="ano_inicio"):
            await api.sinistros(ano_inicio=2025, ano_fim=2020)

    @pytest.mark.asyncio
    @patch.object(api.client, "fetch_periodos", new_callable=AsyncMock)
    async def test_concat_multiplos_periodos(self, mock_fetch):
        csv1 = _make_csv_bytes()
        csv2 = _make_csv_bytes()
        mock_fetch.return_value = [csv1, csv2]
        df = await api.sinistros()
        assert len(df) >= 2

    @pytest.mark.asyncio
    @patch.object(api.client, "fetch_periodos", new_callable=AsyncMock)
    async def test_dataframe_vazio_sem_match(self, mock_fetch):
        mock_fetch.return_value = [_make_csv_bytes()]
        df = await api.sinistros(cultura="ALGODAO")
        assert df.empty


class TestApolices:
    @pytest.mark.asyncio
    @patch.object(api.client, "fetch_periodos", new_callable=AsyncMock)
    async def test_basico(self, mock_fetch):
        mock_fetch.return_value = [_make_csv_bytes()]
        df = await api.apolices()
        assert not df.empty
        assert len(df) == 3

    @pytest.mark.asyncio
    @patch.object(api.client, "fetch_periodos", new_callable=AsyncMock)
    async def test_filtro_cultura(self, mock_fetch):
        mock_fetch.return_value = [_make_csv_bytes()]
        df = await api.apolices(cultura="MILHO")
        assert all(df["cultura"] == "MILHO")

    @pytest.mark.asyncio
    @patch.object(api.client, "fetch_periodos", new_callable=AsyncMock)
    async def test_filtro_uf(self, mock_fetch):
        mock_fetch.return_value = [_make_csv_bytes()]
        df = await api.apolices(uf="PR")
        assert all(df["uf"] == "PR")

    @pytest.mark.asyncio
    @patch.object(api.client, "fetch_periodos", new_callable=AsyncMock)
    async def test_return_meta(self, mock_fetch):
        mock_fetch.return_value = [_make_csv_bytes()]
        result = await api.apolices(return_meta=True)
        assert isinstance(result, tuple)
        df, meta = result
        assert isinstance(meta, MetaInfo)
        assert meta.source == "mapa_psr"
        assert meta.parser_version == 1
        assert "ano_apolice" in meta.columns

    @pytest.mark.asyncio
    @patch.object(api.client, "fetch_periodos", new_callable=AsyncMock)
    async def test_tem_taxa(self, mock_fetch):
        mock_fetch.return_value = [_make_csv_bytes()]
        df = await api.apolices()
        assert "taxa" in df.columns

    @pytest.mark.asyncio
    @patch.object(api.client, "fetch_periodos", new_callable=AsyncMock)
    async def test_meta_fields_corretos(self, mock_fetch):
        mock_fetch.return_value = [_make_csv_bytes()]
        _, meta = await api.apolices(return_meta=True)
        assert meta.source_method == "httpx"
        assert meta.schema_version == "1.0"
        assert meta.attempted_sources == ["mapa_psr"]
        assert meta.selected_source == "mapa_psr"
        assert meta.fetch_duration_ms >= 0
