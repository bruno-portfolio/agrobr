"""Testes para agrobr.alt.mapa_psr.api."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from agrobr.alt.mapa_psr import api, parser
from agrobr.models import MetaInfo
from tests.helpers import binary_stream


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


@pytest.fixture(autouse=True)
def single_period(monkeypatch):
    monkeypatch.setattr(api, "_resolve_periodos", lambda *_: ["2016-2024"])


class TestSinistros:
    @pytest.mark.asyncio
    @patch.object(api.client, "open_periodo")
    async def test_filtro_evento(self, mock_fetch):
        mock_fetch.side_effect = lambda _: binary_stream(_make_csv_bytes())
        df = await api.sinistros(evento="seca")
        assert len(df) >= 1
        assert all("seca" in e for e in df["evento"])


class TestApolices:
    @pytest.mark.asyncio
    @patch.object(api.client, "open_periodo")
    async def test_return_meta(self, mock_fetch):
        mock_fetch.side_effect = lambda _: binary_stream(_make_csv_bytes())
        result = await api.apolices(return_meta=True)
        assert isinstance(result, tuple)
        df, meta = result
        assert isinstance(meta, MetaInfo)
        assert meta.source == "mapa_psr"
        assert meta.parser_version == parser.PARSER_VERSION
        assert "ano_apolice" in meta.columns

    @pytest.mark.asyncio
    @patch.object(api.client, "open_periodo")
    async def test_meta_fields_corretos(self, mock_fetch):
        mock_fetch.side_effect = lambda _: binary_stream(_make_csv_bytes())
        _, meta = await api.apolices(return_meta=True)
        assert meta.source_method == "httpx"
        assert meta.schema_version == "1.2"
        assert meta.attempted_sources == ["mapa_psr"]
        assert meta.selected_source == "mapa_psr"
        assert meta.fetch_duration_ms >= 0

    @pytest.mark.asyncio
    @patch.object(api.client, "open_periodo")
    async def test_uf_com_espaco_e_minuscula_filtra_pela_sigla(self, mock_fetch):
        mock_fetch.side_effect = lambda _: binary_stream(_make_csv_bytes())
        df = await api.apolices(uf=" mt ")
        assert df["nr_apolice"].tolist() == ["AP001"]
        assert df["uf"].tolist() == ["MT"]
