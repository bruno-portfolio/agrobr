"""Testes para agrobr.alt.anp_diesel.models."""

from __future__ import annotations

import pytest

from agrobr.alt.anp_diesel.models import (
    AGREGACOES_VALIDAS,
    MENSAL_BRASIL_URL,
    MENSAL_ESTADOS_URL,
    NIVEIS_VALIDOS,
    NIVEL_BRASIL,
    NIVEL_MUNICIPIO,
    NIVEL_UF,
    PRECOS_BRASIL_URL,
    PRECOS_ESTADOS_URL,
    PRECOS_MUNICIPIOS_URLS,
    PRODUTOS_DIESEL,
    SHLP_BASE,
    UFS_VALIDAS,
    VENDAS_DIESEL_CSV_URL,
    PrecoDiesel,
    VendaDiesel,
    _resolve_periodo_municipio,
)


class TestConstantes:
    def test_shlp_base_url_valida(self):
        assert SHLP_BASE.startswith("https://www.gov.br/anp")
        assert "shlp" in SHLP_BASE

    def test_vendas_diesel_csv_url_valida(self):
        assert VENDAS_DIESEL_CSV_URL.endswith(".csv")
        assert "vendas-oleo-diesel" in VENDAS_DIESEL_CSV_URL

    def test_precos_municipios_urls_nao_vazio(self):
        assert len(PRECOS_MUNICIPIOS_URLS) >= 3

    def test_precos_municipios_urls_formato(self):
        for periodo, url in PRECOS_MUNICIPIOS_URLS.items():
            assert url.startswith("https://")
            assert ".xlsx" in url or ".xls" in url
            parts = periodo.split("-")
            assert len(parts) in (1, 2)
            for p in parts:
                assert p.isdigit()
                assert int(p) >= 2013

    def test_precos_estados_url_xlsx(self):
        assert PRECOS_ESTADOS_URL.endswith(".xlsx")

    def test_precos_brasil_url_xlsx(self):
        assert PRECOS_BRASIL_URL.endswith(".xlsx")

    def test_mensal_urls_xlsx(self):
        assert MENSAL_ESTADOS_URL.endswith(".xlsx")
        assert MENSAL_BRASIL_URL.endswith(".xlsx")

    def test_produtos_diesel_contem_s10(self):
        assert "DIESEL S10" in PRODUTOS_DIESEL
        assert "DIESEL" in PRODUTOS_DIESEL

    def test_produtos_diesel_frozen(self):
        with pytest.raises(AttributeError):
            PRODUTOS_DIESEL.add("GASOLINA")

    def test_niveis_validos(self):
        assert NIVEL_MUNICIPIO in NIVEIS_VALIDOS
        assert NIVEL_UF in NIVEIS_VALIDOS
        assert NIVEL_BRASIL in NIVEIS_VALIDOS

    def test_agregacoes_validas(self):
        assert "semanal" in AGREGACOES_VALIDAS
        assert "mensal" in AGREGACOES_VALIDAS

    def test_ufs_27_estados(self):
        assert len(UFS_VALIDAS) == 27
        assert "SP" in UFS_VALIDAS
        assert "MT" in UFS_VALIDAS
        assert "DF" in UFS_VALIDAS


class TestResolvePeriodoMunicipio:
    def test_ano_2022(self):
        assert _resolve_periodo_municipio(2022) == "2022-2023"

    def test_ano_2023(self):
        assert _resolve_periodo_municipio(2023) == "2022-2023"

    def test_ano_2024(self):
        assert _resolve_periodo_municipio(2024) == "2024-2025"

    def test_ano_2025(self):
        assert _resolve_periodo_municipio(2025) == "2024-2025"

    def test_ano_2026(self):
        assert _resolve_periodo_municipio(2026) == "2026"

    def test_ano_antigo_retorna_none(self):
        assert _resolve_periodo_municipio(2010) is None

    def test_ano_futuro_retorna_none(self):
        assert _resolve_periodo_municipio(2030) is None


class TestPrecoDiesel:
    def test_criacao_basica(self):
        p = PrecoDiesel(
            data="2024-01-15",
            uf="SP",
            municipio="SAO PAULO",
            produto="DIESEL S10",
            preco_venda=6.45,
            preco_compra=5.80,
            margem=0.65,
            n_postos=150,
        )
        assert p.preco_venda == 6.45
        assert p.n_postos == 150

    def test_sentinel_vazio_preco(self):
        p = PrecoDiesel(
            data="2024-01-15",
            produto="DIESEL S10",
            preco_venda="",
            preco_compra="-",
        )
        assert p.preco_venda is None
        assert p.preco_compra is None

    def test_sentinel_vazio_n_postos(self):
        p = PrecoDiesel(
            data="2024-01-15",
            produto="DIESEL",
            n_postos="",
        )
        assert p.n_postos is None

    def test_conversao_string_para_float(self):
        p = PrecoDiesel(
            data="2024-01-15",
            produto="DIESEL S10",
            preco_venda="6.45",
        )
        assert p.preco_venda == 6.45

    def test_conversao_n_postos_float_para_int(self):
        p = PrecoDiesel(
            data="2024-01-15",
            produto="DIESEL",
            n_postos="150.0",
        )
        assert p.n_postos == 150

    def test_valor_invalido_retorna_none(self):
        p = PrecoDiesel(
            data="2024-01-15",
            produto="DIESEL",
            preco_venda="abc",
            n_postos="xyz",
        )
        assert p.preco_venda is None
        assert p.n_postos is None


class TestVendaDiesel:
    def test_criacao_basica(self):
        v = VendaDiesel(
            data="2024-01-01",
            uf="MT",
            regiao="CENTRO-OESTE",
            produto="ÓLEO DIESEL",
            volume_m3=500000.0,
        )
        assert v.volume_m3 == 500000.0
        assert v.uf == "MT"

    def test_sentinel_vazio_volume(self):
        v = VendaDiesel(
            data="2024-01-01",
            produto="DIESEL",
            volume_m3="",
        )
        assert v.volume_m3 is None

    def test_sentinel_traco_volume(self):
        v = VendaDiesel(
            data="2024-01-01",
            produto="DIESEL",
            volume_m3="-",
        )
        assert v.volume_m3 is None

    def test_conversao_string_volume(self):
        v = VendaDiesel(
            data="2024-01-01",
            produto="DIESEL",
            volume_m3="123456.78",
        )
        assert v.volume_m3 == 123456.78

    def test_valor_invalido_retorna_none(self):
        v = VendaDiesel(
            data="2024-01-01",
            produto="DIESEL",
            volume_m3="N/A",
        )
        assert v.volume_m3 is None
