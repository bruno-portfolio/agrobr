import pytest

from agrobr.comtrade.models import (
    COLUNAS_MIRROR,
    COLUNAS_SAIDA,
    COMTRADE_PAISES,
    COMTRADE_PAISES_INV,
    HS_PRODUTOS_AGRO,
    resolve_hs,
    resolve_pais,
)


class TestResolvePais:
    def test_brasil_variants(self):
        assert resolve_pais("BR") == 76
        assert resolve_pais("BRA") == 76
        assert resolve_pais("brasil") == 76
        assert resolve_pais("Brazil") == 76

    def test_china_variants(self):
        assert resolve_pais("CN") == 156
        assert resolve_pais("CHN") == 156
        assert resolve_pais("china") == 156

    def test_usa_variants(self):
        assert resolve_pais("US") == 842
        assert resolve_pais("USA") == 842
        assert resolve_pais("eua") == 842

    def test_argentina(self):
        assert resolve_pais("AR") == 32
        assert resolve_pais("argentina") == 32

    def test_world(self):
        assert resolve_pais("world") == 0
        assert resolve_pais("mundo") == 0

    def test_numeric_code(self):
        assert resolve_pais("76") == 76
        assert resolve_pais("156") == 156

    def test_case_insensitive(self):
        assert resolve_pais("BRASIL") == 76
        assert resolve_pais("China") == 156

    def test_unknown_raises(self):
        with pytest.raises(ValueError, match="desconhecido"):
            resolve_pais("pais_inventado")

    def test_strip_whitespace(self):
        assert resolve_pais("  BR  ") == 76


class TestResolvePaisInv:
    def test_br_to_bra(self):
        assert COMTRADE_PAISES_INV[76] == "BRA"

    def test_cn_to_chn(self):
        assert COMTRADE_PAISES_INV[156] == "CHN"

    def test_world(self):
        assert COMTRADE_PAISES_INV[0] == "WLD"


class TestResolveHs:
    def test_soja(self):
        assert resolve_hs("soja") == ["1201"]

    def test_complexo_soja(self):
        assert resolve_hs("complexo_soja") == ["1201", "1507", "2304"]

    def test_carne_bovina(self):
        assert resolve_hs("carne_bovina") == ["0201", "0202"]

    def test_direct_code(self):
        assert resolve_hs("1201") == ["1201"]

    def test_unknown_raises(self):
        with pytest.raises(ValueError, match="desconhecido"):
            resolve_hs("produto_inventado")

    def test_case_insensitive(self):
        assert resolve_hs("SOJA") == ["1201"]
        assert resolve_hs("Milho") == ["1005"]

    def test_celulose(self):
        assert resolve_hs("celulose") == ["4703"]

    def test_suco_laranja(self):
        assert resolve_hs("suco_laranja") == ["2009"]


class TestConstants:
    def test_paises_has_main_partners(self):
        assert "br" in COMTRADE_PAISES
        assert "cn" in COMTRADE_PAISES
        assert "us" in COMTRADE_PAISES
        assert "ar" in COMTRADE_PAISES
        assert "eu" in COMTRADE_PAISES

    def test_hs_has_main_products(self):
        assert "soja" in HS_PRODUTOS_AGRO
        assert "milho" in HS_PRODUTOS_AGRO
        assert "cafe" in HS_PRODUTOS_AGRO
        assert "carne_bovina" in HS_PRODUTOS_AGRO

    def test_colunas_saida_defined(self):
        assert len(COLUNAS_SAIDA) > 10
        assert "periodo" in COLUNAS_SAIDA
        assert "reporter_iso" in COLUNAS_SAIDA
        assert "partner_iso" in COLUNAS_SAIDA
        assert "hs_code" in COLUNAS_SAIDA
        assert "valor_fob_usd" in COLUNAS_SAIDA
        assert "volume_ton" in COLUNAS_SAIDA

    def test_colunas_mirror_defined(self):
        assert len(COLUNAS_MIRROR) > 10
        assert "periodo" in COLUNAS_MIRROR
        assert "reporter_iso" in COLUNAS_MIRROR
        assert "partner_iso" in COLUNAS_MIRROR
        assert "diff_peso_kg" in COLUNAS_MIRROR
        assert "ratio_valor" in COLUNAS_MIRROR
        assert "ratio_peso" in COLUNAS_MIRROR
