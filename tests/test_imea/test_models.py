"""Testes para os modelos IMEA."""

import pytest

from agrobr.imea.models import (
    IMEA_CADEIAS,
    IMEA_COLUMNS_MAP,
    IMEA_MACRORREGIOES,
    CotacaoIMEA,
    cadeia_name,
    resolve_cadeia_id,
)


class TestResolveCadeiaId:
    def test_soja(self):
        assert resolve_cadeia_id("soja") == 4

    def test_milho(self):
        assert resolve_cadeia_id("milho") == 3

    def test_algodao(self):
        assert resolve_cadeia_id("algodao") == 1

    def test_bovinocultura(self):
        assert resolve_cadeia_id("bovinocultura") == 2

    def test_suinocultura(self):
        assert resolve_cadeia_id("suinocultura") == 7

    def test_leite(self):
        assert resolve_cadeia_id("leite") == 8

    def test_english_names(self):
        assert resolve_cadeia_id("soybeans") == 4
        assert resolve_cadeia_id("corn") == 3
        assert resolve_cadeia_id("cotton") == 1
        assert resolve_cadeia_id("cattle") == 2

    def test_case_insensitive(self):
        assert resolve_cadeia_id("Soja") == 4
        assert resolve_cadeia_id("MILHO") == 3
        assert resolve_cadeia_id("Algodao") == 1

    def test_whitespace_stripped(self):
        assert resolve_cadeia_id("  soja  ") == 4
        assert resolve_cadeia_id(" milho ") == 3

    def test_numeric_id_string(self):
        assert resolve_cadeia_id("4") == 4
        assert resolve_cadeia_id("3") == 3
        assert resolve_cadeia_id("1") == 1

    def test_unknown_raises(self):
        with pytest.raises(ValueError, match="Cadeia desconhecida"):
            resolve_cadeia_id("cafe")

    def test_invalid_numeric_raises(self):
        with pytest.raises(ValueError, match="Cadeia desconhecida"):
            resolve_cadeia_id("99")


class TestCadeiaName:
    def test_known_ids(self):
        assert cadeia_name(4) == "soja"
        assert cadeia_name(3) == "milho"
        assert cadeia_name(1) == "algodao"
        assert cadeia_name(2) == "bovinocultura"
        assert cadeia_name(7) == "suinocultura"
        assert cadeia_name(8) == "leite"

    def test_conjuntura(self):
        assert cadeia_name(5) == "conjuntura"

    def test_unknown_returns_str(self):
        assert cadeia_name(999) == "999"


class TestCotacaoIMEA:
    def test_basic_creation(self):
        rec = CotacaoIMEA(
            cadeia="soja",
            localidade="Médio-Norte",
            valor=125.50,
            variacao=-1.2,
            safra="24/25",
            unidade="R$/sc",
            data_publicacao="2024-06-15",
        )
        assert rec.cadeia == "soja"
        assert rec.localidade == "Médio-Norte"
        assert rec.valor == 125.50
        assert rec.variacao == -1.2
        assert rec.safra == "24/25"
        assert rec.unidade == "R$/sc"

    def test_cadeia_normalization(self):
        rec = CotacaoIMEA(
            cadeia="  SOJA  ",
            localidade="Norte",
        )
        assert rec.cadeia == "soja"

    def test_localidade_strip(self):
        rec = CotacaoIMEA(
            cadeia="milho",
            localidade="  Médio-Norte  ",
        )
        assert rec.localidade == "Médio-Norte"

    def test_optional_fields(self):
        rec = CotacaoIMEA(
            cadeia="soja",
            localidade="Norte",
        )
        assert rec.valor is None
        assert rec.variacao is None
        assert rec.safra == ""
        assert rec.unidade == ""
        assert rec.data_publicacao == ""


class TestImeaCadeias:
    def test_main_products(self):
        assert "soja" in IMEA_CADEIAS
        assert "milho" in IMEA_CADEIAS
        assert "algodao" in IMEA_CADEIAS
        assert "bovinocultura" in IMEA_CADEIAS

    def test_english_aliases(self):
        assert "soybeans" in IMEA_CADEIAS
        assert "corn" in IMEA_CADEIAS
        assert "cotton" in IMEA_CADEIAS
        assert "cattle" in IMEA_CADEIAS

    def test_soja_and_soybeans_same_id(self):
        assert IMEA_CADEIAS["soja"] == IMEA_CADEIAS["soybeans"]

    def test_milho_and_corn_same_id(self):
        assert IMEA_CADEIAS["milho"] == IMEA_CADEIAS["corn"]


class TestImeaMacrorregioes:
    def test_count(self):
        assert len(IMEA_MACRORREGIOES) == 7

    def test_major_regions(self):
        assert "Médio-Norte" in IMEA_MACRORREGIOES
        assert "Noroeste" in IMEA_MACRORREGIOES
        assert "Sudeste" in IMEA_MACRORREGIOES


class TestImeaColumnsMap:
    def test_main_columns(self):
        assert IMEA_COLUMNS_MAP["Localidade"] == "localidade"
        assert IMEA_COLUMNS_MAP["Valor"] == "valor"
        assert IMEA_COLUMNS_MAP["Variacao"] == "variacao"
        assert IMEA_COLUMNS_MAP["Safra"] == "safra"
        assert IMEA_COLUMNS_MAP["DataPublicacao"] == "data_publicacao"
        assert IMEA_COLUMNS_MAP["UnidadeSigla"] == "unidade"
        assert IMEA_COLUMNS_MAP["UnidadeDescricao"] == "unidade_descricao"
