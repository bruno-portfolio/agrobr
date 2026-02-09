"""Testes para os modelos DERAL."""

from agrobr.deral.models import (
    CONDICOES,
    DERAL_PRODUTOS,
    CondicaoLavoura,
    normalize_condicao,
    normalize_produto,
)


class TestNormalizeProduto:
    def test_soja(self):
        assert normalize_produto("soja") == "soja"
        assert normalize_produto("Soja") == "soja"

    def test_milho(self):
        assert normalize_produto("milho") == "milho"

    def test_milho_safras(self):
        assert normalize_produto("milho 1ª safra") == "milho_1"
        assert normalize_produto("milho 2ª safra") == "milho_2"
        assert normalize_produto("milho safrinha") == "milho_2"
        assert normalize_produto("milho verão") == "milho_1"

    def test_trigo(self):
        assert normalize_produto("trigo") == "trigo"
        assert normalize_produto("Trigo") == "trigo"

    def test_feijao(self):
        assert normalize_produto("feijão") == "feijao"
        assert normalize_produto("feijao") == "feijao"

    def test_cana(self):
        assert normalize_produto("cana-de-açúcar") == "cana"
        assert normalize_produto("cana") == "cana"

    def test_cafe(self):
        assert normalize_produto("café") == "cafe"
        assert normalize_produto("cafe") == "cafe"

    def test_unknown_passthrough(self):
        assert normalize_produto("sorgo") == "sorgo"

    def test_whitespace_stripped(self):
        assert normalize_produto("  soja  ") == "soja"


class TestNormalizeCondicao:
    def test_boa(self):
        assert normalize_condicao("boa") == "boa"
        assert normalize_condicao("Boa") == "boa"
        assert normalize_condicao("bom") == "boa"

    def test_media(self):
        assert normalize_condicao("média") == "media"
        assert normalize_condicao("media") == "media"
        assert normalize_condicao("regular") == "media"

    def test_ruim(self):
        assert normalize_condicao("ruim") == "ruim"
        assert normalize_condicao("Ruim") == "ruim"
        assert normalize_condicao("má") == "ruim"

    def test_unknown_passthrough(self):
        assert normalize_condicao("excelente") == "excelente"


class TestCondicaoLavoura:
    def test_basic_creation(self):
        rec = CondicaoLavoura(
            produto="soja",
            data="15/01/2025",
            condicao="boa",
            pct=75.0,
        )
        assert rec.produto == "soja"
        assert rec.data == "15/01/2025"
        assert rec.condicao == "boa"
        assert rec.pct == 75.0

    def test_produto_normalization(self):
        rec = CondicaoLavoura(
            produto="  Soja  ",
            condicao="boa",
        )
        assert rec.produto == "soja"

    def test_condicao_normalization(self):
        rec = CondicaoLavoura(
            produto="soja",
            condicao="média",
        )
        assert rec.condicao == "media"

    def test_optional_fields(self):
        rec = CondicaoLavoura(produto="milho")
        assert rec.data == ""
        assert rec.estagio == ""
        assert rec.condicao == ""
        assert rec.pct is None
        assert rec.plantio_pct is None
        assert rec.colheita_pct is None

    def test_empty_condicao_stays_empty(self):
        rec = CondicaoLavoura(produto="soja", condicao="")
        assert rec.condicao == ""


class TestDeralProdutos:
    def test_main_products(self):
        assert "soja" in DERAL_PRODUTOS
        assert "milho" in DERAL_PRODUTOS
        assert "trigo" in DERAL_PRODUTOS
        assert "cafe" in DERAL_PRODUTOS

    def test_count(self):
        assert len(DERAL_PRODUTOS) >= 10


class TestCondicoes:
    def test_has_three(self):
        assert len(CONDICOES) == 3
        assert "boa" in CONDICOES
        assert "media" in CONDICOES
        assert "ruim" in CONDICOES
