"""Testes para os modelos BCB/SICOR."""


from agrobr.bcb.models import (
    SICOR_PRODUTOS,
    UF_CODES,
    CreditoRural,
    normalize_safra_sicor,
    resolve_produto_sicor,
)


class TestCreditoRural:
    def test_basic_creation(self):
        cr = CreditoRural(
            safra="2023/2024",
            uf="MT",
            municipio="SORRISO",
            cd_municipio="5107248",
            produto="SOJA",
            finalidade="custeio",
            valor=285431200.0,
            area_financiada=98500.0,
            qtd_contratos=1240,
        )

        assert cr.uf == "MT"
        assert cr.produto == "soja"
        assert cr.finalidade == "custeio"
        assert cr.valor == 285431200.0
        assert cr.area_financiada == 98500.0

    def test_normalization(self):
        cr = CreditoRural(
            safra="2023/2024",
            uf="mt",
            produto="  SOJA  ",
            finalidade="  CUSTEIO  ",
            valor=100.0,
        )

        assert cr.uf == "MT"
        assert cr.produto == "soja"
        assert cr.finalidade == "custeio"

    def test_optional_fields(self):
        cr = CreditoRural(
            safra="2023/2024",
            produto="soja",
            valor=100.0,
        )

        assert cr.uf is None
        assert cr.municipio is None
        assert cr.cd_municipio is None
        assert cr.area_financiada is None
        assert cr.qtd_contratos is None


class TestNormalizeSafraSicor:
    def test_full_format(self):
        assert normalize_safra_sicor("2023/2024") == "2023/2024"

    def test_short_format(self):
        assert normalize_safra_sicor("2023/24") == "2023/2024"

    def test_year_only(self):
        assert normalize_safra_sicor("2024") == "2023/2024"

    def test_strip(self):
        assert normalize_safra_sicor("  2023/24  ") == "2023/2024"

    def test_century_boundary(self):
        assert normalize_safra_sicor("2099/00") == "2099/2100"


class TestResolveProdutoSicor:
    def test_known_products(self):
        assert resolve_produto_sicor("soja") == "SOJA"
        assert resolve_produto_sicor("milho") == "MILHO"
        assert resolve_produto_sicor("algodao") == "ALGODAO HERBACEO"
        assert resolve_produto_sicor("cafe") == "CAFE"

    def test_unknown_product(self):
        assert resolve_produto_sicor("quinoa") == "QUINOA"

    def test_case_insensitive(self):
        assert resolve_produto_sicor("SOJA") == "SOJA"
        assert resolve_produto_sicor("Soja") == "SOJA"


class TestUfCodes:
    def test_main_states(self):
        assert UF_CODES["MT"] == "51"
        assert UF_CODES["SP"] == "35"
        assert UF_CODES["PR"] == "41"
        assert UF_CODES["GO"] == "52"

    def test_all_states(self):
        assert len(UF_CODES) == 27

    def test_sicor_produtos_completeness(self):
        assert len(SICOR_PRODUTOS) >= 10
