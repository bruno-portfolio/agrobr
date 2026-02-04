"""Testes do client IBGE."""

from __future__ import annotations

from agrobr.ibge import client


class TestProdutosMapping:
    """Testes dos mapeamentos de produtos."""

    def test_produtos_pam_contains_soja(self):
        """Testa que soja esta nos produtos PAM."""
        assert "soja" in client.PRODUTOS_PAM
        assert client.PRODUTOS_PAM["soja"] == "40124"

    def test_produtos_pam_contains_milho(self):
        """Testa que milho esta nos produtos PAM."""
        assert "milho" in client.PRODUTOS_PAM

    def test_produtos_pam_contains_main_crops(self):
        """Testa que principais culturas estao mapeadas."""
        expected = ["soja", "milho", "arroz", "feijao", "trigo", "algodao", "cafe", "cana"]
        for prod in expected:
            assert prod in client.PRODUTOS_PAM, f"{prod} not in PRODUTOS_PAM"

    def test_produtos_lspa_contains_soja(self):
        """Testa que soja esta nos produtos LSPA."""
        assert "soja" in client.PRODUTOS_LSPA
        assert client.PRODUTOS_LSPA["soja"] == "39443"

    def test_produtos_lspa_contains_safras_milho(self):
        """Testa que safras de milho estao no LSPA."""
        assert "milho_1" in client.PRODUTOS_LSPA
        assert "milho_2" in client.PRODUTOS_LSPA

    def test_produtos_lspa_contains_safras_feijao(self):
        """Testa que safras de feijao estao no LSPA."""
        assert "feijao_1" in client.PRODUTOS_LSPA
        assert "feijao_2" in client.PRODUTOS_LSPA
        assert "feijao_3" in client.PRODUTOS_LSPA


class TestVariaveis:
    """Testes dos mapeamentos de variaveis."""

    def test_variaveis_pam(self):
        """Testa variaveis principais da PAM."""
        assert "area_plantada" in client.VARIAVEIS
        assert "area_colhida" in client.VARIAVEIS
        assert "producao" in client.VARIAVEIS
        assert "rendimento" in client.VARIAVEIS

    def test_variaveis_codes(self):
        """Testa que codigos sao strings numericas."""
        for var, code in client.VARIAVEIS.items():
            assert code.isdigit(), f"{var}: {code} is not numeric"


class TestTabelas:
    """Testes dos mapeamentos de tabelas SIDRA."""

    def test_tabela_pam_nova(self):
        """Testa codigo da tabela PAM nova (5457)."""
        assert "pam_nova" in client.TABELAS
        assert client.TABELAS["pam_nova"] == "5457"

    def test_tabela_lspa(self):
        """Testa codigo da tabela LSPA (6588)."""
        assert "lspa" in client.TABELAS
        assert client.TABELAS["lspa"] == "6588"

    def test_tabela_pam_temporarias(self):
        """Testa codigo da tabela PAM temporarias."""
        assert "pam_temporarias" in client.TABELAS
        assert client.TABELAS["pam_temporarias"] == "1612"


class TestNiveisTerritoriais:
    """Testes dos niveis territoriais."""

    def test_nivel_brasil(self):
        """Testa codigo do nivel Brasil."""
        assert client.NIVEIS_TERRITORIAIS["brasil"] == "1"

    def test_nivel_uf(self):
        """Testa codigo do nivel UF."""
        assert client.NIVEIS_TERRITORIAIS["uf"] == "3"

    def test_nivel_municipio(self):
        """Testa codigo do nivel municipio."""
        assert client.NIVEIS_TERRITORIAIS["municipio"] == "6"


class TestUfCodes:
    """Testes das funcoes de codigo de UF."""

    def test_get_uf_codes(self):
        """Testa obtencao dos codigos de UF."""
        codes = client.get_uf_codes()
        assert len(codes) == 27  # 26 estados + DF

    def test_get_uf_codes_mt(self):
        """Testa codigo do MT."""
        codes = client.get_uf_codes()
        assert codes["MT"] == "51"

    def test_get_uf_codes_sp(self):
        """Testa codigo do SP."""
        codes = client.get_uf_codes()
        assert codes["SP"] == "35"

    def test_uf_to_ibge_code_uppercase(self):
        """Testa conversao de UF para codigo IBGE."""
        assert client.uf_to_ibge_code("MT") == "51"
        assert client.uf_to_ibge_code("PR") == "41"
        assert client.uf_to_ibge_code("RS") == "43"

    def test_uf_to_ibge_code_lowercase(self):
        """Testa conversao com lowercase."""
        assert client.uf_to_ibge_code("mt") == "51"
        assert client.uf_to_ibge_code("pr") == "41"

    def test_uf_to_ibge_code_invalid(self):
        """Testa que codigo invalido retorna o proprio input."""
        assert client.uf_to_ibge_code("XX") == "XX"
        assert client.uf_to_ibge_code("12345") == "12345"


class TestParseSidraResponse:
    """Testes do parser de resposta SIDRA."""

    def test_parse_rename_columns(self):
        """Testa renomeacao de colunas padrao."""
        import pandas as pd

        df = pd.DataFrame(
            {
                "NC": ["1"],
                "NN": ["Brasil"],
                "MC": ["1"],
                "MN": ["Brasil"],
                "V": ["100"],
                "D1N": ["2023"],
            }
        )

        result = client.parse_sidra_response(df)

        assert "nivel_territorial_cod" in result.columns
        assert "localidade" in result.columns
        assert "valor" in result.columns
        assert "ano" in result.columns

    def test_parse_converts_valor_to_numeric(self):
        """Testa conversao de valor para numerico."""
        import pandas as pd

        df = pd.DataFrame(
            {
                "V": ["100", "200.5", "300"],
            }
        )

        result = client.parse_sidra_response(df)

        assert result["valor"].dtype in ["float64", "int64"]
        assert result["valor"].iloc[0] == 100.0

    def test_parse_handles_invalid_values(self):
        """Testa que valores invalidos viram NaN."""
        import pandas as pd

        df = pd.DataFrame(
            {
                "V": ["100", "-", "..."],
            }
        )

        result = client.parse_sidra_response(df)

        assert result["valor"].iloc[0] == 100.0
        assert pd.isna(result["valor"].iloc[1])
        assert pd.isna(result["valor"].iloc[2])

    def test_parse_custom_rename(self):
        """Testa renomeacao customizada."""
        import pandas as pd

        df = pd.DataFrame(
            {
                "D5C": ["123"],
                "D5N": ["Custom"],
            }
        )

        result = client.parse_sidra_response(df, rename_columns={"D5N": "custom_field"})

        assert "custom_field" in result.columns
        assert result["custom_field"].iloc[0] == "Custom"
