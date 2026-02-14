from __future__ import annotations

from agrobr.conab.progresso.models import (
    BASE_URL,
    COLUNAS_SAIDA,
    CULTURAS_PROGRESSO,
    CULTURAS_VALIDAS,
    ESTADOS_PARA_UF,
    TIPOS_OPERACAO,
    estado_para_uf,
    normalizar_cultura,
    parse_cultura_header,
    parse_operacao_header,
)


class TestNormalizarCultura:
    def test_soja(self) -> None:
        assert normalizar_cultura("soja") == "Soja"

    def test_milho_2(self) -> None:
        assert normalizar_cultura("milho_2") == "Milho 2\u00aa"

    def test_algodao(self) -> None:
        assert normalizar_cultura("algodao") == "Algod\u00e3o"

    def test_feijao(self) -> None:
        assert normalizar_cultura("feijao") == "Feij\u00e3o 1\u00aa"

    def test_arroz(self) -> None:
        assert normalizar_cultura("arroz") == "Arroz"

    def test_case_insensitive(self) -> None:
        assert normalizar_cultura("SOJA") == "Soja"

    def test_unknown_passthrough(self) -> None:
        assert normalizar_cultura("Mandioca") == "Mandioca"

    def test_whitespace(self) -> None:
        assert normalizar_cultura("  soja  ") == "Soja"


class TestEstadoParaUf:
    def test_mato_grosso(self) -> None:
        assert estado_para_uf("Mato Grosso") == "MT"

    def test_parana(self) -> None:
        assert estado_para_uf("Paran\u00e1") == "PR"

    def test_goias(self) -> None:
        assert estado_para_uf("Goi\u00e1s") == "GO"

    def test_sao_paulo(self) -> None:
        assert estado_para_uf("S\u00e3o Paulo") == "SP"

    def test_trailing_newline(self) -> None:
        assert estado_para_uf("Mato Grosso\n") == "MT"

    def test_trailing_whitespace(self) -> None:
        assert estado_para_uf("  Mato Grosso  ") == "MT"

    def test_unknown_passthrough(self) -> None:
        assert estado_para_uf("XY") == "XY"

    def test_double_space(self) -> None:
        assert estado_para_uf("Mato  Grosso") == "MT"


class TestParseCulturaHeader:
    def test_soja(self) -> None:
        result = parse_cultura_header("Soja - Safra 2025/26")
        assert result == ("Soja", "2025/26")

    def test_algodao(self) -> None:
        result = parse_cultura_header("Algod\u00e3o - Safra 2025/26")
        assert result == ("Algod\u00e3o", "2025/26")

    def test_milho_2(self) -> None:
        result = parse_cultura_header("Milho 2\u00aa - Safra 2025/26")
        assert result == ("Milho 2\u00aa", "2025/26")

    def test_feijao_1(self) -> None:
        result = parse_cultura_header("Feij\u00e3o 1\u00aa - Safra 2025/26")
        assert result == ("Feij\u00e3o 1\u00aa", "2025/26")

    def test_not_cultura(self) -> None:
        assert parse_cultura_header("Estado") is None

    def test_empty(self) -> None:
        assert parse_cultura_header("") is None

    def test_semeadura(self) -> None:
        assert parse_cultura_header("Semeadura") is None


class TestParseOperacaoHeader:
    def test_semeadura(self) -> None:
        assert parse_operacao_header("Semeadura") == "Semeadura"

    def test_colheita(self) -> None:
        assert parse_operacao_header("Colheita") == "Colheita"

    def test_colheita_asterisk(self) -> None:
        assert parse_operacao_header("Colheita *") == "Colheita"

    def test_colheita_star(self) -> None:
        assert parse_operacao_header("Colheita*") == "Colheita"

    def test_not_operacao(self) -> None:
        assert parse_operacao_header("Estado") is None

    def test_empty(self) -> None:
        assert parse_operacao_header("") is None


class TestConstants:
    def test_culturas_validas_count(self) -> None:
        assert len(CULTURAS_VALIDAS) == 7

    def test_tipos_operacao(self) -> None:
        assert {"Semeadura", "Colheita"} == TIPOS_OPERACAO

    def test_estados_count(self) -> None:
        assert len(ESTADOS_PARA_UF) == 27

    def test_colunas_saida_count(self) -> None:
        assert len(COLUNAS_SAIDA) == 9

    def test_base_url(self) -> None:
        assert "progresso-de-safra" in BASE_URL

    def test_culturas_progresso_includes_keys(self) -> None:
        for key in ["soja", "milho_1", "milho_2", "arroz", "algodao"]:
            assert key in CULTURAS_PROGRESSO
