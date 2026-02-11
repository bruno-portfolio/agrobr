from __future__ import annotations

from datetime import date

import pytest

from agrobr.normalize.dates import (
    anos_para_safra,
    data_para_safra,
    lista_safras,
    mes_para_numero,
    normalizar_safra,
    numero_para_mes,
    periodo_safra,
    safra_anterior,
    safra_atual,
    safra_para_anos,
    safra_posterior,
    validar_safra,
)


class TestSafraAtual:
    def test_segundo_semestre(self):
        assert safra_atual(date(2024, 10, 15)) == "2024/25"

    def test_primeiro_semestre(self):
        assert safra_atual(date(2025, 3, 15)) == "2024/25"

    def test_julho_inicio_safra(self):
        assert safra_atual(date(2024, 7, 1)) == "2024/25"

    def test_junho_safra_anterior(self):
        assert safra_atual(date(2024, 6, 30)) == "2023/24"

    def test_none_uses_today(self):
        result = safra_atual()
        assert len(result) == 7
        assert "/" in result


class TestValidarSafra:
    def test_formato_padrao(self):
        assert validar_safra("2024/25") is True

    def test_formato_curto(self):
        assert validar_safra("24/25") is True

    def test_formato_completo(self):
        assert validar_safra("2024/2025") is True

    def test_invalido(self):
        assert validar_safra("abc") is False

    def test_vazio(self):
        assert validar_safra("") is False

    def test_apenas_ano(self):
        assert validar_safra("2024") is False


class TestNormalizarSafra:
    def test_padrao_retorna_igual(self):
        assert normalizar_safra("2024/25") == "2024/25"

    def test_curta(self):
        assert normalizar_safra("24/25") == "2024/25"

    def test_completa(self):
        assert normalizar_safra("2024/2025") == "2024/25"

    def test_curta_seculo_anterior(self):
        assert normalizar_safra("99/00") == "1999/00"

    def test_invalido_raises(self):
        with pytest.raises(ValueError, match="inválido"):
            normalizar_safra("abc")

    def test_vazio_raises(self):
        with pytest.raises(ValueError):
            normalizar_safra("")

    @pytest.mark.xfail(
        reason="GAP: normalizar_safra não faz strip — espaços não são tratados",
        strict=True,
    )
    def test_espacos_normalizados(self):
        assert normalizar_safra(" 2024 / 25 ") == "2024/25"


class TestSafraParaAnos:
    def test_padrao(self):
        assert safra_para_anos("2024/25") == (2024, 2025)

    def test_curta(self):
        assert safra_para_anos("24/25") == (2024, 2025)

    def test_virada_seculo(self):
        assert safra_para_anos("1999/00") == (1999, 2000)


class TestAnosParaSafra:
    def test_dois_anos(self):
        assert anos_para_safra(2024, 2025) == "2024/25"

    def test_ano_unico(self):
        assert anos_para_safra(2024) == "2024/25"


class TestSafraAnterior:
    def test_uma_safra(self):
        assert safra_anterior("2024/25") == "2023/24"

    def test_tres_safras(self):
        assert safra_anterior("2024/25", 3) == "2021/22"


class TestSafraPosterior:
    def test_uma_safra(self):
        assert safra_posterior("2024/25") == "2025/26"


class TestListaSafras:
    def test_range_5_safras(self):
        result = lista_safras("2020/21", "2024/25")

        assert len(result) == 5
        assert result[0] == "2020/21"
        assert result[-1] == "2024/25"

    def test_mesma_safra(self):
        assert lista_safras("2024/25", "2024/25") == ["2024/25"]


class TestPeriodoSafra:
    def test_periodo(self):
        inicio, fim = periodo_safra("2024/25")

        assert inicio == date(2024, 7, 1)
        assert fim == date(2025, 6, 30)


class TestDataParaSafra:
    def test_data_segundo_semestre(self):
        assert data_para_safra(date(2024, 10, 1)) == "2024/25"

    def test_data_primeiro_semestre(self):
        assert data_para_safra(date(2025, 3, 1)) == "2024/25"


class TestMesParaNumero:
    @pytest.mark.parametrize(
        "mes,numero",
        [
            ("jan", 1), ("fev", 2), ("mar", 3), ("abr", 4),
            ("mai", 5), ("jun", 6), ("jul", 7), ("ago", 8),
            ("set", 9), ("out", 10), ("nov", 11), ("dez", 12),
        ],
    )
    def test_todos_meses(self, mes, numero):
        assert mes_para_numero(mes) == numero

    def test_case_insensitive(self):
        assert mes_para_numero("JAN") == 1
        assert mes_para_numero("Jan") == 1

    def test_invalido_raises(self):
        with pytest.raises(KeyError):
            mes_para_numero("xyz")


class TestNumeroParaMes:
    def test_todos_numeros(self):
        assert numero_para_mes(1) == "jan"
        assert numero_para_mes(12) == "dez"

    def test_zero_wraps_to_last(self):
        assert numero_para_mes(0) == "dez"

    def test_13_raises(self):
        with pytest.raises(IndexError):
            numero_para_mes(13)
