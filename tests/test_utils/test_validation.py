from __future__ import annotations

from datetime import date, datetime

import pandas as pd
import pytest

from agrobr.exceptions import InvalidParameterError
from agrobr.utils import validation
from agrobr.utils.validation import parse_data, validate_bioma, validate_uf, validate_year_uf


class TestParseData:
    @pytest.mark.parametrize(
        "valor",
        [
            "2024-02-01",
            " 01/02/2024 ",
            date(2024, 2, 1),
            datetime(2024, 2, 1, 23, 59),
        ],
    )
    def test_formatos_aceitos(self, valor):
        resultado = parse_data(valor, "inicio")
        assert resultado == date(2024, 2, 1)
        assert type(resultado) is date

    def test_none_returns_none(self):
        assert parse_data(None) is None

    @pytest.mark.parametrize(
        "valor", ["2024/02/01", "1/2/2024", "20240201", "2024-W05-4", 20240201]
    )
    def test_formato_ou_tipo_fora_dos_aceitos(self, valor):
        with pytest.raises(InvalidParameterError, match="inicio deve ser date.*DD/MM/AAAA"):
            parse_data(valor, "inicio")

    @pytest.mark.parametrize("valor", ["2024-02-30", "31/04/2024"])
    def test_data_inexistente(self, valor):
        with pytest.raises(InvalidParameterError, match="fim contém data inexistente"):
            parse_data(valor, "fim")

    def test_nat_recusado(self):
        with pytest.raises(InvalidParameterError, match="inicio é NaT"):
            parse_data(pd.NaT, "inicio")

    def test_timestamp_aceito(self):
        resultado = parse_data(pd.Timestamp("2024-02-01 10:30"), "inicio")
        assert resultado == date(2024, 2, 1)
        assert type(resultado) is date


class TestValidateBioma:
    @pytest.mark.parametrize(
        ("entrada", "esperado"),
        [
            ("Amazonia", "Amazônia"),
            ("AMAZÔNIA", "Amazônia"),
            (" mata atlantica ", "Mata Atlântica"),
            ("Cerrado", "Cerrado"),
            ("Caatinga", "Caatinga"),
            ("Pampa", "Pampa"),
            ("Pantanal", "Pantanal"),
        ],
    )
    def test_valid_bioma(self, entrada, esperado):
        assert validate_bioma(entrada) == esperado

    def test_none_returns_none(self):
        assert validate_bioma(None) is None

    def test_invalid_bioma(self):
        with pytest.raises(ValueError, match="Bioma inválido.*Atlantida"):
            validate_bioma("Atlantida")

    def test_bioma_nao_str(self):
        with pytest.raises(InvalidParameterError, match="Bioma inválido: 3. Valores válidos: Amaz"):
            validate_bioma(3)


class TestValidateUf:
    def test_valid_uf_with_whitespace(self):
        assert validate_uf("  SP  ") == "SP"

    def test_uf_none_returns_none(self):
        assert validate_uf(None) is None

    def test_invalid_long_string(self):
        with pytest.raises(
            InvalidParameterError, match="UF inválida: 'INVALID'. Valores válidos: AC, AL"
        ):
            validate_uf("INVALID")

    @pytest.mark.parametrize("uf", [51, ["MT"], b"MT"])
    def test_uf_nao_str(self, uf):
        with pytest.raises(InvalidParameterError, match="UF inválida.*Valores válidos: AC, AL"):
            validate_uf(uf)

    @pytest.mark.parametrize("uf", ["Mato Grosso", " SÃO PAULO ", "Sao Paulo", "Distrito Federal"])
    def test_nome_completo_segue_recusado(self, uf):
        with pytest.raises(InvalidParameterError, match="UF inválida.*Valores válidos: AC, AL"):
            validate_uf(uf)

    @pytest.mark.parametrize("uf", ["Mato", "Grosso", "Estado de São Paulo", "São Paulo/SP"])
    def test_trecho_do_nome_segue_recusado(self, uf):
        with pytest.raises(InvalidParameterError, match="UF inválida.*Valores válidos: AC, AL"):
            validate_uf(uf)


class TestValidateYearUf:
    def test_ano_above_current(self):
        with pytest.raises(ValueError, match="fora do intervalo válido"):
            validate_year_uf(ano=2099)

    def test_ano_inicio_below_min(self):
        with pytest.raises(ValueError, match="anterior"):
            validate_year_uf(ano_inicio=2005, ano_min=2010)

    def test_ano_fim_future(self):
        with pytest.raises(ValueError, match="posterior"):
            validate_year_uf(ano_fim=2099)

    def test_inicio_after_fim(self):
        with pytest.raises(ValueError, match="ano_inicio"):
            validate_year_uf(ano_inicio=2024, ano_fim=2020)

    def test_custom_ufs_validas(self):
        custom = frozenset({"SP", "RJ"})
        validate_year_uf(uf="SP", ufs_validas=custom)
        with pytest.raises(ValueError, match="UF"):
            validate_year_uf(uf="MG", ufs_validas=custom)

    @pytest.mark.parametrize("uf", ["", "  ", [], ["MT"], 0])
    def test_uf_falsy_ou_nao_str_nao_passa(self, uf):
        with pytest.raises(InvalidParameterError, match="UF inválida"):
            validate_year_uf(uf=uf)

    def test_ano_corrente_vem_do_relogio_de_brasilia(self, monkeypatch):
        monkeypatch.setattr(validation.time_utils, "hoje", lambda: date(2099, 1, 1))
        validate_year_uf(ano=2099, ano_fim=2099)
        with pytest.raises(InvalidParameterError, match="fora do intervalo válido"):
            validate_year_uf(ano=2100)

    def test_all_params_valid(self):
        validate_year_uf(uf="SP", ano_inicio=2020, ano_fim=2023, ano_min=2010)
