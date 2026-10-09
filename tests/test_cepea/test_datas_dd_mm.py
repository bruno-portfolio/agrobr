from datetime import date

import pytest

from agrobr.cepea import api
from agrobr.exceptions import InvalidParameterError
from tests.helpers import levanta_exatamente


def test_normalize_dates_aceita_dd_mm_aaaa_como_o_resto_da_lib():
    assert api._normalize_dates("15/01/2025", "2025-01-31") == (
        date(2025, 1, 15),
        date(2025, 1, 31),
    )
    assert api._normalize_dates(" 01/02/2024 ", None)[0] == date(2024, 2, 1)


@pytest.mark.parametrize(
    ("inicio", "fim", "mensagem"),
    [
        (
            "2025/01/15",
            None,
            "inicio deve ser date, datetime ou texto AAAA-MM-DD ou DD/MM/AAAA: '2025/01/15'",
        ),
        ("2025-01-01", "31/02/2025", "fim contém data inexistente: '31/02/2025'"),
        (
            20250101,
            None,
            "inicio deve ser date, datetime ou texto AAAA-MM-DD ou DD/MM/AAAA: 20250101",
        ),
    ],
)
def test_normalize_dates_recusa_com_a_mensagem_do_parse_data(inicio, fim, mensagem):
    with levanta_exatamente(InvalidParameterError) as erro:
        api._normalize_dates(inicio, fim)

    assert str(erro.value) == mensagem
