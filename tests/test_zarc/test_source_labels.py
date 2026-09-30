from __future__ import annotations

import pytest

from agrobr.exceptions import InvalidParameterError
from agrobr.zarc import parser as zarc_parser
from agrobr.zarc import query
from tests.helpers import zarc_csv


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("  Caju  Anão Produção\xa0 ", "caju_anao"),
        ("Macaúba (Acrocomia aculeata) Implantação", "macauba_aculeata_implantacao"),
        ("Macaúba (Acrocomia intumescens) Produção", "macauba_intumescens"),
        ("  CAFÉ ARÁBICA  PRODUÇÃO ", "cafe_arabica"),
    ],
)
def test_rotulos_normalizam_espacos_acentos_case(raw: str, expected: str):
    assert zarc_parser._normalize_cultura(raw) == expected


@pytest.mark.parametrize(
    "culture,season",
    [("sisal", "perene"), ("soja", "2025/2026"), ("cafe_arabica", "perene")],
)
def test_cultura_ausente_nao_inventa_dica(culture: str, season: str):
    years = ("", "PERENE") if season == "perene" else ("2025", "2026")
    content = zarc_csv([{"Nome_cultura": "Arroz", "SafraIni": years[0], "SafraFin": years[1]}])
    with pytest.raises(InvalidParameterError) as error:
        zarc_parser.parse_tabua_risco_bundle(
            content, query=query.build_query(produto=culture, safra=season), expected_safra=season
        )
    assert "não encontrada na tábua" in str(error.value)
    assert "está na" not in str(error.value)
