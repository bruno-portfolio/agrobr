"""Modulo IBGE - Dados PAM, LSPA, PPM, Abate, PEVS, Leite, PIB, Censo Agropecuario, Censo Legado, Serie Historica e Municipal 1985."""

from __future__ import annotations

from agrobr.ibge.api import (
    abate,
    especies_abate,
    especies_ppm,
    lspa,
    pam,
    ppm,
    produtos_lspa,
    produtos_pam,
    ufs,
)
from agrobr.ibge.censo_api import (
    censo_agro,
    censo_agro_historico,
    temas_censo_agro,
    temas_censo_agro_historico,
)
from agrobr.ibge.censo_municipal_1985 import (
    censo_agro_municipal_1985,
    cobertura_censo_agro_municipal_1985,
    temas_censo_agro_municipal_1985,
)
from agrobr.ibge.legacy_api import censo_agro_legado, temas_censo_agro_legado
from agrobr.ibge.pesquisas_api import (
    especies_silvicultura_area,
    extracao_vegetal,
    leite_trimestral,
    pib_agro,
    produtos_extracao_vegetal,
    produtos_silvicultura,
    silvicultura,
)

__all__ = [
    "abate",
    "censo_agro",
    "censo_agro_historico",
    "censo_agro_legado",
    "censo_agro_municipal_1985",
    "cobertura_censo_agro_municipal_1985",
    "especies_abate",
    "especies_ppm",
    "especies_silvicultura_area",
    "extracao_vegetal",
    "leite_trimestral",
    "lspa",
    "pam",
    "pib_agro",
    "ppm",
    "produtos_extracao_vegetal",
    "produtos_lspa",
    "produtos_pam",
    "produtos_silvicultura",
    "silvicultura",
    "temas_censo_agro",
    "temas_censo_agro_historico",
    "temas_censo_agro_legado",
    "temas_censo_agro_municipal_1985",
    "ufs",
]
