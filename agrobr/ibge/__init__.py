"""Modulo IBGE - Dados PAM, LSPA, PPM, Abate e Censo Agropecuario."""

from __future__ import annotations

from agrobr.ibge.api import (
    abate,
    censo_agro,
    especies_abate,
    especies_ppm,
    lspa,
    pam,
    ppm,
    produtos_lspa,
    produtos_pam,
    temas_censo_agro,
    ufs,
)

__all__ = [
    "abate",
    "censo_agro",
    "especies_abate",
    "especies_ppm",
    "lspa",
    "pam",
    "ppm",
    "produtos_lspa",
    "produtos_pam",
    "temas_censo_agro",
    "ufs",
]
