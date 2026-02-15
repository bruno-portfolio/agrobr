"""Modulo IBGE - Dados PAM, LSPA, PPM e Abate."""

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

__all__ = [
    "abate",
    "especies_abate",
    "especies_ppm",
    "lspa",
    "pam",
    "ppm",
    "produtos_lspa",
    "produtos_pam",
    "ufs",
]
