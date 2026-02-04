"""Modulo IBGE - Dados PAM e LSPA."""

from __future__ import annotations

from agrobr.ibge.api import (
    lspa,
    pam,
    produtos_lspa,
    produtos_pam,
    ufs,
)

__all__ = [
    "pam",
    "lspa",
    "produtos_pam",
    "produtos_lspa",
    "ufs",
]
