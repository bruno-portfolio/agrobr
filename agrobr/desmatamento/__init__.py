"""Desmatamento — Dados de desmatamento PRODES e alertas DETER (INPE/TerraBrasilis).

Dados tabulares de desmatamento consolidado (PRODES, anual) e alertas
em tempo real (DETER, diario) para todos os biomas brasileiros.

Fonte: https://terrabrasilis.dpi.inpe.br
Licenca: Dados publicos governo federal — uso livre com citacao.
"""

from agrobr.desmatamento.api import deter, prodes

__all__ = ["deter", "prodes"]
