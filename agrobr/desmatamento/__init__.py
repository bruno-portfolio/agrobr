"""Desmatamento — Dados de desmatamento PRODES e alertas DETER (INPE/TerraBrasilis).

Dados tabulares de desmatamento consolidado (PRODES, anual, nos 6 biomas) e alertas
em tempo real (DETER, diario, so Amazonia e Cerrado).

Fonte: https://terrabrasilis.dpi.inpe.br
Licenca: Dados publicos governo federal — uso livre com citacao.
"""

from agrobr.desmatamento.api import deter, deter_geo, prodes, prodes_geo

__all__ = ["deter", "deter_geo", "prodes", "prodes_geo"]
