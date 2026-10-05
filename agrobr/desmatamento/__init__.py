"""Desmatamento — Dados de desmatamento PRODES e alertas DETER (INPE/TerraBrasilis).

Dados tabulares de desmatamento consolidado (PRODES, anual, nos 6 biomas) e alertas
em tempo real (DETER, diario, so Amazonia e Cerrado).

Fonte: https://terrabrasilis.dpi.inpe.br
Licença: CC BY-SA 4.0 (INPE), com atribuição e CompartilhaIgual nas adaptações.
"""

from agrobr.desmatamento.api import deter, deter_geo, prodes, prodes_geo

__all__ = ["deter", "deter_geo", "prodes", "prodes_geo"]
