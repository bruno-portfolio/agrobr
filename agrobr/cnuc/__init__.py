"""CNUC — Cadastro Nacional de Unidades de Conservação (MMA).

Fonte: WFS do MapServer do portal CNUC (sem autenticação), com as UCs federais, estaduais e
municipais, inclusive RPPNs, que têm limite cadastrado.
Licença: CC-BY (portal de dados abertos do MMA).
"""

from agrobr.cnuc.api import ucs, ucs_geo

__all__ = ["ucs", "ucs_geo"]
