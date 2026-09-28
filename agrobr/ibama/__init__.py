"""IBAMA — Embargos ambientais.
Fonte: portal de dados abertos do IBAMA (CSV de termos de embargo com geometrias WKT, sem auth).
Licenca: "Outra (Aberta)" no catalogo; dados abertos federais (Decreto 8.777/2016).
"""

from agrobr.ibama.api import embargos, embargos_geo

__all__ = ["embargos", "embargos_geo"]
