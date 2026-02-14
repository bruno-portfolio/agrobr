"""MapBiomas — Cobertura e uso da terra no Brasil (1985-presente).

Dados tabulares do Projeto MapBiomas: area (ha) por classe de cobertura
e uso da terra, bioma e estado, com serie historica anual desde 1985.
Inclui dados de cobertura e transicao entre classes.

Fonte: https://brasil.mapbiomas.org
Dados abertos: https://brasil.mapbiomas.org/estatisticas/

LICENCA: Dados publicos — livre para uso com citacao ao projeto MapBiomas.
"""

from agrobr.mapbiomas.api import cobertura, transicao

__all__ = ["cobertura", "transicao"]
