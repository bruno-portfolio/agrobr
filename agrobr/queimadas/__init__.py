"""Queimadas/INPE — Focos de calor detectados por satelite.

Dados do Programa Queimadas do INPE (Instituto Nacional de Pesquisas Espaciais).
Focos de calor diarios e mensais detectados por satelites (NOAA-20, AQUA, TERRA, etc.)
com coordenadas, municipio, bioma e potencia radiativa do fogo (FRP).
Serie historica desde 1998.

Fonte: https://terrabrasilis.dpi.inpe.br/queimadas/portal/
Dados abertos: https://dataserver-coids.inpe.br/queimadas/queimadas/focos/csv/

LICENCA: Dados publicos INPE — livre para uso com citacao.
"""

from agrobr.queimadas.api import focos

__all__ = ["focos"]
