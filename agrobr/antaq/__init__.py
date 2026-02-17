"""Módulo ANTAQ — movimentação portuária de carga.

Dados: Estatístico Aquaviário (ANTAQ)
URL: https://web3.antaq.gov.br/ea/sense/download.html
Licença: livre (dados públicos governo federal)
"""

from agrobr.antaq.api import movimentacao

__all__ = ["movimentacao"]
