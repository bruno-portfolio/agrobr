"""ABIOVE — Associação Brasileira das Indústrias de Óleos Vegetais.

Dados de exportação do complexo soja e milho.
Fonte: https://abiove.org.br/estatisticas/
"""

from agrobr.abiove.api import exportacao

__all__ = ["exportacao"]
