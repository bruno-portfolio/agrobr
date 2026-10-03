"""ABIOVE — Associação Brasileira das Indústrias de Óleos Vegetais.

Dados de exportação do complexo soja e milho.
Fonte: https://abiove.org.br/estatisticas/

LICENÇA: fonte privada sem licença de reutilização das estatísticas localizada.
Classificação: zona_cinza.
"""

from agrobr.abiove.api import exportacao

__all__ = ["exportacao"]
