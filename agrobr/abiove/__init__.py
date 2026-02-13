"""ABIOVE — Associação Brasileira das Indústrias de Óleos Vegetais.

Dados de exportação do complexo soja e milho.
Fonte: https://abiove.org.br/estatisticas/

LICENÇA: Sem termos de uso públicos localizados. Autorização formal
solicitada em fev/2026 — aguardando resposta. Classificação: zona_cinza.
"""

from agrobr.abiove.api import exportacao

__all__ = ["exportacao"]
