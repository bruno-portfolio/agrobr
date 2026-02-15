"""CONAB CEASA/PROHORT — precos diarios de atacado hortifruti.

Precos de 48 produtos (frutas, hortalicas, ovos) em 43 CEASAs do Brasil.
Dados do sistema PROHORT via Pentaho CDA REST API.

Fonte: https://portaldeinformacoes.conab.gov.br/mercado-atacadista-hortigranjeiro.html
LICENCA: zona_cinza (credenciais publicas, API nao documentada oficialmente).
"""

from agrobr.conab.ceasa.api import categorias, lista_ceasas, precos, produtos

__all__ = ["categorias", "lista_ceasas", "precos", "produtos"]
