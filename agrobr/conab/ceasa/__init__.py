"""CONAB CEASA/PROHORT — precos diarios de atacado hortifruti.

Precos de 48 produtos (frutas, hortalicas, ovos) em 43 CEASAs do Brasil.
Dados do sistema PROHORT via Pentaho CDA REST API.

Fonte: https://portaldeinformacoes.conab.gov.br/mercado-atacadista-hortigranjeiro.html
LICENCA: zona_cinza (credenciais publicas, API nao documentada oficialmente).
"""

from agrobr.conab.ceasa.api import categorias as categorias
from agrobr.conab.ceasa.api import lista_ceasas as lista_ceasas
from agrobr.conab.ceasa.api import precos as precos
from agrobr.conab.ceasa.api import produtos as produtos

__all__: list[str] = []
