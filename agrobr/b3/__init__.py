"""B3 — Futuros agricolas (ajustes diarios + posicoes em aberto).

Dados de ajuste diario (settlement prices) e posicoes em aberto (open interest)
de contratos futuros agropecuarios negociados na B3 (Brasil, Bolsa, Balcao).
Inclui boi gordo (BGI), milho (CCM), cafe arabica (ICF), cafe conillon (CNL),
etanol (ETH), soja cross (SJC) e soja FOB (SOY).

Fonte: https://www.b3.com.br
Ajustes: https://www.b3.com.br/pesquisapregao/download (BVBG-086 ZIP/XML)
OI: https://arquivos.b3.com.br/api/download (DerivativesOpenPosition)

LICENCA: zona_cinza — B3 e empresa privada. Dados publicados abertamente
sem autenticacao. Uso programatico nao possui termos claros.
"""

from agrobr.b3.api import ajustes, contratos, historico, oi_historico, posicoes_abertas

__all__ = ["ajustes", "contratos", "historico", "oi_historico", "posicoes_abertas"]
