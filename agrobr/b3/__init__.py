"""B3 — Ajustes diarios de futuros agricolas (boi, milho, soja, cafe, etanol).

Dados de ajuste diario (settlement prices) de contratos futuros agropecuarios
negociados na B3 (Brasil, Bolsa, Balcao). Inclui boi gordo (BGI), milho (CCM),
cafe arabica (ICF), cafe conillon (CNL), etanol (ETH), soja cross (SJC) e
soja FOB (SOY).

Fonte: https://www.b3.com.br
Dados: https://www2.bmf.com.br/pages/portal/bmfbovespa/boletim1/Ajustes1.asp

LICENCA: zona_cinza — B3 e empresa privada. Ajustes diarios sao publicados
abertamente sem autenticacao. Uso programatico nao possui termos claros.
"""

from agrobr.b3.api import ajustes, contratos, historico

__all__ = ["ajustes", "contratos", "historico"]
