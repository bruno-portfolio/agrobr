"""ANP Diesel — precos de revenda e volumes de venda de diesel no Brasil.

Fonte: Agencia Nacional do Petroleo, Gas Natural e Biocombustiveis (ANP).
Dados abertos Gov.br (Decreto 8.777/2016). Licenca: livre.

Proxy de atividade mecanizada agricola — diesel eh o principal insumo
energetico da producao agropecuaria brasileira.
"""

from agrobr.alt.anp_diesel.api import precos_diesel, vendas_diesel

__all__ = ["precos_diesel", "vendas_diesel"]
