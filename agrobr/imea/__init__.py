"""IMEA — Instituto Mato-Grossense de Economia Agropecuária.

Cotações, indicadores e dados de safra para Mato Grosso.
Fonte: API pública IMEA (api1.imea.com.br).

LICENÇA: Termos de uso IMEA proíbem redistribuição de dados sem
autorização escrita. Uso pessoal/educacional apenas.
Ref: https://imea.com.br/imea-site/termo-de-uso.html
"""

from agrobr.imea.api import cotacoes

__all__ = ["cotacoes"]
