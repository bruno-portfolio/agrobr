"""IMEA — Instituto Mato-Grossense de Economia Agropecuária.

Cotações, indicadores e dados de safra para Mato Grosso.
Fonte: API pública IMEA (api1.imea.com.br).

LICENÇA: zona_cinza para as séries públicas, sem licença de reutilização comprovada.
Arquivos não públicos exigem autorização escrita para compartilhamento.
Ref: https://imea.com.br/imea-site/termo-de-uso.html
"""

from agrobr.imea.api import cotacoes

__all__ = ["cotacoes"]
