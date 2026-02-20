"""ANTT Pedagio — fluxo de veiculos em pracas de pedagio rodoviario.

Fonte: dados abertos ANTT via portal CKAN (CC-BY).
Cobertura: 2010+, atualizacao mensal.

Proxy de escoamento de safra — veiculos comerciais pesados (3+ eixos)
correlacionam com transporte de graos.
"""

from agrobr.alt.antt_pedagio.api import fluxo_pedagio, pracas_pedagio

__all__ = ["fluxo_pedagio", "pracas_pedagio"]
