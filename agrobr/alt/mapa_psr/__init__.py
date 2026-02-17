"""MAPA PSR — apolices e sinistros do seguro rural brasileiro.

Fonte: SISSER/MAPA dados abertos (CC-BY).
Cobertura: 2006+, atualizacao anual.

Proxy de revisao de producao — sinistros elevados em soja Q1
antecedem cortes na estimativa CONAB Q2.
"""

from agrobr.alt.mapa_psr.api import apolices, sinistros

__all__ = ["apolices", "sinistros"]
