"""Cadastro, autorizações e composição de defensivos do Agrofit/MAPA.

Fonte: Portal de Dados Abertos do MAPA, sob Creative Commons Attribution.
"""

from agrobr.defensivos.api import autorizacoes, composicao, formulados, tecnicos

__all__ = ["autorizacoes", "composicao", "formulados", "tecnicos"]
