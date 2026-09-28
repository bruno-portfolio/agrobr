"""Módulo INMET — dados meteorológicos do Brasil."""

from agrobr.inmet.api import clima_uf, estacao, estacoes, historico, historico_periodo, historico_uf

__all__ = ["estacao", "estacoes", "clima_uf", "historico", "historico_periodo", "historico_uf"]
