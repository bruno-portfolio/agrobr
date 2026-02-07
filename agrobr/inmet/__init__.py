"""Módulo INMET — dados meteorológicos do Brasil."""

from agrobr.inmet.api import clima_uf, estacao, estacoes

__all__ = ["estacao", "estacoes", "clima_uf"]
