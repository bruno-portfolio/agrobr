"""Módulo para coleta de dados do Notícias Agrícolas (fonte alternativa CEPEA)."""

from agrobr.noticias_agricolas.client import fetch_indicador_page
from agrobr.noticias_agricolas.parser import parse_indicador

__all__ = ["fetch_indicador_page", "parse_indicador"]
