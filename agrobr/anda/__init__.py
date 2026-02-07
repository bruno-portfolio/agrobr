"""Módulo ANDA — dados de entregas de fertilizantes.

Requer pdfplumber como dependência opcional: pip install agrobr[pdf]
"""

from agrobr.anda.api import entregas

__all__ = ["entregas"]
