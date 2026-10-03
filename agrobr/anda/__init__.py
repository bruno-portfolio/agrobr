"""Módulo ANDA — dados de entregas de fertilizantes.

Requer pdfplumber como dependência opcional: pip install agrobr[pdf]

LICENÇA: fonte privada sem licença de reutilização das estatísticas localizada.
Classificação: zona_cinza.
"""

from agrobr.anda.api import entregas

__all__ = ["entregas"]
