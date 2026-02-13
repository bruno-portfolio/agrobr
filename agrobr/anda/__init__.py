"""Módulo ANDA — dados de entregas de fertilizantes.

Requer pdfplumber como dependência opcional: pip install agrobr[pdf]

LICENÇA: Sem termos de uso públicos localizados. Autorização formal
solicitada em fev/2026 — aguardando resposta. Classificação: zona_cinza.
"""

from agrobr.anda.api import entregas

__all__ = ["entregas"]
