"""agrobr - Dados agricolas brasileiros em uma linha de codigo."""

from __future__ import annotations

__version__ = "0.1.0"
__author__ = "Bruno"

from agrobr import cepea, conab, ibge

__all__ = ["cepea", "conab", "ibge", "__version__"]
