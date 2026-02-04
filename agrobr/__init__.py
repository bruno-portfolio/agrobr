"""agrobr - Dados agricolas brasileiros em uma linha de codigo."""

from __future__ import annotations

__version__ = "0.2.0"
__author__ = "Bruno"

from agrobr import cepea, conab, ibge
from agrobr.models import MetaInfo

__all__ = ["cepea", "conab", "ibge", "MetaInfo", "__version__"]
