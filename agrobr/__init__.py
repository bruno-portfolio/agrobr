"""agrobr - Dados agricolas brasileiros em uma linha de codigo."""

from __future__ import annotations

__version__ = "0.6.3"
__author__ = "Bruno"

from agrobr import cepea, conab, datasets, ibge
from agrobr.datasets.deterministic import deterministic
from agrobr.models import MetaInfo

__all__ = [
    "cepea",
    "conab",
    "datasets",
    "deterministic",
    "ibge",
    "MetaInfo",
    "__version__",
]
