"""agrobr - Dados agricolas brasileiros em uma linha de codigo."""

from __future__ import annotations

__version__ = "0.9.0"
__author__ = "Bruno"

from agrobr import (
    abiove,
    anda,
    bcb,
    cepea,
    comexstat,
    conab,
    contracts,
    datasets,
    deral,
    ibge,
    imea,
    inmet,
    nasa_power,
    usda,
)
from agrobr.datasets.deterministic import deterministic
from agrobr.models import MetaInfo

__all__ = [
    "abiove",
    "anda",
    "bcb",
    "cepea",
    "comexstat",
    "conab",
    "contracts",
    "datasets",
    "deral",
    "deterministic",
    "ibge",
    "imea",
    "inmet",
    "nasa_power",
    "usda",
    "MetaInfo",
    "__version__",
]
