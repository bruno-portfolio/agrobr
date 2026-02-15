"""agrobr - Dados agricolas brasileiros em uma linha de codigo."""

from __future__ import annotations

__version__ = "0.9.0"
__author__ = "Bruno"

from agrobr import (
    abiove,
    anda,
    b3,
    bcb,
    cepea,
    comexstat,
    conab,
    contracts,
    datasets,
    deral,
    desmatamento,
    ibge,
    imea,
    inmet,
    mapbiomas,
    nasa_power,
    queimadas,
    usda,
)
from agrobr.datasets.deterministic import deterministic
from agrobr.models import MetaInfo

__all__ = [
    "abiove",
    "anda",
    "b3",
    "bcb",
    "cepea",
    "comexstat",
    "conab",
    "contracts",
    "datasets",
    "deral",
    "desmatamento",
    "deterministic",
    "ibge",
    "imea",
    "inmet",
    "mapbiomas",
    "nasa_power",
    "queimadas",
    "usda",
    "MetaInfo",
    "__version__",
]
