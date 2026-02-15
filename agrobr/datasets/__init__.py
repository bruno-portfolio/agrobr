"""Camada semântica de datasets do agrobr."""

from agrobr.datasets.abate_trimestral import abate_trimestral
from agrobr.datasets.balanco import balanco
from agrobr.datasets.censo_agropecuario import censo_agropecuario
from agrobr.datasets.credito_rural import credito_rural
from agrobr.datasets.custo_producao import custo_producao
from agrobr.datasets.deterministic import deterministic, get_snapshot, is_deterministic
from agrobr.datasets.estimativa_safra import estimativa_safra
from agrobr.datasets.exportacao import exportacao
from agrobr.datasets.fertilizante import fertilizante
from agrobr.datasets.pecuaria_municipal import pecuaria_municipal
from agrobr.datasets.preco_diario import preco_diario
from agrobr.datasets.producao_anual import producao_anual
from agrobr.datasets.registry import (
    describe,
    describe_all,
    get_dataset,
    info,
    list_datasets,
    list_products,
)

__all__ = [
    "abate_trimestral",
    "balanco",
    "censo_agropecuario",
    "credito_rural",
    "custo_producao",
    "describe",
    "describe_all",
    "deterministic",
    "estimativa_safra",
    "exportacao",
    "fertilizante",
    "get_dataset",
    "get_snapshot",
    "info",
    "is_deterministic",
    "list_datasets",
    "list_products",
    "pecuaria_municipal",
    "preco_diario",
    "producao_anual",
]
