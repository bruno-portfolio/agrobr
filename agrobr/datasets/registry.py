"""Registry de datasets com auto-descoberta."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from agrobr.datasets.base import BaseDataset

_REGISTRY: dict[str, BaseDataset] = {}


def register(dataset: BaseDataset) -> BaseDataset:
    """Registra um dataset no registry global."""
    _REGISTRY[dataset.info.name] = dataset
    return dataset


def get_dataset(name: str) -> BaseDataset:
    """Retorna instância de um dataset pelo nome."""
    if name not in _REGISTRY:
        raise KeyError(f"Dataset '{name}' não encontrado. Disponíveis: {list(_REGISTRY.keys())}")
    return _REGISTRY[name]


def list_datasets() -> list[str]:
    """Lista nomes de todos os datasets registrados."""
    return sorted(_REGISTRY.keys())


def list_products(name: str) -> list[str]:
    """Lista produtos disponíveis para um dataset."""
    return get_dataset(name).info.products


def info(name: str) -> dict[str, Any]:
    """Retorna metadados de um dataset."""
    return get_dataset(name).info.to_dict()
