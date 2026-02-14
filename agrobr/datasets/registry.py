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


def describe(name: str) -> str:
    """Retorna descricao formatada de um dataset para exibicao."""
    d = get_dataset(name)
    i = d.info
    lines = [
        f"Dataset: {i.name}",
        f"  {i.description}",
        f"  Institution: {i.source_institution or 'N/A'}",
        f"  URL: {i.source_url or 'N/A'}",
        f"  License: {i.license}",
        f"  Products: {', '.join(i.products)}",
        f"  Sources: {' > '.join(s.name for s in i.sources)}",
        f"  Frequency: {i.update_frequency} (latency: {i.typical_latency})",
        f"  Contract: v{i.contract_version}",
        f"  Min date: {i.min_date or 'N/A'}",
        f"  Unit: {i.unit or 'N/A'}",
    ]
    return "\n".join(lines)


def describe_all() -> str:
    """Retorna tabela resumida de todos os datasets registrados."""
    lines = [
        f"{'Dataset':<20} {'Institution':<15} {'Frequency':<10} {'License':<12} {'Products'}",
        "-" * 90,
    ]
    for name in sorted(_REGISTRY):
        i = _REGISTRY[name].info
        products = ", ".join(i.products[:4])
        if len(i.products) > 4:
            products += f" +{len(i.products) - 4}"
        lines.append(
            f"{i.name:<20} {i.source_institution:<15} "
            f"{i.update_frequency:<10} {i.license:<12} {products}"
        )
    return "\n".join(lines)
