from __future__ import annotations

import copy
from typing import TYPE_CHECKING, Any

from agrobr import constants, exceptions

if TYPE_CHECKING:
    from agrobr.datasets.base import BaseDataset, DatasetInfo

_REGISTRY: dict[str, BaseDataset] = {}
_FONTES_INTERNAS: dict[str, tuple[str, ...]] = {"cepea": ("noticias_agricolas",)}


def register(dataset: BaseDataset) -> BaseDataset:
    _REGISTRY[dataset.info.name] = dataset
    return dataset


def get_dataset(name: str) -> BaseDataset:
    if not isinstance(name, str) or name not in _REGISTRY:
        raise exceptions.UnknownNameError(
            f"Dataset '{name}' não encontrado. Disponíveis: {list(_REGISTRY.keys())}"
        )
    dataset = copy.deepcopy(_REGISTRY[name])
    dataset.info = copy.deepcopy(dataset.info)
    return dataset


def list_datasets() -> list[str]:
    return sorted(_REGISTRY.keys())


def list_products(name: str) -> list[str]:
    return get_dataset(name).info.products


def info(name: str) -> dict[str, Any]:
    dataset_info = get_dataset(name).info
    return {**dataset_info.to_dict(), "licenses": _licencas_por_fonte(dataset_info)}


def _licencas_por_fonte(info: DatasetInfo) -> dict[str, str | None]:
    """Licença de cada adaptador e, logo depois dele, das fontes que ele tenta por dentro."""
    nomes = [
        nome
        for fonte in info.sources
        for nome in (fonte.name, *_FONTES_INTERNAS.get(fonte.name, ()))
    ]
    return {nome: constants.licenca_da_fonte(nome) for nome in nomes}


def _licencas(info: DatasetInfo) -> str:
    classes = _licencas_por_fonte(info)
    if len({classe for classe in classes.values() if classe}) < 2:
        return info.license
    return ", ".join(f"{classe} ({fonte})" for fonte, classe in classes.items() if classe)


def describe(name: str) -> str:
    d = get_dataset(name)
    i = d.info
    lines = [
        f"Dataset: {i.name}",
        f"  {i.description}",
        f"  Institution: {i.source_institution or 'N/A'}",
        f"  URL: {i.source_url or 'N/A'}",
        f"  License: {_licencas(i)}",
        f"  Products: {', '.join(i.products)}",
        f"  Sources: {' > '.join(s.name for s in i.sources)}",
        f"  Frequency: {i.update_frequency} (latency: {i.typical_latency})",
        f"  Contract: v{i.contract_version}",
        f"  Min date: {i.min_date or 'N/A'}",
        f"  Unit: {i.unit or 'N/A'}",
    ]
    return "\n".join(lines)


def describe_all() -> str:
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
            f"{i.update_frequency:<10} {_licencas(i):<12} {products}"
        )
    return "\n".join(lines)
