from __future__ import annotations

import copy
from typing import TYPE_CHECKING, Any

from agrobr import contracts, exceptions

if TYPE_CHECKING:
    from agrobr.datasets.base import BaseDataset, DatasetInfo

_REGISTRY: dict[str, BaseDataset] = {}


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
    return get_dataset(name).info.to_dict()


def _licencas(info: DatasetInfo) -> str:
    classes: dict[str, str | None] = info.to_dict()["licenses"]
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
        f"  Contract: v{i.contract_version}" + (" (modo padrão)" if d._modos_de_contrato else ""),
        *(
            f"    {rotulo}: {nome} v{contracts.get_contract(nome).version}"
            for rotulo, argumentos in d._modos_de_contrato.items()
            for nome in [d._contract_name(**argumentos)]
            if nome
        ),
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
