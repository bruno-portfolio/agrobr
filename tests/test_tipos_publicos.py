from __future__ import annotations

import importlib
import inspect
import typing
from collections.abc import Callable
from typing import Any

import pytest

from agrobr import datasets, sync

FONTES = sorted(
    {f"agrobr.{nome}" for nome in sync._modules if nome not in ("datasets", "sicar")}
    | {f"agrobr.alt.{nome}" for nome in sync._SyncAlt()._modules}
)


def _funcoes_das_fontes() -> list[tuple[str, Callable[..., Any]]]:
    vistas: dict[int, tuple[str, Callable[..., Any]]] = {}
    for caminho in FONTES:
        modulo = importlib.import_module(caminho)
        for nome, funcao in inspect.getmembers(modulo, inspect.iscoroutinefunction):
            if not nome.startswith("_") and "return_meta" in inspect.signature(funcao).parameters:
                vistas.setdefault(id(funcao), (f"{caminho}.{nome}", funcao))
    return sorted(vistas.values())


def _retornos(funcao: Callable[..., Any], return_meta: str) -> set[str]:
    return {
        sobrecarga.__annotations__["return"]
        for sobrecarga in typing.get_overloads(funcao)
        if sobrecarga.__annotations__.get("return_meta") == return_meta
    }


def _conferir(funcao: Callable[..., Any], simples: str, com_meta: str) -> None:
    assert typing.get_overloads(funcao), "sem @overload: o checker vê a união DataFrame | tuple"
    assert {simples, "DataFrame", "result_utils.DataFrame"} & _retornos(funcao, "Literal[False]")
    assert {
        com_meta,
        "tuple[DataFrame, MetaInfo]",
        "tuple[result_utils.DataFrame, MetaInfo]",
    } & _retornos(funcao, "Literal[True]")


@pytest.mark.parametrize("nome", datasets.list_datasets())
def test_dataset_tipa_o_retorno_por_return_meta(nome):
    _conferir(getattr(datasets, nome), "pd.DataFrame", "tuple[pd.DataFrame, MetaInfo]")


@pytest.mark.parametrize(
    ("nome", "funcao"),
    _funcoes_das_fontes(),
    ids=lambda valor: valor if isinstance(valor, str) else "",
)
def test_fonte_tipa_o_retorno_por_return_meta(nome, funcao):
    retornos = _retornos(funcao, "Literal[True]")
    assert typing.get_overloads(funcao), f"{nome} sem @overload"
    assert _retornos(funcao, "Literal[False]"), f"{nome} sem o @overload de return_meta=False"
    assert retornos and all(retorno.startswith("tuple[") for retorno in retornos), (nome, retornos)
    assert not any(
        retorno.startswith("tuple[") for retorno in _retornos(funcao, "Literal[False]")
    ), nome


def test_cepea_indicador_sem_as_polars_e_so_pandas():
    from agrobr import cepea

    sobrecargas = typing.get_overloads(cepea.indicador)
    sem_polars = {
        (s.__annotations__["return_meta"], s.__annotations__["return"])
        for s in sobrecargas
        if s.__annotations__["as_polars"] == "Literal[False]"
    }
    assert sem_polars == {
        ("Literal[False]", "pd.DataFrame"),
        ("Literal[True]", "tuple[pd.DataFrame, MetaInfo]"),
    }


def test_fontes_percorridas_incluem_as_do_cr442():
    nomes = {nome for nome, _ in _funcoes_das_fontes()}
    assert {
        "agrobr.inmet.historico",
        "agrobr.nasa_power.clima_ponto",
        "agrobr.alt.mapa_psr.apolices",
        "agrobr.alt.anp_diesel.vendas_diesel",
        "agrobr.alt.antt_pedagio.fluxo_pedagio",
        "agrobr.incra.vinculos_quilombolas",
    } <= nomes
