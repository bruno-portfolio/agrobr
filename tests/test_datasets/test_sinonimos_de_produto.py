from __future__ import annotations

import inspect
from typing import Any
from unittest.mock import patch

import pandas as pd
import pytest

from agrobr import datasets
from agrobr.datasets import base
from agrobr.exceptions import InvalidParameterError
from agrobr.normalize.crops import normalizar_cultura
from tests.helpers import isolated_dataset_case

CAFE = ("cafe_robusta", "cafe_conilon", "cafe_conillon")
CASTANHA = ("castanha_para", "castanha_do_brasil")
CASOS = [
    ("preco_diario", CAFE),
    ("futuros_agricolas", CAFE),
    ("serie_historica_safra", CAFE),
    ("custo_producao", CAFE),
    ("extrativismo_vegetal", CASTANHA),
    ("custo_sociobiodiversidade", CASTANHA),
]
POSICIONAL = {"ano": 2024}


class _SemRede(Exception):
    pass


def _sem_rede(*_args: Any, **_kwargs: Any) -> None:
    raise _SemRede


def _argumentos(nome: str, produto: str) -> tuple[list[Any], dict[str, Any]]:
    posicionais: list[Any] = []
    nomeados: dict[str, Any] = {}
    for parametro in inspect.signature(getattr(datasets, nome)).parameters.values():
        if parametro.default is not inspect.Parameter.empty:
            continue
        valor = produto if parametro.name == "produto" else POSICIONAL.get(parametro.name)
        if parametro.kind is parametro.KEYWORD_ONLY:
            nomeados[parametro.name] = valor
        else:
            posicionais.append(valor)
    if "produto" not in inspect.signature(getattr(datasets, nome)).parameters:
        raise AssertionError(f"{nome} sem parâmetro produto")
    if not posicionais and "produto" not in nomeados:
        nomeados["produto"] = produto
    return posicionais, nomeados


def test_sinonimos_do_cafe_conilon_e_da_castanha_no_normalize():
    assert {nome: normalizar_cultura(nome) for nome in (*CAFE, "Café Conillon")} == dict.fromkeys(
        (*CAFE, "Café Conillon"), "cafe_robusta"
    )
    assert {nome: normalizar_cultura(nome) for nome in (*CASTANHA, "Castanha do Pará")} == (
        dict.fromkeys((*CASTANHA, "Castanha do Pará"), "castanha_do_brasil")
    )


@pytest.mark.parametrize(
    ("nome", "produto"),
    [(nome, produto) for nome, produtos in CASOS for produto in produtos],
    ids=[f"{nome}-{produto}" for nome, produtos in CASOS for produto in produtos],
)
async def test_dataset_aceita_os_sinonimos_do_produto(
    nome: str, produto: str, tmp_path, monkeypatch
):
    monkeypatch.setenv("AGROBR_CACHE_CACHE_DIR", str(tmp_path))
    posicionais, nomeados = _argumentos(nome, produto)
    recusa: InvalidParameterError | ValueError | None = None
    with (
        isolated_dataset_case(f"{nome}-{produto}"),
        patch("httpx.AsyncClient", side_effect=_sem_rede),
    ):
        try:
            await getattr(datasets, nome)(*posicionais, **nomeados)
        except (InvalidParameterError, ValueError) as erro:
            recusa = erro
        except Exception:
            pass

    assert recusa is None or produto not in str(recusa), f"{nome} recusou {produto}: {recusa}"


@pytest.mark.parametrize("native", [False, True])
@pytest.mark.parametrize("positional", [False, True])
@pytest.mark.parametrize("produto", ["SOJA", "soybean"])
async def test_wrapper_normaliza_produto_antes_do_fetch(native, positional, produto):
    dataset = datasets.get_dataset("balanco")

    async def fetch(self, produto):
        self._validate_produto(produto)
        return pd.DataFrame({"produto": [produto]})

    async def native_fetch(self, produto, *, as_polars=False):
        assert as_polars is False
        return await fetch(self, produto)

    wrapped = base._with_output_format(native_fetch if native else fetch)
    frame = (
        await wrapped(dataset, produto, as_polars=False)
        if positional
        else await wrapped(dataset, produto=produto, as_polars=False)
    )
    assert frame["produto"].tolist() == ["soja"]
