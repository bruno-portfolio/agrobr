from __future__ import annotations

import typing

from agrobr import conab


def _sobrecargas(funcao: typing.Any) -> set[tuple[str, str, str]]:
    return {
        (
            s.__annotations__["as_polars"],
            s.__annotations__["return_meta"],
            s.__annotations__["return"],
        )
        for s in typing.get_overloads(funcao)
    }


def test_catalogo_sociobiodiversidade_tipa_pandas_e_polars_como_o_custo_producao():
    assert _sobrecargas(conab.catalogo_sociobiodiversidade) == _sobrecargas(conab.custo_producao)
    assert ("Literal[True]", "Literal[False]", "pl.DataFrame") in _sobrecargas(
        conab.catalogo_sociobiodiversidade
    )
