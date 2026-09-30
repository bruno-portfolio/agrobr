from __future__ import annotations

import re

from agrobr.exceptions import InvalidParameterError
from agrobr.normalize.dates import MESES_PT as MESES_PT
from agrobr.utils.time import hoje

_EDICAO = re.compile(r"(\d{4})-(0[1-9]|1[0-2])")

ANO_MINIMO = 2010

ABIOVE_PRODUTOS: dict[str, str] = {
    "grao": "grao",
    "grão": "grao",
    "soja em grão": "grao",
    "soja em grao": "grao",
    "soja grão": "grao",
    "soja grao": "grao",
    "grain": "grao",
    "soybeans": "grao",
    "soybean": "grao",
    "farelo": "farelo",
    "farelo de soja": "farelo",
    "soybean meal": "farelo",
    "soymeal": "farelo",
    "meal": "farelo",
    "oleo": "oleo",
    "óleo": "oleo",
    "oleo de soja": "oleo",
    "óleo de soja": "oleo",
    "soybean oil": "oleo",
    "soyoil": "oleo",
    "oil": "oleo",
    "milho": "milho",
    "corn": "milho",
    "maize": "milho",
    "total": "total",
}


def validate_selection(ano: int, mes: int | None, edicao: str | None) -> None:
    ano_atual = hoje().year
    if isinstance(ano, bool) or not isinstance(ano, int) or not ANO_MINIMO <= ano <= ano_atual:
        raise InvalidParameterError(
            f"ano deve ser um inteiro de {ANO_MINIMO} a {ano_atual}, recebido {ano!r}"
        )
    if mes is not None and (
        isinstance(mes, bool) or not isinstance(mes, int) or not 1 <= mes <= 12
    ):
        raise InvalidParameterError(f"mes deve estar entre 1 e 12, recebido {mes!r}")
    if edicao is None:
        return
    match = _EDICAO.fullmatch(edicao) if isinstance(edicao, str) else None
    if match is None or int(match[1]) not in (ano, ano + 1):
        raise InvalidParameterError(
            f"edicao deve ser 'AAAA-MM' de {ano} ou {ano + 1}, as edições que publicam {ano}; "
            f"recebido {edicao!r}"
        )


def normalize_produto(nome: str) -> str:
    key = nome.strip().lower()
    return ABIOVE_PRODUTOS.get(key, key)


def resolve_produto(nome: str) -> str:
    key = nome.strip().lower()
    if key not in ABIOVE_PRODUTOS:
        raise InvalidParameterError(
            f"produto desconhecido: {nome!r}. Válidos: {sorted(set(ABIOVE_PRODUTOS.values()))}"
        )
    return ABIOVE_PRODUTOS[key]
