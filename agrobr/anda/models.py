from __future__ import annotations

from agrobr.exceptions import InvalidParameterError

FERTILIZANTES_MAP: dict[str, str] = {
    "npk": "npk",
    "ureia": "ureia",
    "uréia": "ureia",
    "map": "map",
    "dap": "dap",
    "kcl": "kcl",
    "cloreto de potássio": "kcl",
    "cloreto de potassio": "kcl",
    "superfosfato simples": "ssp",
    "ssp": "ssp",
    "superfosfato triplo": "tsp",
    "tsp": "tsp",
    "sulfato de amônio": "sulfato de amonio",
    "sulfato de amonio": "sulfato de amonio",
    "nitrato de amônio": "nitrato de amonio",
    "nitrato de amonio": "nitrato de amonio",
    "total": "total",
}


def normalize_fertilizante(nome: str) -> str:
    key = nome.strip().lower()
    return FERTILIZANTES_MAP.get(key, key)


def resolve_produto(nome: str) -> str:
    if not isinstance(nome, str):
        raise InvalidParameterError("produto deve ser uma string")
    produto = normalize_fertilizante(nome)
    if produto != "total":
        raise InvalidParameterError(
            f"A ANDA disponibiliza apenas entregas totais; produto {nome!r} não está disponível"
        )
    return produto
