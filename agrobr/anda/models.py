from __future__ import annotations

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

ANDA_UFS: list[str] = [
    "AC",
    "AL",
    "AM",
    "AP",
    "BA",
    "CE",
    "DF",
    "ES",
    "GO",
    "MA",
    "MG",
    "MS",
    "MT",
    "PA",
    "PB",
    "PE",
    "PI",
    "PR",
    "RJ",
    "RN",
    "RO",
    "RR",
    "RS",
    "SC",
    "SE",
    "SP",
    "TO",
]


def normalize_fertilizante(nome: str) -> str:
    key = nome.strip().lower()
    return FERTILIZANTES_MAP.get(key, key)


def resolve_produto(nome: str) -> str:
    produto = normalize_fertilizante(nome)
    if produto != "total":
        raise ValueError(
            f"A ANDA disponibiliza apenas entregas totais; produto {nome!r} não está disponível"
        )
    return produto
