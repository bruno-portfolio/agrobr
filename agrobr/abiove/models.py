from __future__ import annotations

from agrobr.normalize.dates import MESES_PT as MESES_PT

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


def normalize_produto(nome: str) -> str:
    key = nome.strip().lower()
    return ABIOVE_PRODUTOS.get(key, key)


def resolve_produto(nome: str) -> str:
    key = nome.strip().lower()
    if key not in ABIOVE_PRODUTOS:
        raise ValueError(
            f"produto desconhecido: {nome!r}. Válidos: {sorted(set(ABIOVE_PRODUTOS.values()))}"
        )
    return ABIOVE_PRODUTOS[key]
