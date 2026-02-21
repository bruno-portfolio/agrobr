from __future__ import annotations

from decimal import Decimal
from typing import Literal

UnidadeOrigem = Literal[
    "sc60kg",
    "sc50kg",
    "sc40kg",
    "ton",
    "t",
    "kg",
    "bu",
    "bushel",
    "arroba",
    "@",
    "mil_ton",
    "mil_t",
    "mil_ha",
    "ha",
]

UnidadeDestino = UnidadeOrigem

PESO_SACA_KG: dict[str, Decimal] = {
    "sc60kg": Decimal("60"),
    "sc50kg": Decimal("50"),
    "sc40kg": Decimal("40"),
}

PESO_BUSHEL_KG: dict[str, Decimal] = {
    "soja": Decimal("27.2155"),
    "milho": Decimal("25.4012"),
    "trigo": Decimal("27.2155"),
}

PESO_ARROBA_KG = Decimal("15")

FATORES_CONVERSAO: dict[tuple[str, str], Decimal] = {
    ("kg", "ton"): Decimal("0.001"),
    ("ton", "kg"): Decimal("1000"),
    ("kg", "t"): Decimal("0.001"),
    ("t", "kg"): Decimal("1000"),
    ("kg", "sc60kg"): Decimal("1") / Decimal("60"),
    ("sc60kg", "kg"): Decimal("60"),
    ("kg", "sc50kg"): Decimal("1") / Decimal("50"),
    ("sc50kg", "kg"): Decimal("50"),
    ("ton", "sc60kg"): Decimal("1000") / Decimal("60"),
    ("sc60kg", "ton"): Decimal("60") / Decimal("1000"),
    ("t", "sc60kg"): Decimal("1000") / Decimal("60"),
    ("sc60kg", "t"): Decimal("60") / Decimal("1000"),
    ("kg", "arroba"): Decimal("1") / Decimal("15"),
    ("arroba", "kg"): Decimal("15"),
    ("kg", "@"): Decimal("1") / Decimal("15"),
    ("@", "kg"): Decimal("15"),
    ("arroba", "@"): Decimal("1"),
    ("@", "arroba"): Decimal("1"),
    ("ton", "arroba"): Decimal("1000") / Decimal("15"),
    ("arroba", "ton"): Decimal("15") / Decimal("1000"),
    ("mil_ton", "ton"): Decimal("1000"),
    ("ton", "mil_ton"): Decimal("0.001"),
    ("mil_t", "t"): Decimal("1000"),
    ("t", "mil_t"): Decimal("0.001"),
    ("mil_ha", "ha"): Decimal("1000"),
    ("ha", "mil_ha"): Decimal("0.001"),
}


def converter(
    valor: Decimal | float | int,
    de: UnidadeOrigem,
    para: UnidadeDestino,
    produto: str | None = None,
) -> Decimal:
    if not isinstance(valor, Decimal):
        valor = Decimal(str(valor))

    de_norm = _normalizar_unidade(de)
    para_norm = _normalizar_unidade(para)

    if de_norm == para_norm:
        return valor

    if de_norm in ("bu", "bushel") or para_norm in ("bu", "bushel"):
        return _converter_bushel(valor, de_norm, para_norm, produto)

    chave = (de_norm, para_norm)
    if chave in FATORES_CONVERSAO:
        return valor * FATORES_CONVERSAO[chave]

    valor_kg = _para_kg(valor, de_norm)
    return _de_kg(valor_kg, para_norm)


def _normalizar_unidade(unidade: str) -> str:
    unidade = unidade.lower().strip()

    aliases = {
        "t": "ton",
        "tonelada": "ton",
        "toneladas": "ton",
        "quilograma": "kg",
        "quilogramas": "kg",
        "saca": "sc60kg",
        "sacas": "sc60kg",
        "bushel": "bu",
        "bushels": "bu",
        "@": "arroba",
        "arrobas": "arroba",
        "mil_t": "mil_ton",
        "hectare": "ha",
        "hectares": "ha",
    }

    return aliases.get(unidade, unidade)


def _para_kg(valor: Decimal, unidade: str) -> Decimal:
    if unidade == "kg":
        return valor
    if unidade == "ton":
        return valor * Decimal("1000")
    if unidade == "mil_ton":
        return valor * Decimal("1000000")
    if unidade in PESO_SACA_KG:
        return valor * PESO_SACA_KG[unidade]
    if unidade == "sc60kg":
        return valor * Decimal("60")
    if unidade == "arroba":
        return valor * PESO_ARROBA_KG

    raise ValueError(f"Conversão de '{unidade}' para kg não suportada")


def _de_kg(valor_kg: Decimal, unidade: str) -> Decimal:
    if unidade == "kg":
        return valor_kg
    if unidade == "ton":
        return valor_kg / Decimal("1000")
    if unidade == "mil_ton":
        return valor_kg / Decimal("1000000")
    if unidade in PESO_SACA_KG:
        return valor_kg / PESO_SACA_KG[unidade]
    if unidade == "sc60kg":
        return valor_kg / Decimal("60")
    if unidade == "arroba":
        return valor_kg / PESO_ARROBA_KG

    raise ValueError(f"Conversão de kg para '{unidade}' não suportada")


def _converter_bushel(
    valor: Decimal,
    de: str,
    para: str,
    produto: str | None,
) -> Decimal:
    if produto is None:
        raise ValueError("Produto é necessário para conversões com bushel")

    produto_norm = produto.lower()
    if produto_norm not in PESO_BUSHEL_KG:
        raise ValueError(f"Peso do bushel para '{produto}' não definido")

    peso_bu = PESO_BUSHEL_KG[produto_norm]

    if de in ("bu", "bushel"):
        valor_kg = valor * peso_bu
        return _de_kg(valor_kg, para)
    else:
        valor_kg = _para_kg(valor, de)
        return valor_kg / peso_bu


def sacas_para_toneladas(sacas: Decimal | float, peso_saca_kg: int = 60) -> Decimal:
    if not isinstance(sacas, Decimal):
        sacas = Decimal(str(sacas))
    return sacas * Decimal(str(peso_saca_kg)) / Decimal("1000")


def toneladas_para_sacas(toneladas: Decimal | float, peso_saca_kg: int = 60) -> Decimal:
    if not isinstance(toneladas, Decimal):
        toneladas = Decimal(str(toneladas))
    return toneladas * Decimal("1000") / Decimal(str(peso_saca_kg))


def preco_saca_para_tonelada(preco_saca: Decimal | float, peso_saca_kg: int = 60) -> Decimal:
    if not isinstance(preco_saca, Decimal):
        preco_saca = Decimal(str(preco_saca))
    sacas_por_ton = Decimal("1000") / Decimal(str(peso_saca_kg))
    return preco_saca * sacas_por_ton


def preco_tonelada_para_saca(preco_ton: Decimal | float, peso_saca_kg: int = 60) -> Decimal:
    if not isinstance(preco_ton, Decimal):
        preco_ton = Decimal(str(preco_ton))
    sacas_por_ton = Decimal("1000") / Decimal(str(peso_saca_kg))
    return preco_ton / sacas_por_ton
