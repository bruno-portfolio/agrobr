from __future__ import annotations

B3_CONTRATOS_AGRO: dict[str, str] = {
    "boi": "BGI",
    "milho": "CCM",
    "cafe_arabica": "ICF",
    "cafe_conillon": "CNL",
    "etanol": "ETH",
    "soja_cross": "SJC",
    "soja_fob": "SOY",
}

TICKERS_AGRO: set[str] = set(B3_CONTRATOS_AGRO.values())

TICKER_PARA_CONTRATO: dict[str, str] = {v: k for k, v in B3_CONTRATOS_AGRO.items()}

MONTH_CODES: dict[str, int] = {
    "F": 1,
    "G": 2,
    "H": 3,
    "J": 4,
    "K": 5,
    "M": 6,
    "N": 7,
    "Q": 8,
    "U": 9,
    "V": 10,
    "X": 11,
    "Z": 12,
}

UNIDADES: dict[str, str] = {
    "BGI": "BRL/@",
    "CCM": "BRL/sc60kg",
    "ICF": "USD/sc60kg",
    "CNL": "USD/ton",
    "ETH": "BRL/m3",
    "SJC": "USD/sc60kg",
    "SOY": "USD/ton",
}

COLUNAS_SAIDA: list[str] = [
    "data",
    "ticker",
    "descricao",
    "vencimento_codigo",
    "vencimento_mes",
    "vencimento_ano",
    "ajuste_anterior",
    "ajuste_atual",
    "variacao",
    "ajuste_por_contrato",
    "unidade",
]

B3_CONTRATOS_AGRO_INV: dict[str, str] = {v: k for k, v in B3_CONTRATOS_AGRO.items()}

TICKERS_AGRO_OI: set[str] = {"BGI", "CCM", "ETH", "ICF", "SJC", "CNL"}

COLUNAS_OI_SAIDA: list[str] = [
    "data",
    "ticker",
    "descricao",
    "ticker_completo",
    "vencimento_codigo",
    "vencimento_mes",
    "vencimento_ano",
    "tipo",
    "posicoes_abertas",
    "variacao_posicoes",
    "unidade",
]


def parse_vencimento(codigo: str) -> tuple[int, int]:
    codigo = codigo.strip()
    letter = codigo[0].upper()
    year_suffix = int(codigo[1:])
    year = 2000 + year_suffix if year_suffix < 100 else year_suffix
    month = MONTH_CODES[letter]
    return year, month


def parse_numero_br(texto: str) -> float | None:
    texto = texto.strip()
    if not texto or texto == "-":
        return None
    texto = texto.replace(".", "").replace(",", ".")
    return float(texto)
