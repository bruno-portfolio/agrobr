from __future__ import annotations

import re

from agrobr.exceptions import InvalidParameterError
from agrobr.normalize.crops import normalizar_cultura

PARSER_VERSION: int = 2

SAFRA_RE = re.compile(r"^\d{4}/\d{4}$")

PRODUTOS_QUINZENAL: dict[str, tuple[int, str]] = {
    "cana": (3, "t"),
    "acucar": (4, "t"),
    "etanol_total": (5, "m3"),
    "etanol_anidro": (6, "m3"),
    "etanol_hidratado": (7, "m3"),
}

PRODUTOS_HISTORICO: list[str] = [
    "cana",
    "acucar",
    "etanol_anidro",
    "etanol_hidratado",
    "etanol_total",
]

REGIOES_QUINZENAL: list[str] = ["sao_paulo", "centro_sul", "demais_estados"]

RESUMO_LABELS: dict[str, tuple[str, str]] = {
    "cana-de-acucar": ("cana", "mil_t"),
    "acucar": ("acucar", "mil_t"),
    "etanol anidro": ("etanol_anidro", "mi_litros"),
    "etanol hidratado": ("etanol_hidratado", "mi_litros"),
    "etanol total": ("etanol_total", "mi_litros"),
    "atr": ("atr", "mil_t"),
    "atr/ tonelada de cana": ("atr_por_tonelada", "kg_t"),
    "litros etanol/ tonelada de cana": ("litros_etanol_por_tonelada", "l_t"),
    "kg acucar/ tonelada de cana": ("kg_acucar_por_tonelada", "kg_t"),
}

MIX_LABELS: dict[str, str] = {
    "acucar": "mix_acucar",
    "etanol": "mix_etanol",
}

UNIDADES_HISTORICO: dict[str, str] = {
    "mil toneladas": "mil_t",
    "mil m3": "mil_m3",
}

AGREGADOS_HISTORICO: dict[str, str] = {
    "regiao centro-sul": "centro_sul",
    "regiao norte-nordeste": "norte_nordeste",
    "brasil": "brasil",
}

UFS_FORM: list[str] = [
    "RS",
    "SC",
    "PR",
    "SP",
    "RJ",
    "MG",
    "ES",
    "MS",
    "MT",
    "GO",
    "DF",
    "BA",
    "SE",
    "AL",
    "PE",
    "PB",
    "RN",
    "CE",
    "PI",
    "MA",
    "TO",
    "PA",
    "AP",
    "RO",
    "AM",
    "AC",
    "RR",
]

IDTABELA_HISTORICO_PRODUTO = "2494"

PERIODOS_RESUMO: tuple[str, ...] = ("acumulado", "quinzena", "mensal")

MESES: dict[str, int] = {
    "janeiro": 1,
    "fevereiro": 2,
    "marco": 3,
    "abril": 4,
    "maio": 5,
    "junho": 6,
    "julho": 7,
    "agosto": 8,
    "setembro": 9,
    "outubro": 10,
    "novembro": 11,
    "dezembro": 12,
}

SAFRA_HISTORICO_MIN = "1980/1981"
SAFRA_HISTORICO_MAX = "2020/2021"

COLUNAS_SERIES: list[str] = [
    "data",
    "quinzena",
    "safra",
    "produto",
    "regiao",
    "valor",
    "valor_safra_anterior",
    "variacao_pct",
    "unidade",
]

COLUNAS_RESUMO: list[str] = [
    "produto",
    "regiao",
    "safra",
    "periodo",
    "data_inicio",
    "data_fim",
    "valor",
    "valor_safra_anterior",
    "variacao_pct",
    "unidade",
]

COLUNAS_HISTORICO: list[str] = ["safra", "localidade", "produto", "valor", "unidade"]

SANIDADE_MAX_SERIES: dict[str, float] = {
    "cana": 700_000_000,
    "acucar": 50_000_000,
    "etanol_total": 40_000_000,
    "etanol_anidro": 40_000_000,
    "etanol_hidratado": 40_000_000,
}

SANIDADE_MAX_HISTORICO: dict[str, float] = {
    "cana": 700_000,
    "acucar": 50_000,
    "etanol_anidro": 40_000,
    "etanol_hidratado": 40_000,
    "etanol_total": 40_000,
}


def resolve_produto(nome: str, validos: dict[str, tuple[int, str]] | list[str]) -> str:
    if not isinstance(nome, str):
        raise InvalidParameterError("produto deve ser uma string")
    canonico = normalizar_cultura(nome)
    if canonico not in validos:
        raise InvalidParameterError(
            f"Produto '{nome}' inválido para UNICA. Opções: {sorted(validos)}"
        )
    return canonico
