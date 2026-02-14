from __future__ import annotations

import re

BASE_URL = (
    "https://www.gov.br/conab/pt-br/atuacao/informacoes-agropecuarias/safras/progresso-de-safra"
)

CULTURAS_PROGRESSO: dict[str, str] = {
    "algodao": "Algodão",
    "algodão": "Algodão",
    "arroz": "Arroz",
    "feijao_1": "Feijão 1ª",
    "feijão 1ª": "Feijão 1ª",
    "feijao 1a": "Feijão 1ª",
    "feijao": "Feijão 1ª",
    "milho_1": "Milho 1ª",
    "milho 1a": "Milho 1ª",
    "milho 1ª": "Milho 1ª",
    "milho_2": "Milho 2ª",
    "milho 2a": "Milho 2ª",
    "milho 2ª": "Milho 2ª",
    "soja": "Soja",
    "trigo": "Trigo",
}

CULTURAS_VALIDAS = {"Algodão", "Arroz", "Feijão 1ª", "Milho 1ª", "Milho 2ª", "Soja", "Trigo"}

TIPOS_OPERACAO = {"Semeadura", "Colheita"}

ESTADOS_PARA_UF: dict[str, str] = {
    "Acre": "AC",
    "Alagoas": "AL",
    "Amapá": "AP",
    "Amazonas": "AM",
    "Bahia": "BA",
    "Ceará": "CE",
    "Distrito Federal": "DF",
    "Espírito Santo": "ES",
    "Goiás": "GO",
    "Maranhão": "MA",
    "Mato Grosso": "MT",
    "Mato Grosso do Sul": "MS",
    "Minas Gerais": "MG",
    "Pará": "PA",
    "Paraíba": "PB",
    "Paraná": "PR",
    "Pernambuco": "PE",
    "Piauí": "PI",
    "Rio de Janeiro": "RJ",
    "Rio Grande do Norte": "RN",
    "Rio Grande do Sul": "RS",
    "Rondônia": "RO",
    "Roraima": "RR",
    "Santa Catarina": "SC",
    "São Paulo": "SP",
    "Sergipe": "SE",
    "Tocantins": "TO",
}

COLUNAS_SAIDA = [
    "cultura",
    "safra",
    "operacao",
    "estado",
    "semana_atual",
    "pct_ano_anterior",
    "pct_semana_anterior",
    "pct_semana_atual",
    "pct_media_5_anos",
]

_RE_CULTURA = re.compile(r"^(.+?)\s*-\s*Safra\s+(\d{4}/\d{2})$")
_RE_OPERACAO = re.compile(r"^(Semeadura|Colheita)\s*\*?\s*$")


def normalizar_cultura(cultura: str) -> str:
    key = cultura.strip().lower()
    return CULTURAS_PROGRESSO.get(key, cultura.strip())


def estado_para_uf(estado: str) -> str:
    cleaned = estado.strip().rstrip("\n")
    cleaned = re.sub(r"\s+", " ", cleaned)
    return ESTADOS_PARA_UF.get(cleaned, cleaned)


def parse_cultura_header(text: str) -> tuple[str, str] | None:
    m = _RE_CULTURA.match(text.strip())
    if m:
        return m.group(1).strip(), m.group(2).strip()
    return None


def parse_operacao_header(text: str) -> str | None:
    m = _RE_OPERACAO.match(text.strip())
    if m:
        return m.group(1)
    return None
