from __future__ import annotations

import re
from datetime import date
from typing import Literal

MesSafra = Literal[
    "jan", "fev", "mar", "abr", "mai", "jun", "jul", "ago", "set", "out", "nov", "dez"
]

REGEX_SAFRA_COMPLETA = re.compile(r"^(\d{4})/(\d{2})$")
REGEX_SAFRA_CURTA = re.compile(r"^(\d{2})/(\d{2})$")
REGEX_SAFRA_BARRA = re.compile(r"^(\d{4})/(\d{4})$")

INICIO_SAFRA_MES = 7


def safra_atual(data: date | None = None) -> str:
    if data is None:
        data = date.today()

    ano_inicio = data.year if data.month >= INICIO_SAFRA_MES else data.year - 1

    ano_fim = ano_inicio + 1
    return f"{ano_inicio}/{str(ano_fim)[-2:]}"


def validar_safra(safra: str) -> bool:
    if REGEX_SAFRA_COMPLETA.match(safra):
        return True
    if REGEX_SAFRA_CURTA.match(safra):
        return True
    return bool(REGEX_SAFRA_BARRA.match(safra))


def normalizar_safra(safra: str) -> str:
    safra = re.sub(r"\s*/\s*", "/", safra.strip())

    match_completa = REGEX_SAFRA_COMPLETA.match(safra)
    if match_completa:
        return safra

    match_curta = REGEX_SAFRA_CURTA.match(safra)
    if match_curta:
        ano_inicio = int(match_curta.group(1))
        ano_fim = match_curta.group(2)
        ano_inicio = 1900 + ano_inicio if ano_inicio >= 50 else 2000 + ano_inicio
        return f"{ano_inicio}/{ano_fim}"

    match_barra = REGEX_SAFRA_BARRA.match(safra)
    if match_barra:
        ano_inicio_str = match_barra.group(1)
        ano_fim_str = match_barra.group(2)[-2:]
        return f"{ano_inicio_str}/{ano_fim_str}"

    raise ValueError(f"Formato de safra inválido: '{safra}'")


def safra_para_anos(safra: str) -> tuple[int, int]:
    safra_norm = normalizar_safra(safra)
    match = REGEX_SAFRA_COMPLETA.match(safra_norm)

    if match is None:
        raise ValueError(f"Formato de safra inválido: '{safra}'")

    ano_inicio = int(match.group(1))
    ano_fim_curto = int(match.group(2))

    seculo = (ano_inicio // 100) * 100
    ano_fim = seculo + ano_fim_curto

    if ano_fim < ano_inicio:
        ano_fim += 100

    return ano_inicio, ano_fim


def anos_para_safra(ano_inicio: int, ano_fim: int | None = None) -> str:
    if ano_fim is None:
        ano_fim = ano_inicio + 1

    return f"{ano_inicio}/{str(ano_fim)[-2:]}"


def safra_anterior(safra: str, n: int = 1) -> str:
    ano_inicio, _ = safra_para_anos(safra)
    return anos_para_safra(ano_inicio - n)


def safra_posterior(safra: str, n: int = 1) -> str:
    ano_inicio, _ = safra_para_anos(safra)
    return anos_para_safra(ano_inicio + n)


def lista_safras(inicio: str, fim: str) -> list[str]:
    ano_inicio, _ = safra_para_anos(inicio)
    ano_fim, _ = safra_para_anos(fim)

    return [anos_para_safra(ano) for ano in range(ano_inicio, ano_fim + 1)]


def data_para_safra(data: date) -> str:
    return safra_atual(data)


def periodo_safra(safra: str) -> tuple[date, date]:
    ano_inicio, ano_fim = safra_para_anos(safra)

    data_inicio = date(ano_inicio, INICIO_SAFRA_MES, 1)
    data_fim = date(ano_fim, 6, 30)

    return data_inicio, data_fim


def mes_para_numero(mes: str | MesSafra) -> int:
    meses = {
        "jan": 1,
        "fev": 2,
        "mar": 3,
        "abr": 4,
        "mai": 5,
        "jun": 6,
        "jul": 7,
        "ago": 8,
        "set": 9,
        "out": 10,
        "nov": 11,
        "dez": 12,
    }
    return meses[mes.lower()[:3]]


def numero_para_mes(numero: int) -> str:
    meses = ["jan", "fev", "mar", "abr", "mai", "jun", "jul", "ago", "set", "out", "nov", "dez"]
    return meses[numero - 1]
