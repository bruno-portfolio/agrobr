"""
Utilitários para datas de safra agrícola.

No Brasil, a safra agrícola não coincide com o ano civil:
- Safra de verão (soja, milho 1ª): plantio Set-Dez, colheita Jan-Abr
- Safra de inverno (milho 2ª, trigo): plantio Fev-Mar, colheita Jun-Set

Notação: "2024/25" significa plantio em 2024 e colheita em 2025.
"""

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
    """
    Retorna a safra agrícola atual no formato '2024/25'.

    A safra é determinada pelo mês:
    - Jul a Dez: safra do ano atual / próximo ano
    - Jan a Jun: safra do ano anterior / ano atual

    Args:
        data: Data de referência (default: hoje)

    Returns:
        String no formato '2024/25'

    Examples:
        >>> safra_atual(date(2024, 10, 15))
        '2024/25'
        >>> safra_atual(date(2025, 3, 15))
        '2024/25'
    """
    if data is None:
        data = date.today()

    ano_inicio = data.year if data.month >= INICIO_SAFRA_MES else data.year - 1

    ano_fim = ano_inicio + 1
    return f"{ano_inicio}/{str(ano_fim)[-2:]}"


def validar_safra(safra: str) -> bool:
    """
    Valida se string está no formato de safra válido.

    Formatos aceitos:
    - '2024/25' (padrão)
    - '24/25' (curto)
    - '2024/2025' (completo)

    Args:
        safra: String a validar

    Returns:
        True se válido, False caso contrário
    """
    if REGEX_SAFRA_COMPLETA.match(safra):
        return True
    if REGEX_SAFRA_CURTA.match(safra):
        return True
    return bool(REGEX_SAFRA_BARRA.match(safra))


def normalizar_safra(safra: str) -> str:
    """
    Normaliza safra para formato padrão '2024/25'.

    Args:
        safra: Safra em qualquer formato aceito

    Returns:
        Safra no formato '2024/25'

    Raises:
        ValueError: Se formato inválido

    Examples:
        >>> normalizar_safra('24/25')
        '2024/25'
        >>> normalizar_safra('2024/2025')
        '2024/25'
    """
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
        ano_inicio = match_barra.group(1)
        ano_fim = match_barra.group(2)[-2:]
        return f"{ano_inicio}/{ano_fim}"

    raise ValueError(f"Formato de safra inválido: '{safra}'")


def safra_para_anos(safra: str) -> tuple[int, int]:
    """
    Converte safra para anos de início e fim.

    Args:
        safra: Safra no formato '2024/25'

    Returns:
        Tupla (ano_inicio, ano_fim)

    Examples:
        >>> safra_para_anos('2024/25')
        (2024, 2025)
    """
    safra_norm = normalizar_safra(safra)
    match = REGEX_SAFRA_COMPLETA.match(safra_norm)

    ano_inicio = int(match.group(1))
    ano_fim_curto = int(match.group(2))

    seculo = (ano_inicio // 100) * 100
    ano_fim = seculo + ano_fim_curto

    if ano_fim < ano_inicio:
        ano_fim += 100

    return ano_inicio, ano_fim


def anos_para_safra(ano_inicio: int, ano_fim: int | None = None) -> str:
    """
    Converte anos para formato de safra.

    Args:
        ano_inicio: Ano de início (plantio)
        ano_fim: Ano de fim (colheita). Se None, assume ano_inicio + 1

    Returns:
        Safra no formato '2024/25'

    Examples:
        >>> anos_para_safra(2024)
        '2024/25'
        >>> anos_para_safra(2024, 2025)
        '2024/25'
    """
    if ano_fim is None:
        ano_fim = ano_inicio + 1

    return f"{ano_inicio}/{str(ano_fim)[-2:]}"


def safra_anterior(safra: str, n: int = 1) -> str:
    """
    Retorna a safra N anos antes.

    Args:
        safra: Safra de referência
        n: Número de safras anteriores (default: 1)

    Returns:
        Safra anterior

    Examples:
        >>> safra_anterior('2024/25')
        '2023/24'
        >>> safra_anterior('2024/25', 3)
        '2021/22'
    """
    ano_inicio, _ = safra_para_anos(safra)
    return anos_para_safra(ano_inicio - n)


def safra_posterior(safra: str, n: int = 1) -> str:
    """
    Retorna a safra N anos depois.

    Args:
        safra: Safra de referência
        n: Número de safras posteriores (default: 1)

    Returns:
        Safra posterior

    Examples:
        >>> safra_posterior('2024/25')
        '2025/26'
    """
    ano_inicio, _ = safra_para_anos(safra)
    return anos_para_safra(ano_inicio + n)


def lista_safras(inicio: str, fim: str) -> list[str]:
    """
    Gera lista de safras entre início e fim (inclusive).

    Args:
        inicio: Safra inicial
        fim: Safra final

    Returns:
        Lista de safras

    Examples:
        >>> lista_safras('2020/21', '2024/25')
        ['2020/21', '2021/22', '2022/23', '2023/24', '2024/25']
    """
    ano_inicio, _ = safra_para_anos(inicio)
    ano_fim, _ = safra_para_anos(fim)

    return [anos_para_safra(ano) for ano in range(ano_inicio, ano_fim + 1)]


def data_para_safra(data: date) -> str:
    """
    Determina a safra de uma data.

    Args:
        data: Data

    Returns:
        Safra correspondente
    """
    return safra_atual(data)


def periodo_safra(safra: str) -> tuple[date, date]:
    """
    Retorna período aproximado da safra (Jul a Jun).

    Args:
        safra: Safra no formato '2024/25'

    Returns:
        Tupla (data_inicio, data_fim)
    """
    ano_inicio, ano_fim = safra_para_anos(safra)

    data_inicio = date(ano_inicio, INICIO_SAFRA_MES, 1)
    data_fim = date(ano_fim, 6, 30)

    return data_inicio, data_fim


def mes_para_numero(mes: str | MesSafra) -> int:
    """
    Converte nome do mês para número.

    Args:
        mes: Nome do mês (pt-BR, 3 letras)

    Returns:
        Número do mês (1-12)
    """
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
    """
    Converte número do mês para nome.

    Args:
        numero: Número do mês (1-12)

    Returns:
        Nome do mês (3 letras)
    """
    meses = ["jan", "fev", "mar", "abr", "mai", "jun", "jul", "ago", "set", "out", "nov", "dez"]
    return meses[numero - 1]
