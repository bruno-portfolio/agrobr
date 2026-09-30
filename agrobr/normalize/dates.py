from __future__ import annotations

import re
import warnings
from datetime import date
from typing import NamedTuple

import pandas as pd

from agrobr import _log
from agrobr.exceptions import InvalidParameterError
from agrobr.utils.result import ATRIBUTO_AVISOS
from agrobr.utils.time import hoje

logger = _log.get_logger(__name__)

DATA_ANO_MINIMO = 1900
DATA_ANO_MAXIMO = 2099


class DatasConvertidas(NamedTuple):
    datas: pd.Series
    descartadas: int


def converter_datas(
    valores: pd.Series,
    *,
    fonte: str,
    formato: str | None = None,
    dayfirst: bool = False,
    ate: pd.Timestamp | None = None,
) -> DatasConvertidas:
    """Converte a coluna em datas com o mesmo resultado no pandas 2 e no 3.

    Valor ilegível, com ano fora de `DATA_ANO_MINIMO`–`DATA_ANO_MAXIMO` ou, com `ate`, de dia
    posterior a ele vira `NaT`. `descartadas` conta os valores presentes que viraram `NaT`
    (vazio não conta). A saída é sempre em nanossegundos.
    """
    datas = pd.to_datetime(valores, errors="coerce", format=formato, dayfirst=dayfirst)
    fora = ~datas.dt.year.between(DATA_ANO_MINIMO, DATA_ANO_MAXIMO)
    if ate is not None:
        fora |= datas.dt.normalize() > ate.normalize()
    datas = datas.mask(fora).dt.as_unit("ns")
    texto = valores.astype("string").str.strip()
    descartadas = int((texto.notna() & texto.ne("") & datas.isna()).sum())
    if descartadas:
        logger.warning(
            "datas_descartadas",
            fonte=fonte,
            coluna=str(valores.name),
            descartadas=descartadas,
            intervalo=f"{DATA_ANO_MINIMO}-{DATA_ANO_MAXIMO}",
        )
    return DatasConvertidas(datas, descartadas)


def converter_coluna(
    df: pd.DataFrame,
    coluna: str,
    *,
    fonte: str,
    formato: str | None = None,
    dayfirst: bool = False,
    ate: pd.Timestamp | None = None,
    efeito: str = "",
) -> None:
    """Converte `df[coluna]` com `converter_datas`; havendo descarte, emite `UserWarning` e
    anota a mensagem em `df.attrs`, de onde o `build_source_meta` a leva ao `MetaInfo`."""
    convertidas = converter_datas(
        df[coluna], fonte=fonte, formato=formato, dayfirst=dayfirst, ate=ate
    )
    df[coluna] = convertidas.datas
    if not convertidas.descartadas:
        return
    limite = f" ou de dia posterior a {ate:%Y-%m-%d}" if ate is not None else ""
    aviso = (
        f"{fonte}: {convertidas.descartadas} valor(es) de {coluna} viraram NaT (data ilegível "
        f"ou com ano fora de {DATA_ANO_MINIMO}–{DATA_ANO_MAXIMO}{limite}).{efeito}"
    )
    df.attrs.setdefault(ATRIBUTO_AVISOS, []).append(aviso)
    warnings.warn(aviso, UserWarning, stacklevel=2)


MESES_PT: dict[str, int] = {
    "janeiro": 1,
    "fevereiro": 2,
    "março": 3,
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


def month_to_number(text: str) -> int | None:
    return MESES_PT.get(text.strip().lower())


REGEX_SAFRA_COMPLETA = re.compile(r"^(\d{4})/(\d{2})$")
REGEX_SAFRA_CURTA = re.compile(r"^(\d{2})/(\d{2})$")
REGEX_SAFRA_BARRA = re.compile(r"^(\d{4})/(\d{4})$")

INICIO_SAFRA_MES = 7
_FORMATO_SAFRA_INVALIDO = "Formato de safra inválido (aceitos: 2024/25, 2024/2025, 24/25)"


def safra_atual(data: date | None = None) -> str:
    if data is None:
        data = hoje()

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
    if not isinstance(safra, str):
        raise InvalidParameterError(f"{_FORMATO_SAFRA_INVALIDO}: {safra!r}")
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

    raise InvalidParameterError(f"{_FORMATO_SAFRA_INVALIDO}: {safra!r}")


def safra_para_anos(safra: str) -> tuple[int, int]:
    safra_norm = normalizar_safra(safra)
    match = REGEX_SAFRA_COMPLETA.match(safra_norm)

    if match is None:
        raise InvalidParameterError(f"{_FORMATO_SAFRA_INVALIDO}: {safra!r}")

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
    if ano_fim != ano_inicio + 1:
        raise InvalidParameterError(
            f"Safra cobre dois anos consecutivos: ano_fim deve ser {ano_inicio + 1}, recebeu {ano_fim}"
        )

    return f"{ano_inicio}/{str(ano_fim)[-2:]}"


def safra_anterior(safra: str, n: int = 1) -> str:
    ano_inicio, _ = safra_para_anos(safra)
    return anos_para_safra(ano_inicio - n)


def safra_posterior(safra: str, n: int = 1) -> str:
    ano_inicio, _ = safra_para_anos(safra)
    return anos_para_safra(ano_inicio + n)


def lista_safras(safra_inicio: str, safra_fim: str) -> list[str]:
    ano_inicio, _ = safra_para_anos(safra_inicio)
    ano_fim, _ = safra_para_anos(safra_fim)
    if ano_inicio > ano_fim:
        raise InvalidParameterError(
            f"safra_inicio ({safra_inicio!r}) posterior a safra_fim ({safra_fim!r})"
        )

    return [anos_para_safra(ano) for ano in range(ano_inicio, ano_fim + 1)]


def periodo_safra(safra: str) -> tuple[date, date]:
    ano_inicio, ano_fim = safra_para_anos(safra)

    data_inicio = date(ano_inicio, INICIO_SAFRA_MES, 1)
    data_fim = date(ano_fim, 6, 30)

    return data_inicio, data_fim
