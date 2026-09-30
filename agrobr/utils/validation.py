from __future__ import annotations

import re
from datetime import date, datetime
from typing import overload

from agrobr.exceptions import InvalidParameterError
from agrobr.normalize import dates
from agrobr.utils import time as time_utils

_FORMATOS_DATA = {
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}": "%Y-%m-%d",
    r"[0-9]{2}/[0-9]{2}/[0-9]{4}": "%d/%m/%Y",
}


def validate_safra(safra: str | None) -> str | None:
    if safra is None:
        return None
    if not isinstance(safra, str):
        raise InvalidParameterError("safra deve ser uma string com anos consecutivos")
    text = re.sub(r"\s*/\s*", "/", safra.strip())
    complete = re.fullmatch(r"(\d{4})/(\d{4})", text)
    if complete and int(complete[2]) != int(complete[1]) + 1:
        raise InvalidParameterError(f"Safra deve conter anos consecutivos: {safra!r}")
    try:
        normalized = dates.normalizar_safra(text)
        first, last = dates.safra_para_anos(normalized)
    except ValueError as exc:
        raise InvalidParameterError(str(exc)) from exc
    if last != first + 1:
        raise InvalidParameterError(f"Safra deve conter anos consecutivos: {safra!r}")
    return normalized


@overload
def parse_data(valor: None, nome: str = ...) -> None: ...
@overload
def parse_data(valor: str | date | datetime, nome: str = ...) -> date: ...
def parse_data(valor: str | date | datetime | None, nome: str = "data") -> date | None:
    """Converte data de entrada do usuário para `date`.

    Aceita `date`, `datetime` (a hora é descartada) e texto em `AAAA-MM-DD` ou `DD/MM/AAAA`.
    Outro formato nunca é adivinhado: `01/02/2024` é sempre 1º de fevereiro.

    Raises:
        InvalidParameterError: tipo ou formato fora dos aceitos, ou data inexistente.
    """
    if valor is None:
        return None
    if isinstance(valor, datetime):
        return valor.date()
    if isinstance(valor, date):
        return valor
    texto = valor.strip() if isinstance(valor, str) else ""
    formato = next((f for padrao, f in _FORMATOS_DATA.items() if re.fullmatch(padrao, texto)), None)
    if formato is None:
        raise InvalidParameterError(
            f"{nome} deve ser date, datetime ou texto AAAA-MM-DD ou DD/MM/AAAA: {valor!r}"
        )
    try:
        return datetime.strptime(texto, formato).date()
    except ValueError:
        raise InvalidParameterError(f"{nome} contém data inexistente: {valor!r}") from None


def _uf_valida(uf: object, validas: frozenset[str]) -> str:
    if not isinstance(uf, str) or uf.strip().upper() not in validas:
        raise InvalidParameterError(
            f"UF inválida: {uf!r}. Valores válidos: {', '.join(sorted(validas))}"
        )
    return uf.strip().upper()


def validate_uf(uf: str | None) -> str | None:
    if uf is None:
        return None
    from agrobr.normalize.regions import UFS_VALIDAS

    return _uf_valida(uf, UFS_VALIDAS)


def validate_bioma(bioma: str | None) -> str | None:
    if bioma is None:
        return None

    from agrobr.normalize.regions import BIOMAS_VALIDOS, normalizar_bioma

    validos = ", ".join(sorted(BIOMAS_VALIDOS))
    if not isinstance(bioma, str):
        raise InvalidParameterError(f"Bioma inválido: {bioma!r}. Valores válidos: {validos}")
    bioma_normalizado = normalizar_bioma(bioma)
    if bioma_normalizado not in BIOMAS_VALIDOS:
        raise InvalidParameterError(f"Bioma inválido: {bioma!r}. Valores válidos: {validos}")
    return bioma_normalizado


def validate_year_uf(
    *,
    uf: str | None = None,
    ano: int | None = None,
    ano_inicio: int | None = None,
    ano_fim: int | None = None,
    ufs_validas: frozenset[str] | None = None,
    ano_min: int = 2000,
) -> None:
    if ufs_validas is None:
        from agrobr.normalize.regions import UFS_VALIDAS

        ufs_validas = UFS_VALIDAS

    if uf is not None:
        _uf_valida(uf, ufs_validas)

    current_year = time_utils.hoje().year
    if ano is not None and (ano < ano_min or ano > current_year):
        raise InvalidParameterError(
            f"Ano {ano} fora do intervalo válido ({ano_min}-{current_year})"
        )
    if ano_inicio is not None and ano_inicio < ano_min:
        raise InvalidParameterError(f"ano_inicio {ano_inicio} anterior a {ano_min}")
    if ano_fim is not None and ano_fim > current_year:
        raise InvalidParameterError(f"ano_fim {ano_fim} posterior ao ano atual ({current_year})")
    if ano_inicio is not None and ano_fim is not None and ano_inicio > ano_fim:
        raise InvalidParameterError(f"ano_inicio ({ano_inicio}) > ano_fim ({ano_fim})")
