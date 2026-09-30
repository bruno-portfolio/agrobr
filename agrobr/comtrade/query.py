from __future__ import annotations

import hashlib
import json
import re
from typing import Literal, cast

import pydantic

from agrobr import constants
from agrobr.exceptions import InvalidParameterError
from agrobr.utils import time as time_utils

from . import acquisition


def validate_access_options(api_key: str | None, require_complete: bool) -> None:
    if not isinstance(require_complete, bool):
        raise InvalidParameterError("require_complete deve ser booleano")
    if api_key is not None and (not isinstance(api_key, str) or not api_key.strip()):
        raise InvalidParameterError("api_key deve ser texto não vazio ou None")


def _year(value: str) -> int:
    corrente = time_utils.hoje().year
    if re.fullmatch(r"[0-9]{4}", value) is None or not 1962 <= int(value) <= corrente:
        raise InvalidParameterError(f"Ano deve conter quatro dígitos entre 1962 e {corrente}")
    return int(value)


def _month(value: str) -> int:
    if re.fullmatch(r"[0-9]{6}", value) is None:
        raise InvalidParameterError("Mês deve ter formato YYYYMM")
    year = _year(value[:4])
    month = int(value[4:])
    if not 1 <= month <= 12:
        raise InvalidParameterError("Mês deve estar entre 01 e 12")
    return year * 12 + month - 1


def _monthly_range(start: int, end: int) -> list[str]:
    if start > end:
        raise InvalidParameterError("Intervalo de períodos invertido")
    return [f"{index // 12:04d}{index % 12 + 1:02d}" for index in range(start, end + 1)]


def expand_periods(period: str | int, freq: str) -> list[str]:
    if isinstance(period, bool) or not isinstance(period, (str, int)):
        raise InvalidParameterError("Período deve ser ano, mês, lista ou intervalo textual")
    text = str(period).strip()
    if "," in text:
        tokens = [part.strip() for part in text.split(",")]
        width = 4 if freq == "A" else 6
        if any(len(token) != width for token in tokens):
            raise InvalidParameterError("Lista de períodos deve ser homogênea")
        periods = []
        for token in tokens:
            periods.extend(expand_periods(token, freq))
        return sorted(set(periods))
    if "-" in text:
        ends = [part.strip() for part in text.split("-")]
        if len(ends) != 2 or len(ends[0]) != len(ends[1]):
            raise InvalidParameterError("Intervalo de períodos inválido")
        first, last = ends
        if freq == "M" and len(first) == 6:
            return _monthly_range(_month(first), _month(last))
        start, end = _year(first), _year(last)
        if start > end:
            raise InvalidParameterError("Intervalo de períodos invertido")
        if freq == "M":
            return _monthly_range(start * 12, end * 12 + 11)
        return [f"{year:04d}" for year in range(start, end + 1)]
    if freq == "M":
        if len(text) == 4:
            year = _year(text)
            return _monthly_range(year * 12, year * 12 + 11)
        _month(text)
    else:
        _year(text)
    return [text]


def build_query(
    *,
    reporter: int,
    partner: int | None,
    hs_codes: list[str],
    flow: str,
    period: str | int,
    freq: str = "A",
) -> acquisition.TradeQuery:
    if not isinstance(freq, str) or freq.strip().upper() not in {"A", "M"}:
        raise InvalidParameterError("freq deve ser A ou M")
    if not isinstance(flow, str) or flow.strip().upper() not in {"X", "M"}:
        raise InvalidParameterError("flow deve ser X ou M")
    if (
        not isinstance(hs_codes, list)
        or not hs_codes
        or any(not isinstance(code, str) for code in hs_codes)
    ):
        raise InvalidParameterError("hs_codes deve ser lista de códigos textuais")
    frequency = freq.strip().upper()
    periods = expand_periods(period, frequency)
    try:
        return acquisition.TradeQuery(
            reporter=reporter,
            partner=partner,
            hs_codes=sorted({code.strip() for code in hs_codes}),
            flow=cast(Literal["X", "M"], flow.strip().upper()),
            freq=cast(Literal["A", "M"], frequency),
            periods=periods,
            requested_period=str(period),
        )
    except pydantic.ValidationError as exc:
        fields = sorted({str(error["loc"][0]) for error in exc.errors() if error["loc"]})
        raise InvalidParameterError(f"Consulta Comtrade inválida: {fields}") from None


def make_partition(
    periods: list[str],
    hs_codes: list[str],
    parent_id: str | None = None,
) -> acquisition.TradePartition:
    digest = hashlib.sha256(
        json.dumps([periods, hs_codes], separators=(",", ":")).encode()
    ).hexdigest()
    return acquisition.TradePartition(
        partition_id=digest, parent_id=parent_id, periods=periods, hs_codes=hs_codes
    )


def plan_partitions(
    query: acquisition.TradeQuery,
    access: Literal["guest", "authenticated"] = "guest",
) -> list[acquisition.TradePartition]:
    limit = (
        constants.COMTRADE_GUEST_MAX_PERIODS
        if access == "guest"
        else constants.COMTRADE_AUTH_MAX_PERIODS
    )
    return [
        make_partition(query.periods[index : index + limit], query.hs_codes)
        for index in range(0, len(query.periods), limit)
    ]


def split_partition(partition: acquisition.TradePartition) -> list[acquisition.TradePartition]:
    if len(partition.periods) > 1:
        return [
            make_partition([period], partition.hs_codes, partition.partition_id)
            for period in partition.periods
        ]
    if len(partition.hs_codes) > 1:
        return [
            make_partition(partition.periods, [code], partition.partition_id)
            for code in partition.hs_codes
        ]
    return []
