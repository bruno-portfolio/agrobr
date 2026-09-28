from __future__ import annotations

import math
import re
from dataclasses import dataclass
from decimal import Decimal

from agrobr import constants


def integer_token(value: object, *, nullable: bool = False) -> int | None:
    if value == "" and nullable:
        return None
    if not isinstance(value, str) or not re.fullmatch(r"[0-9]+", value):
        raise ValueError("esperado inteiro ASCII literal não negativo")
    if len(value) > 19:
        raise ValueError("inteiro excede capacidade Int64")
    parsed = int(value)
    if parsed > 2**63 - 1:
        raise ValueError("inteiro excede capacidade Int64")
    return parsed


def money_token(value: object) -> Decimal | None:
    if value == "":
        return None
    if not isinstance(value, str) or len(value) > constants.COMEXSTAT_MAX_NUMBER_CHARS:
        raise ValueError("medida decimal excede limite lexical ou não é texto")
    if not re.fullmatch(r"-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?", value):
        raise ValueError("esperado decimal ASCII finito sem whitespace")
    exponent_token = re.split(r"[eE]", value)
    if len(exponent_token) == 2 and (
        len(exponent_token[1].lstrip("+-")) > 4
        or abs(int(exponent_token[1])) > constants.COMEXSTAT_MAX_NUMBER_EXPONENT
    ):
        raise ValueError("expoente decimal excede limite operacional")
    parsed = Decimal(value)
    exponent = parsed.as_tuple().exponent
    if not isinstance(exponent, int) or abs(exponent) > constants.COMEXSTAT_MAX_NUMBER_EXPONENT:
        raise ValueError("expoente decimal excede limite operacional")
    if parsed < 0:
        raise ValueError("medida negativa")
    binary64(parsed)
    return parsed


def binary64(value: Decimal | int | None) -> float:
    if value is None:
        return float("nan")
    result = float(value)
    if not math.isfinite(result) or (result == 0 and value != 0):
        raise ValueError("medida não representável em float64 finito sem underflow")
    return result


@dataclass(slots=True)
class ExactSum:
    coefficient: int = 0
    exponent: int = 0
    missing: int = 0
    count: int = 0
    zeros: int = 0
    negative_zero_only: bool = True

    def add(self, value: Decimal | int | None) -> None:
        self.count += 1
        if value is None:
            self.missing += 1
            return
        if isinstance(value, int):
            coefficient, exponent = value, 0
            negative_zero = False
        else:
            parts = value.as_tuple()
            coefficient = int("".join(map(str, parts.digits)))
            exponent = int(parts.exponent)
            negative_zero = bool(parts.sign) and coefficient == 0
        if coefficient == 0:
            self.zeros += 1
        self.negative_zero_only = self.negative_zero_only and negative_zero
        common = min(self.exponent, exponent)
        self.coefficient = self.coefficient * 10 ** (self.exponent - common) + coefficient * 10 ** (
            exponent - common
        )
        self.exponent = common

    def value(self) -> Decimal | None:
        if self.missing:
            return None
        sign = "-" if self.coefficient == 0 and self.negative_zero_only and self.count else ""
        return Decimal(f"{sign}{self.coefficient}e{self.exponent}")

    def integer(self) -> int | None:
        value = self.value()
        if value is None:
            return None
        result = int(value)
        if value != result or not 0 <= result <= 2**63 - 1:
            raise ValueError("soma excede capacidade Int64")
        return result

    def details(self) -> dict[str, int | str | None]:
        value = self.value()
        return {
            "count": self.count,
            "nulls": self.missing,
            "zeros": self.zeros,
            "exact_sum": str(value) if value is not None else None,
            "known_values_sum": f"{self.coefficient}e{self.exponent}",
        }
