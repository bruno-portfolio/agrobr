from __future__ import annotations

import json
import math
import re
from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

from agrobr import constants


@dataclass(frozen=True, slots=True)
class Number:
    lexeme: str


class RawNumber(str):
    pass


def _lexeme(value: Any) -> str:
    if isinstance(value, Number):
        return value.lexeme
    if isinstance(value, RawNumber) or type(value) in (int, float):
        return str(value)
    raise ValueError("Número JSON obrigatório")


def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for name, value in items:
        if name in result:
            raise ValueError("Chave JSON repetida")
        result[name] = value
    return result


def reject_constant(value: str) -> Any:
    raise ValueError(f"Constante JSON inválida: {value}")


def decode(
    content: bytes,
    *,
    encoding: str | None = "utf-8-sig",
    number_type: Callable[[str], Any] = Number,
) -> Any:
    return json.loads(
        content.decode(encoding) if encoding else content,
        parse_int=number_type,
        parse_float=number_type,
        parse_constant=reject_constant,
        object_pairs_hook=pairs,
    )


def decimal(value: Any) -> Decimal:
    lexeme = _lexeme(value)
    if re.fullmatch(constants.JSON_NUMBER_PATTERN, lexeme) is None:
        raise ValueError("Lexema JSON numérico inválido")
    try:
        number = Decimal(lexeme)
    except InvalidOperation as exc:
        raise ValueError("Expoente decimal não representável") from exc
    if not number.is_finite():
        raise ValueError("Número não finito")
    return number


def integer(value: Any, bits: int | None = 64, *, token_only: bool = False) -> int:
    if token_only and (
        not isinstance(value, (Number, RawNumber))
        and type(value) is not int
        or isinstance(value, (Number, RawNumber))
        and any(character in _lexeme(value) for character in ".eE")
    ):
        raise ValueError("Token JSON inteiro obrigatório")
    number = decimal(value)
    if bits is not None and not -(2 ** (bits - 1)) <= number < 2 ** (bits - 1):
        raise ValueError("Inteiro fora do domínio XSD")
    if number != number.to_integral_value():
        raise ValueError("Fração incompatível com inteiro XSD")
    return int(number)


def count(value: Any, bits: int | None = 64) -> int:
    lexeme = _lexeme(value)
    if re.fullmatch(constants.JSON_COUNT_PATTERN, lexeme) is None:
        raise ValueError("Contagem exige token inteiro não negativo")
    return integer(value, bits)


def floating(value: Any) -> float:
    exact = decimal(value)
    result = float(exact)
    if not math.isfinite(result) or (result == 0 and exact != 0):
        raise ValueError("Número não representável em float64")
    return result


def numeric_identifier(value: Any) -> str:
    if not isinstance(value, (Number, RawNumber)) and type(value) is not int:
        raise ValueError("Identificador JSON numérico obrigatório")
    lexeme = _lexeme(value)
    if re.fullmatch(constants.JSON_NUMBER_PATTERN, lexeme) is None:
        raise ValueError("Identificador numérico inválido")
    return lexeme


def canonical(value: Any, *, preserve_signed_zero: bool = True) -> Any:
    if isinstance(value, (Number, RawNumber)):
        number = decimal(value)
        sign, digits, exponent = number.as_tuple()
        if not number:
            return ["number", sign if preserve_signed_zero else 0, 0]
        normalized = list(digits)
        while normalized[-1] == 0:
            normalized.pop()
            exponent = int(exponent) + 1
        return ["number", sign, normalized, exponent]
    if isinstance(value, dict):
        return [
            "object",
            [
                [name, canonical(item, preserve_signed_zero=preserve_signed_zero)]
                for name, item in sorted(value.items())
            ],
        ]
    if isinstance(value, list):
        return [
            "array",
            [canonical(item, preserve_signed_zero=preserve_signed_zero) for item in value],
        ]
    return [type(value).__name__, value]


def plain_numbers(value: Any) -> Any:
    if isinstance(value, (Number, RawNumber)):
        return (
            floating(value)
            if any(character in _lexeme(value) for character in ".eE")
            else integer(value, bits=None)
        )
    if isinstance(value, dict):
        return {key: plain_numbers(item) for key, item in value.items()}
    if isinstance(value, list):
        return [plain_numbers(item) for item in value]
    return value
