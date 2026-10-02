from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from datetime import date, datetime
from typing import Any, Literal, cast

from agrobr import constants
from agrobr.exceptions import InvalidParameterError
from agrobr.normalize.regions import remover_acentos
from agrobr.utils import time as time_utils
from agrobr.utils import validation

from . import models


@dataclass(frozen=True)
class FluxoQuery:
    anos: tuple[int, ...]
    frequencia: Literal["mensal", "diaria"]
    concessionaria: str | None
    rodovia: str | None
    uf: str | None
    praca: str | None
    tipo_veiculo: str | None
    tipo_cobranca: str | None
    inicio: date | None
    fim: date | None
    apenas_pesados: bool
    enriquecer: bool
    max_linhas: int
    max_memoria_bytes: int

    def details(self) -> dict[str, Any]:
        values = asdict(self)
        values["anos"] = list(self.anos)
        for name in ("inicio", "fim"):
            values[name] = values[name].isoformat() if values[name] is not None else None
        return values


def validate_flags(**flags: bool) -> None:
    for name, value in flags.items():
        if type(value) is not bool:
            raise InvalidParameterError(f"{name} deve ser booleano")


def _text(name: str, value: str | None) -> str | None:
    if value is not None and (not isinstance(value, str) or not value.strip()):
        raise InvalidParameterError(f"{name} deve ser texto não vazio")
    return value


def chave_texto(value: str) -> str:
    return " ".join(remover_acentos(value).casefold().split())


def _rodovia(valor: str) -> str:
    return re.sub(r"^([A-Z]+)0*(?=[0-9])", r"\1", re.sub(r"[\s-]", "", valor.upper()))


def _tipo_veiculo(value: str | None) -> str | None:
    if value is None:
        return None
    tipos = {chave_texto(tipo): tipo for tipo in constants.ANTT_TIPOS_VEICULO}
    if chave_texto(value) not in tipos:
        raise InvalidParameterError(
            f"tipo_veiculo inválido: {value!r}. Valores válidos: "
            f"{', '.join(constants.ANTT_TIPOS_VEICULO)}"
        )
    return tipos[chave_texto(value)]


def _years(ano: int | None, ano_inicio: int | None, ano_fim: int | None) -> tuple[int, ...]:
    current = time_utils.hoje().year
    for name, value in (("ano", ano), ("ano_inicio", ano_inicio), ("ano_fim", ano_fim)):
        if value is not None and (
            type(value) is not int or not models.ANO_INICIO <= value <= current
        ):
            raise InvalidParameterError(
                f"{name} deve ser inteiro entre {models.ANO_INICIO} e {current}"
            )
    if ano is not None and (ano_inicio is not None or ano_fim is not None):
        raise InvalidParameterError("Use ano ou intervalo ano_inicio/ano_fim, sem combiná-los")
    if ano_inicio is not None and ano_fim is not None and ano_inicio > ano_fim:
        raise InvalidParameterError("ano_inicio deve ser menor ou igual a ano_fim")
    return tuple(models._resolve_anos(ano=ano, ano_inicio=ano_inicio, ano_fim=ano_fim))


def build_query(
    *,
    ano: int | None,
    ano_inicio: int | None,
    ano_fim: int | None,
    frequencia: str,
    concessionaria: str | None,
    rodovia: str | None,
    uf: str | None,
    praca: str | None,
    tipo_veiculo: str | None,
    tipo_cobranca: str | None,
    inicio: str | date | datetime | None,
    fim: str | date | datetime | None,
    apenas_pesados: bool,
    enriquecer: bool,
    max_linhas: int,
    max_memoria_bytes: int,
) -> FluxoQuery:
    validate_flags(apenas_pesados=apenas_pesados, enriquecer=enriquecer)
    if not isinstance(frequencia, str) or frequencia not in ("mensal", "diaria"):
        raise InvalidParameterError("frequencia deve ser 'mensal' ou 'diaria'")
    years = _years(ano, ano_inicio, ano_fim)
    texts = {
        name: _text(name, value)
        for name, value in (
            ("concessionaria", concessionaria),
            ("rodovia", rodovia),
            ("uf", uf),
            ("praca", praca),
            ("tipo_veiculo", tipo_veiculo),
            ("tipo_cobranca", tipo_cobranca),
        )
    }
    texts["uf"] = validation.validate_uf(texts["uf"])
    texts["tipo_veiculo"] = _tipo_veiculo(texts["tipo_veiculo"])
    start, end = validation.parse_data(inicio, "inicio"), validation.parse_data(fim, "fim")
    for name, value in (("inicio", start), ("fim", end)):
        if value is not None and value.year not in years:
            raise InvalidParameterError(
                f"{name}={value.isoformat()} fora dos anos solicitados: {list(years)}"
            )
        if value is not None and frequencia == "mensal" and value.day != 1:
            raise InvalidParameterError(
                f"{name}={value.isoformat()}: o filtro mensal usa a referência no primeiro dia "
                f"do mês ({value.replace(day=1).isoformat()})"
            )
    if start is not None and end is not None and start > end:
        raise InvalidParameterError("inicio deve ser menor ou igual a fim")
    if not enriquecer and (uf is not None or rodovia is not None):
        raise InvalidParameterError("Filtros de UF/rodovia exigem enriquecer=True")
    for name, limit in (("max_linhas", max_linhas), ("max_memoria_bytes", max_memoria_bytes)):
        if type(limit) is not int or limit < 1:
            raise InvalidParameterError(f"{name} deve ser inteiro positivo")
    return FluxoQuery(
        anos=years,
        frequencia=cast(Literal["mensal", "diaria"], frequencia),
        inicio=start,
        fim=end,
        apenas_pesados=apenas_pesados,
        enriquecer=enriquecer,
        max_linhas=max_linhas,
        max_memoria_bytes=max_memoria_bytes,
        **texts,
    )
