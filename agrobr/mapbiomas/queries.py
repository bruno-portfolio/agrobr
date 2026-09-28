from __future__ import annotations

import re
from typing import Any

from agrobr import constants
from agrobr.exceptions import InvalidParameterError
from agrobr.normalize import regions

from . import models


def validar_colecao(colecao: int | None) -> int:
    if colecao is None:
        return models.COLECAO_ATUAL
    if (
        isinstance(colecao, bool)
        or not isinstance(colecao, int)
        or colecao not in models.ANOS_FINAIS
    ):
        raise InvalidParameterError(
            f"colecao {colecao} nao suportada; disponíveis: {sorted(models.ANOS_FINAIS)}"
        )
    return colecao


def normalizar_estado(estado: str | None) -> str | None:
    if estado is None:
        return None
    if not isinstance(estado, str):
        raise InvalidParameterError("estado deve ser uma string")
    estado_key = regions.remover_acentos(estado.strip().lower())
    estado_uf = regions.NOMES_PARA_UF.get(estado_key)
    if estado_uf is None:
        raise InvalidParameterError(
            f"Estado inválido: {estado!r}. Use a sigla ou o nome completo de uma UF"
        )
    return estado_uf


def normalizar_bioma(bioma: str | None) -> str | None:
    if bioma is None:
        return None
    if not isinstance(bioma, str):
        raise InvalidParameterError("bioma deve ser uma string")
    normalized = models.normalizar_bioma(bioma)
    if normalized not in models.BIOMAS_VALIDOS:
        raise InvalidParameterError(
            f"Bioma inválido: {bioma!r}. Opções: {sorted(models.BIOMAS_VALIDOS)}"
        )
    return normalized


def validar_ano(ano: int | None, colecao: int) -> None:
    ano_fim = models.ANOS_FINAIS[colecao]
    if ano is not None and (
        not isinstance(ano, int) or isinstance(ano, bool) or not models.ANO_INICIO <= ano <= ano_fim
    ):
        raise InvalidParameterError(
            f"ano deve estar entre {models.ANO_INICIO} e {ano_fim} na coleção {colecao}"
        )


def validar_periodo(periodo: str | None, colecao: int) -> None:
    if periodo is None:
        return
    if not isinstance(periodo, str) or not re.fullmatch(r"[0-9]{4}-[0-9]{4}", periodo):
        raise InvalidParameterError("periodo deve ter o formato AAAA-AAAA")
    inicio, fim = map(int, periodo.split("-"))
    validar_ano(inicio, colecao)
    validar_ano(fim, colecao)
    if inicio >= fim:
        raise InvalidParameterError("periodo deve ter ano inicial anterior ao final")


def validar_classe(nome: str, valor: int | None) -> None:
    if valor is not None and (isinstance(valor, bool) or not isinstance(valor, int) or valor < 0):
        raise InvalidParameterError(f"{nome} deve ser um inteiro não negativo")


def validar_opcoes(kwargs: dict[str, Any], *, as_polars: bool, return_meta: bool) -> None:
    if kwargs:
        raise InvalidParameterError(f"Parâmetros desconhecidos: {', '.join(sorted(kwargs))}")
    for nome, valor in (("as_polars", as_polars), ("return_meta", return_meta)):
        if not isinstance(valor, bool):
            raise InvalidParameterError(f"{nome} deve ser booleano")


def validar_dimensao(nivel: str, municipio: str | None, geocodigo: str | None) -> None:
    if not isinstance(nivel, str) or nivel not in {"estado", "municipio"}:
        raise InvalidParameterError("nivel deve ser 'estado' ou 'municipio'")
    if municipio is not None and (not isinstance(municipio, str) or not municipio.strip()):
        raise InvalidParameterError("municipio deve ser uma string não vazia")
    if geocodigo is not None and (
        not isinstance(geocodigo, str)
        or not re.fullmatch(constants.MAPBIOMAS_GEOCODE_PATTERN, geocodigo)
    ):
        raise InvalidParameterError("geocodigo deve ser uma string de sete dígitos ASCII")
    if nivel != "municipio" and (municipio is not None or geocodigo is not None):
        raise InvalidParameterError("municipio e geocodigo exigem nivel='municipio'")
