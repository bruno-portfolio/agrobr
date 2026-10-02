from __future__ import annotations

import re
from typing import Any

from agrobr import constants
from agrobr.exceptions import InvalidParameterError
from agrobr.normalize import municipalities, regions

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


def normalizar_uf(uf: str | None) -> str | None:
    if uf is None:
        return None
    sigla = (
        regions.NOMES_PARA_UF.get(regions.remover_acentos(uf.strip().lower()))
        if isinstance(uf, str)
        else None
    )
    if sigla is None:
        raise InvalidParameterError(
            f"UF inválida: {uf!r}. Use a sigla ou o nome completo; siglas válidas: "
            f"{', '.join(sorted(regions.UFS_VALIDAS))}"
        )
    return sigla


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


def geocodigo_do_municipio(nivel: str, municipio: str | int | None, uf: str | None) -> str | None:
    """Geocódigo do MapBiomas para o ``municipio`` dado por nome ou por código de 7 dígitos.

    O nome passa por ``normalize.resolver_municipio`` (nome inteiro, com a ``uf`` para
    desambiguar). O código segue direto: o recurso municipal também publica geocódigos fora do
    cadastro de municípios do IBGE (Lagoa Mirim e Lagoa dos Patos), conferidos depois do download.
    """
    if not isinstance(nivel, str) or nivel not in {"estado", "municipio"}:
        raise InvalidParameterError("nivel deve ser 'estado', 'uf' ou 'municipio'")
    if municipio is None:
        return None
    if nivel != "municipio":
        raise InvalidParameterError("municipio exige nivel='municipio'")
    if isinstance(municipio, bool) or not isinstance(municipio, (str, int)):
        raise InvalidParameterError(
            f"municipio deve ser o nome ou o código de 7 dígitos: {municipio!r}"
        )
    texto = str(municipio).strip()
    if re.fullmatch(constants.MAPBIOMAS_GEOCODE_PATTERN, texto):
        return texto
    if isinstance(municipio, int) or texto.isdigit():
        raise InvalidParameterError(f"municipio como código deve ter 7 dígitos: {municipio!r}")
    return f"{municipalities.resolver_municipio(texto, uf)['codigo_ibge']:07d}"
