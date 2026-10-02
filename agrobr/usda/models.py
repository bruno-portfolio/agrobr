from __future__ import annotations

import json
from functools import cache
from pathlib import Path

from agrobr.exceptions import InvalidParameterError
from agrobr.normalize import crops, regions
from agrobr.utils import time as time_utils

CATALOGOS = Path(__file__).parent / "catalogos"
MUNDO = "00"
NOME_MUNDO = "World"

PSD_COMMODITIES: dict[str, str] = {
    "soja": "2222000",
    "soybeans": "2222000",
    "milho": "0440000",
    "corn": "0440000",
    "trigo": "0410000",
    "wheat": "0410000",
    "arroz": "0422110",
    "rice": "0422110",
    "algodao": "2631000",
    "cotton": "2631000",
    "acucar": "0612000",
    "sugar": "0612000",
    "farelo_soja": "0813100",
    "soybean_meal": "0813100",
    "oleo_soja": "4232000",
    "soybean_oil": "4232000",
    "cafe": "0711100",
    "coffee": "0711100",
}

_SINONIMOS_QUE_MUDAM_O_RECORTE = frozenset({"arroz_casca", "arroz_em_casca"})

_COMMODITY_NAMES: dict[str, str] = {
    "2222000": "soja",
    "0440000": "milho",
    "0410000": "trigo",
    "0422110": "arroz",
    "2631000": "algodao",
    "0612000": "acucar",
    "0813100": "farelo_soja",
    "4232000": "oleo_soja",
    "0711100": "cafe",
}

PSD_ATTRIBUTES: dict[int, str] = {
    4: "area_colhida",
    20: "estoque_inicial",
    28: "producao",
    57: "importacao",
    86: "oferta_total",
    88: "exportacao",
    176: "estoque_final",
    178: "distribuicao_total",
    184: "produtividade",
}

CONSUMO_DOMESTICO: dict[str, int] = {"0612000": 126, "2631000": 142}
PERDAS: dict[str, int] = {"2631000": 150}

PSD_COUNTRIES: dict[str, str] = {
    "brasil": "BR",
    "brazil": "BR",
    "br": "BR",
    "eua": "US",
    "usa": "US",
    "us": "US",
    "china": "CH",
    "argentina": "AR",
    "india": "IN",
    "indonesia": "ID",
    "mexico": "MX",
    "ue": "E4",
    "eu": "E4",
}


@cache
def _catalogo(nome: str, chave: str, rotulo: str) -> dict[str | int, str]:
    registros = json.loads((CATALOGOS / f"{nome}.json").read_bytes())
    return {registro[chave]: registro[rotulo].strip() for registro in registros}


def nomes_de_atributo() -> dict[str | int, str]:
    return _catalogo("commodityAttributes", "attributeId", "attributeName")


def nomes_de_produto() -> dict[str | int, str]:
    return _catalogo("commodities", "commodityCode", "commodityName")


def nomes_de_pais() -> dict[str | int, str]:
    return _catalogo("countries", "countryCode", "countryName")


def unidades() -> dict[str | int, str]:
    return _catalogo("unitsOfMeasure", "unitId", "unitDescription")


def attribute_br(commodity_code: str, attribute_id: int) -> str | None:
    if attribute_id == CONSUMO_DOMESTICO.get(commodity_code, 125):
        return "consumo_domestico"
    if attribute_id == PERDAS.get(commodity_code):
        return "perdas"
    return PSD_ATTRIBUTES.get(attribute_id)


def resolve_commodity_code(nome: str) -> str:
    if not isinstance(nome, str):
        raise InvalidParameterError("produto deve ser uma string")
    key = nome.strip().lower()
    if "_".join(regions.remover_acentos(key).split()) not in _SINONIMOS_QUE_MUDAM_O_RECORTE:
        key = crops.normalizar_cultura(key)
    if key in PSD_COMMODITIES:
        return PSD_COMMODITIES[key]
    if key in nomes_de_produto():
        return key
    raise InvalidParameterError(
        f"Produto desconhecido: '{nome}'. Opções: {sorted(set(_COMMODITY_NAMES.values()))} "
        "ou um commodityCode do catálogo oficial do PSD"
    )


def resolve_country_code(nome: str) -> str:
    if not isinstance(nome, str):
        raise InvalidParameterError("país deve ser uma string")
    key = nome.strip().lower()
    if key in {"world", "all"}:
        return MUNDO if key == "world" else "all"
    if key in PSD_COUNTRIES:
        return PSD_COUNTRIES[key]
    if key.upper() in nomes_de_pais():
        return key.upper()
    raise InvalidParameterError(
        f"País desconhecido: '{nome}'. Opções: {sorted(PSD_COUNTRIES)}, 'world', 'all' "
        "ou um countryCode do catálogo oficial do PSD (não é ISO: CH é a China, E4 a União Europeia)"
    )


def resolve_attributes(attributes: list[str] | None) -> list[str] | None:
    if attributes is None:
        return None
    if not isinstance(attributes, list) or not all(isinstance(a, str) for a in attributes):
        raise InvalidParameterError(
            "attributes (atributos em datasets.oferta_demanda_global) deve ser uma lista de strings"
        )
    conhecidos = {nome.lower() for nome in nomes_de_atributo().values()}
    conhecidos |= {*PSD_ATTRIBUTES.values(), "consumo_domestico", "perdas"}
    pedidos = [a.strip().lower() for a in attributes]
    desconhecidos = [
        a for a, pedido in zip(attributes, pedidos, strict=True) if pedido not in conhecidos
    ]
    if desconhecidos:
        raise InvalidParameterError(
            f"Atributo desconhecido: {desconhecidos}. Use o nome oficial do PSD (ex.: 'Production') "
            f"ou um rótulo do agrobr: {sorted({*PSD_ATTRIBUTES.values(), 'consumo_domestico', 'perdas'})}"
        )
    return pedidos


def commodity_name(code: str) -> str:
    return _COMMODITY_NAMES.get(code) or nomes_de_produto().get(code, code)


def validate_market_year(ano: int | None) -> None:
    corrente = time_utils.hoje().year
    if ano is not None and (
        not isinstance(ano, int) or isinstance(ano, bool) or not 1960 <= ano <= corrente
    ):
        raise InvalidParameterError(
            "market_year (ano_comercial em datasets.oferta_demanda_global) deve ser inteiro "
            f"entre 1960 e {corrente}: {ano!r}"
        )
