from __future__ import annotations

import json
import re
from collections.abc import Callable
from typing import Any

import pytest

from agrobr.ana import models as ana_models
from agrobr.ana import parser as ana_parser
from agrobr.exceptions import ParseError
from agrobr.sfb import models as sfb_models
from agrobr.sfb import parser as sfb_parser
from agrobr.utils.geo import LayerConfig
from tests import helpers

Parse = Callable[[list[bytes], bool], Any]


def _por_chave(modulo: Any, chave: str) -> Parse:
    def parse(pages: list[bytes], geo: bool) -> Any:
        funcao = modulo.parse_layer_geojson if geo else modulo.parse_layer_tabular
        return funcao(pages, layer_key=chave)

    return parse


def _massas(pages: list[bytes], geo: bool) -> Any:
    return ana_parser.parse_massas_dagua(pages, geo=geo)


CAMADAS: list[tuple[str, LayerConfig, Parse]] = [
    *[(f"ana_{k}", cfg, _por_chave(ana_parser, k)) for k, cfg in ana_models.LAYERS.items()],
    ("ana_massas_dagua", ana_models.MASSAS_DAGUA, _massas),
    *[(f"sfb_{k}", cfg, _por_chave(sfb_parser, k)) for k, cfg in sfb_models.LAYERS.items()],
]
PARAMETROS = [pytest.param(cfg, parse, id=nome) for nome, cfg, parse in CAMADAS]


def _pagina(campos: dict[str, int], *, geo: bool) -> list[bytes]:
    if geo:
        feicao = {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [-47.9, -15.8]},
            "properties": campos,
        }
        return [json.dumps({"type": "FeatureCollection", "features": [feicao]}).encode()]
    return [json.dumps({"features": [{"attributes": campos}]}).encode()]


@pytest.mark.parametrize("geo", [False, True], ids=["tabular", "geo"])
@pytest.mark.parametrize(("config", "parse"), PARAMETROS)
def test_campo_pedido_ausente_vira_parseerror(config, parse, geo):
    if geo:
        pytest.importorskip("geopandas")
    pedidos = config["fields"].split(",")
    with helpers.sem_excecao():
        parse(_pagina(dict.fromkeys(pedidos, 1), geo=geo), geo)
    for campo in pedidos:
        incompleta = _pagina({c: 1 for c in pedidos if c != campo}, geo=geo)
        with pytest.raises(ParseError, match=re.escape(f"'{campo}'")):
            parse(incompleta, geo)
