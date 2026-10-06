from __future__ import annotations

import json

import pytest

from agrobr.embrapa_solos import api, parser
from agrobr.exceptions import ContractViolationError, ParseError
from tests import helpers


@pytest.mark.parametrize(
    "defeito,motivo",
    [
        ("produto", "Produto Embrapa Solos inválido"),
        ("negativa", "Contagem negativa"),
        ("bool", "Modo geométrico deve ser bool"),
        ("totais", "Contagens publicadas divergentes"),
        ("ausente", "Membro geometry ausente"),
    ],
    ids=["produto", "negativa", "bool", "totais", "ausente"],
)
def test_pagina_layout_invalido(defeito, motivo):
    features = helpers.embrapa_solos_features()[:1]
    page = {"type": "FeatureCollection", "features": features}
    if defeito == "negativa":
        page["numberMatched"] = -1
    elif defeito == "totais":
        page.update(numberMatched=1, totalFeatures=2)
    elif defeito == "ausente":
        del features[0]["geometry"]
    with helpers.levanta_exatamente(ParseError, motivo):
        parser.parse_page(
            json.dumps(page).encode(),
            product="desconhecido" if defeito == "produto" else "perfis",
            include_geometry=0 if defeito == "bool" else False,
        )


async def test_perfis_identificador_reparado_vazio(monkeypatch):
    features = helpers.embrapa_solos_features()[:1]
    features[0]["id"] = "\u00c2\u00a0"
    helpers.install_embrapa_solos_wfs(monkeypatch, features)
    with helpers.levanta_exatamente(ContractViolationError, "Feature.id requires nonblank"):
        await api.perfis(max_registros=None)
