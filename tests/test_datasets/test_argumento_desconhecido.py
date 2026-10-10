from __future__ import annotations

import inspect
import re
from typing import Any
from unittest.mock import AsyncMock

import pandas as pd
import pytest

from agrobr import datasets
from tests import helpers

SONDA = {
    "abate_trimestral": {"ufs": ["SC"]},
    "autorizacoes_defensivos": {"registros": ["0513"]},
    "balanco": {"produtos": ["soja"]},
    "cadastro_rural": {"ufs": ["SP"]},
    "censo_agropecuario": {"ufs": ["DF"]},
    "censo_agropecuario_historico": {"ufs": ["MT"]},
    "censo_agropecuario_legado": {"ufs": ["DF"]},
    "censo_agropecuario_municipal_1985": {"ufs": ["MG"]},
    "clima": {"ufs": ["MT"]},
    "comercio_internacional": {"produtos": ["soja"]},
    "comparacao_anual_anec": {"produtos": ["soja"]},
    "composicao_defensivos": {"registros": ["0513"]},
    "condicao_lavouras": {"produtos": ["trigo"]},
    "cotacoes_cambio": {"moedas": ["JPY"]},
    "credito_rural": {"ufs": ["RS"]},
    "cultivares_protegidas": {"cultivares": ["BRS 1003IPRO"]},
    "cultivares_registradas": {"cultivares": ["BRS 1003IPRO"]},
    "custo_producao": {"ufs": ["TO"]},
    "custo_sociobiodiversidade": {"ufs": ["AM"]},
    "defensivos_formulados": {"registros": ["0513"]},
    "defensivos_tecnicos": {"ufs": ["MT"]},
    "desmatamento": {"ufs": ["DF"]},
    "destinos_anec": {"produtos": ["soja"]},
    "embarques_anec": {"produtos": ["soja"]},
    "embarques_mensais_anec": {"produtos": ["soja"]},
    "empregadores_lista_suja": {"ufs": ["TO"]},
    "estimativa_safra": {"ufs": ["MT"]},
    "expectativas_mercado": {"indicadores": ["PIB Agropecuária"]},
    "exportacao": {"ufs": ["MG"]},
    "extrativismo_vegetal": {"ufs": ["PA"]},
    "fertilizante": {"ufs": ["BR"]},
    "futuros_agricolas": {"produtos": ["boi"]},
    "importacao": {"ufs": ["PR"]},
    "leite_industrial": {"ufs": ["MG"]},
    "moedas_cambio": {"ufs": ["MT"]},
    "movimentacao_portuaria": {"ufs": ["SP"]},
    "oferta_demanda_global": {"produtos": ["soja"]},
    "pecuaria_municipal": {"ufs": ["RO"]},
    "pib_agro": {"produtos": ["agropecuaria"]},
    "posicionamento_fundos": {"produtos": ["soja"]},
    "preco_atacado": {"produtos": ["batata"]},
    "preco_diario": {"produtos": ["soja"]},
    "precos_diesel": {"ufs": ["SP"]},
    "producao_acucar_etanol": {"ufs": ["SP"]},
    "producao_anual": {"ufs": ["DF"]},
    "progresso_safra": {"estado": "MT"},
    "queimadas": {"ufs": ["MT"]},
    "seguro_rural": {"ufs": ["RS"]},
    "serie_historica_safra": {"ufs": ["GO"]},
    "series_economicas": {"codigos": [432]},
    "silvicultura": {"ufs": ["PR"]},
    "unidades_conservacao": {"ufs": ["DF"]},
    "unidades_conservacao_federais": {"ufs": ["DF"]},
    "uso_do_solo": {"estado": "DF"},
    "zoneamento_agricola": {"ufs": ["SP"]},
}
POSICIONAL = {"ano": 2026, "mes": 1, "codigo": 432}


def test_sonda_cobre_todos_os_datasets():
    assert sorted(SONDA) == datasets.list_datasets()


def obrigatorios(nome: str) -> tuple[list[Any], dict[str, Any]]:
    produtos = datasets.get_dataset(nome).info.products
    posicionais: list[Any] = []
    nomeados: dict[str, Any] = {}
    for parametro in inspect.signature(getattr(datasets, nome)).parameters.values():
        if parametro.default is not inspect.Parameter.empty:
            continue
        valor = POSICIONAL.get(parametro.name, produtos[0] if produtos else None)
        if parametro.kind is parametro.POSITIONAL_OR_KEYWORD:
            posicionais.append(valor)
        elif parametro.kind is parametro.KEYWORD_ONLY:
            nomeados[parametro.name] = valor
    return posicionais, nomeados


@pytest.mark.parametrize("nome", sorted(SONDA))
async def test_argumento_desconhecido_recusado_antes_da_rede(nome):
    posicionais, nomeados = obrigatorios(nome)
    (argumento,) = SONDA[nome]
    with helpers.levanta_exatamente(TypeError, match=re.escape(argumento)):
        await getattr(datasets, nome)(*posicionais, **nomeados, **SONDA[nome])


@pytest.mark.parametrize("nome", sorted(SONDA))
async def test_funcao_publica_repassa_as_polars_ao_fetch(nome, monkeypatch):
    fetch = AsyncMock(return_value=pd.DataFrame())
    monkeypatch.setattr(type(datasets.get_dataset(nome)), "fetch", fetch)
    posicionais, nomeados = obrigatorios(nome)
    with helpers.sem_excecao():
        await getattr(datasets, nome)(*posicionais, **nomeados, as_polars=True)
    assert fetch.await_args.kwargs["as_polars"] is True
