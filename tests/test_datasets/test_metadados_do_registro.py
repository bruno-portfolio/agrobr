from unittest.mock import AsyncMock
from urllib.parse import urlsplit

import pandas as pd
import pytest

from agrobr import anda, constants, datasets
from agrobr.anec import models as anec_models
from agrobr.b3 import models as b3_models
from agrobr.cepea.parsers import v1 as cepea_v1
from agrobr.conab._serie_historica import client as serie_client
from agrobr.conab.ceasa import client as ceasa_client
from agrobr.conab.ceasa import models as ceasa_models
from agrobr.deral import models as deral_models
from agrobr.exceptions import InvalidParameterError
from agrobr.noticias_agricolas import parser as na_parser
from agrobr.utils import time as time_utils
from agrobr.zarc import models as zarc_models
from tests.helpers import levanta_exatamente
from tests.test_conab_ceasa import test_nome_fora_do_publicado


def _licenca_do_describe(nome: str) -> str:
    (linha,) = [
        linha for linha in datasets.describe(nome).splitlines() if linha.startswith("  License: ")
    ]
    return linha.removeprefix("  License: ")


def test_describe_all_mostra_a_licenca_do_describe():
    linhas = {linha.split()[0]: linha for linha in datasets.describe_all().splitlines()[2:]}
    divergentes = {
        nome: (_licenca_do_describe(nome), linhas[nome])
        for nome in datasets.list_datasets()
        if _licenca_do_describe(nome) not in linhas[nome]
    }
    assert divergentes == {}
    assert "livre (comexstat), zona_cinza (abiove)" in linhas["exportacao"]


def test_preco_diario_publica_a_licenca_da_noticias_agricolas_tentada_pela_cepea():
    info = datasets.info("preco_diario")
    dataset = datasets.get_dataset("preco_diario")
    assert info["sources"] == ["cepea"]
    assert [fonte.name for fonte in dataset.info.sources if fonte.enabled] == ["cepea"]
    assert info["licenses"] == {"cepea": "nc", "noticias_agricolas": "zona_cinza"}
    assert (
        "  License: nc (cepea), zona_cinza (noticias_agricolas)"
        in datasets.describe("preco_diario").splitlines()
    )
    outras = [nome for nome in datasets.list_datasets() if nome != "preco_diario"]
    assert {nome: datasets.info(nome)["licenses"] for nome in outras} == {
        nome: {
            fonte.name: constants.licenca_da_fonte(fonte.name)
            for fonte in datasets.get_dataset(nome).info.sources
        }
        for nome in outras
    }


def test_comercio_internacional_publica_a_classificacao_restrito():
    info = datasets.info("comercio_internacional")
    assert info["license"] == "restrito"
    assert info["licenses"] == {"comtrade": "restrito"}
    assert _licenca_do_describe("comercio_internacional") == "restrito"


def test_futuros_agricolas_unit_cita_a_unidade_de_cada_contrato():
    unit = datasets.info("futuros_agricolas")["unit"]
    assert sorted(u for u in set(b3_models.UNIDADES.values()) if u not in unit) == []
    assert "ajuste_por_contrato" in unit
    assert "posições em contratos" in unit


def test_preco_diario_unit_cita_o_rotulo_fora_de_brl_das_duas_fontes():
    unit = datasets.info("preco_diario")["unit"]
    parser = cepea_v1.CepeaParserV1()
    rotulos = {
        rotulo
        for produto in datasets.list_products("preco_diario")
        for rotulo in (na_parser.UNIDADES.get(produto), parser._detect_unidade(produto, []))
        if rotulo and not rotulo.startswith("BRL/")
    }
    assert rotulos == {"cBRL/lb"}
    assert "algodão em centavos de BRL por libra-peso (cBRL/lb)" in unit


@pytest.mark.parametrize(
    ("nome", "vocabulario"),
    [
        ("embarques_anec", set(anec_models.PRODUTO_ALIASES.values())),
        ("preco_atacado", ceasa_models.PRODUTOS_PROHORT),
        ("zoneamento_agricola", zarc_models.CULTURAS_CANONICAS),
    ],
)
def test_list_products_traz_o_vocabulario_fechado_da_fonte(nome, vocabulario):
    assert datasets.list_products(nome) == sorted(vocabulario)


def test_condicao_lavouras_lista_os_produtos_que_o_validador_aceita():
    assert datasets.list_products("condicao_lavouras") == sorted(deral_models.PRODUTOS_ACEITOS)


@pytest.mark.parametrize(
    ("nome", "produto"),
    [
        ("embarques_anec", "farelo_soja"),
        ("embarques_anec", "banana_inexistente"),
        ("preco_atacado", "TOMATO"),
        ("preco_atacado", "banana_inexistente"),
        ("zoneamento_agricola", "corn"),
        ("zoneamento_agricola", "banana_inexistente"),
    ],
)
async def test_produto_fora_do_vocabulario_chega_igual_a_fonte_e_e_recusado(
    monkeypatch, nome, produto
):
    if nome == "preco_atacado":
        test_nome_fora_do_publicado._servir(monkeypatch)
    ano = {"ano": 2026} if nome == "embarques_anec" else {}
    with levanta_exatamente(InvalidParameterError, match=f"'{produto}'"):
        await getattr(datasets, nome)(produto=produto, **ano)


def test_source_url_e_o_endereco_que_o_client_chama():
    serie = datasets.info("serie_historica_safra")["source_url"]
    atacado = datasets.info("preco_atacado")["source_url"]
    assert serie == serie_client.SERIES_HISTORICAS_URL
    assert all(
        serie_client.get_xls_url(produto).startswith(f"{serie}/")
        for produto in datasets.list_products("serie_historica_safra")
    )
    assert ceasa_client._build_url(ceasa_models.CDA_PROHORT, ceasa_models.QUERY_PRECOS).startswith(
        f"{atacado}?"
    )
    partes = urlsplit(atacado)
    assert (partes.scheme, partes.username, partes.password, partes.query) == (
        "https",
        None,
        None,
        "",
    )


async def test_fertilizante_frequencia_segue_o_ano_em_curso_do_fetcher(monkeypatch):
    entregas = AsyncMock(return_value=(pd.DataFrame(), None))
    monkeypatch.setattr(anda, "entregas", entregas)
    (fonte,) = datasets.get_dataset("fertilizante").info.sources
    await fonte.fetch_fn("total")
    info = datasets.info("fertilizante")
    assert entregas.await_args.args == (time_utils.hoje().year,)
    assert info["update_frequency"] == "monthly"
    assert info["typical_latency"].startswith("edição acumulada do ano em curso")
