from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pandas as pd
import pytest

from agrobr import conab, datasets, ibge
from agrobr.conab.progresso import parser as progresso_parser
from agrobr.exceptions import ContractViolationError
from tests import helpers
from tests.integration import test_produtos_live as matrix
from tests.integration import test_produtos_sicar_live as sicar_matrix


@pytest.mark.parametrize("count", [1, 5, 6, 11, 53])
def test_rotacao_limita_e_cobre_catalogo(count: int):
    products = [f"produto_{index}" for index in range(count)]
    groups = [matrix._weekly_products(products, week) for week in range(1, (count + 4) // 5 + 1)]
    assert all(0 < len(group) <= 5 for group in groups)
    flattened = [product for group in groups for product in group]
    assert sorted(flattened) == sorted(products)
    assert len(flattened) == len(set(flattened))


def test_rotacao_independe_da_ordem_do_catalogo():
    products = datasets.list_products("censo_agropecuario_municipal_1985")
    assert matrix._weekly_products(products, 38) == matrix._weekly_products(products[::-1], 38)


@pytest.mark.slow
def test_rotacao_independe_do_hashseed():
    command = (
        "import json; from agrobr import datasets; "
        "from tests.integration import test_produtos_live as matrix; "
        "print(json.dumps(matrix._weekly_products(datasets.list_products('producao_anual'), 38)))"
    )
    outputs = [
        subprocess.run(
            [sys.executable, "-c", command],
            cwd=Path(__file__).resolve().parents[1],
            env={**os.environ, "PYTHONHASHSEED": seed},
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        ).stdout
        for seed in ("1", "2")
    ]
    assert json.loads(outputs[0]) == json.loads(outputs[1])
    assert 0 < len(json.loads(outputs[0])) <= 5


def test_matriz_cobre_catalogos_sem_duplicar_c7():
    cases = matrix._product_cases(38)
    eligible = {
        name
        for name in datasets.list_datasets()
        if datasets.list_products(name)
        and name not in matrix.C7_DATASETS | matrix.OFF_MATRIX_DATASETS
    }
    rotating = {"embarques_anec", "preco_atacado"}
    assert {name for name, _ in cases} == eligible
    assert rotating <= eligible
    assert len(cases) == len(set(cases))
    for name in eligible:
        advertised = set(datasets.list_products(name))
        selected = {product for dataset, product in cases if dataset == name}
        assert selected <= advertised
        if matrix._uses_ibge(name) or name in rotating:
            assert 0 < len(selected) <= 5
        else:
            assert selected == advertised


def test_zoneamento_agricola_fica_fora_da_matriz_pela_safra_fixa_da_consulta():
    assert datasets.list_products("zoneamento_agricola")
    assert "zoneamento_agricola" not in {name for name, _ in matrix._product_cases(38)}, (
        "a consulta de referência fixa safra='2025/2026', e as culturas perenes e as anuais "
        "que terminam em 2023/2024 não estão nessa tábua"
    )


@pytest.mark.parametrize("name,product", [("precos_diesel", "DIESEL"), ("producao_anual", "milho")])
def test_consulta_preserva_recorte_e_troca_produto(name: str, product: str):
    original_args, original_kwargs = matrix.test_datasets_live.LIVE_CASES[name]
    before = dict(original_kwargs)
    args, kwargs = matrix._product_query(name, product)
    if name == "precos_diesel":
        assert args == original_args == ()
        assert kwargs.pop("produto") == product
    else:
        assert args[0] == product and args[1:] == original_args[1:]
    assert kwargs == {key: value for key, value in before.items() if key not in {"uf", "estado"}}
    assert original_kwargs == before


@pytest.mark.parametrize("name", ["comparacao_anual_anec", "embarques_mensais_anec"])
async def test_matriz_usa_fetcher_dos_recortes(name: str, monkeypatch: pytest.MonkeyPatch):
    fetcher = AsyncMock(
        return_value=(
            pd.DataFrame({"valor": [1]}),
            SimpleNamespace(selected_source="anec", attempted_sources=["anec"]),
        )
    )
    monkeypatch.setattr(type(datasets.get_dataset(name)), "fetch", fetcher)
    monkeypatch.setattr(helpers.asyncio, "sleep", AsyncMock())
    await matrix.test_produto_live(name, "soybean", lambda *_: None, lambda *_: None)
    fetcher.assert_awaited_once_with(
        "soybean", ano=2026, semana=13, use_cache=False, return_meta=True
    )


def test_arquivos_particionam_matriz_sem_perder_casos():
    general = set(matrix.GENERAL_CASES)
    sicar = set(sicar_matrix.PRODUCT_CASES)
    calendar = {
        ("progresso_safra", product) for product in datasets.list_products("progresso_safra")
    }
    assert not general & sicar
    assert not (general | sicar) & calendar
    assert general | sicar | calendar == set(matrix.PRODUCT_CASES)
    assert {name for name, _ in sicar} == {"cadastro_rural"}


@pytest.mark.parametrize("empty", [False, True])
async def test_sociobio_escolhe_ano_observado_e_ordem_estavel(monkeypatch, empty):
    inventory = pd.DataFrame(
        [
            {"ano": 2022, "uf": "PI", "aba": "segunda", "indice_aba": 2, "status": "identified"},
            {"ano": 2023, "uf": "CE", "aba": "recusada", "indice_aba": 0, "status": "unresolved"},
            {"ano": 2021, "uf": "CE", "aba": "antiga", "indice_aba": 1, "status": "identified"},
            {"ano": 2022, "uf": "CE", "aba": "primeira", "indice_aba": 1, "status": "identified"},
        ]
    )
    if empty:
        inventory["status"] = "unresolved"
    monkeypatch.setattr(conab, "catalogo_sociobiodiversidade", AsyncMock(return_value=inventory))
    fetcher = AsyncMock(
        return_value=(
            pd.DataFrame({"valor": [1]}),
            SimpleNamespace(selected_source="conab_sociobio", attempted_sources=["conab_sociobio"]),
        )
    )
    monkeypatch.setattr(type(datasets.get_dataset("custo_sociobiodiversidade")), "fetch", fetcher)
    monkeypatch.setattr(helpers.asyncio, "sleep", AsyncMock())
    properties = {}
    if empty:
        with pytest.raises(AssertionError, match="Nenhuma aba"):
            await matrix.test_produto_live(
                "custo_sociobiodiversidade", "carnauba", properties.__setitem__, lambda *_: None
            )
        fetcher.assert_not_awaited()
    else:
        await matrix.test_produto_live(
            "custo_sociobiodiversidade", "carnauba", properties.__setitem__, lambda *_: None
        )
        fetcher.assert_awaited_once_with(
            "carnauba", ano=2022, uf="CE", aba="primeira", return_meta=True
        )
        assert properties["ano"] == 2022
        assert properties["uf"] == "CE"
        assert properties["aba"] == "primeira"


@pytest.mark.parametrize(
    "name",
    [
        "abate_trimestral",
        "censo_agropecuario",
        "censo_agropecuario_historico",
        "credito_rural",
        "estimativa_safra",
        "exportacao",
        "extrativismo_vegetal",
        "importacao",
        "leite_industrial",
        "pecuaria_municipal",
        "producao_anual",
        "silvicultura",
    ],
)
def test_consulta_nacional_preserva_periodo_agregacao(name: str):
    product = datasets.list_products(name)[0]
    original_args, original_kwargs = matrix.test_datasets_live.LIVE_CASES[name]
    before = dict(original_kwargs)
    args, kwargs = matrix._product_query(name, product)
    assert args == (product, *original_args[1:])
    assert "uf" not in kwargs and "estado" not in kwargs
    assert kwargs == {key: value for key, value in before.items() if key not in {"uf", "estado"}}
    assert original_kwargs == before


@pytest.mark.parametrize(
    "biome,uf",
    [
        ("Amazônia", "PA"),
        ("Caatinga", "BA"),
        ("Cerrado", "MT"),
        ("Mata Atlântica", "MG"),
        ("Pampa", "RS"),
        ("Pantanal", "MS"),
    ],
)
async def test_desmatamento_consulta_e_registra_uf_do_bioma(
    biome: str, uf: str, monkeypatch: pytest.MonkeyPatch
):
    fetcher = AsyncMock(
        return_value=(
            pd.DataFrame({"valor": [1]}),
            SimpleNamespace(selected_source="desmatamento", attempted_sources=["desmatamento"]),
        )
    )
    monkeypatch.setattr(type(datasets.get_dataset("desmatamento")), "fetch", fetcher)
    monkeypatch.setattr(helpers.asyncio, "sleep", AsyncMock())
    original_args, original_kwargs = matrix.test_datasets_live.LIVE_CASES["desmatamento"]
    before = dict(original_kwargs)
    properties = {}
    await matrix.test_produto_live("desmatamento", biome, properties.__setitem__, lambda *_: None)
    fetcher.assert_awaited_once_with(
        biome, *original_args[1:], **{**before, "uf": uf}, return_meta=True
    )
    assert properties["uf"] == properties["query_scope"] == uf
    assert original_args == ("Cerrado",)
    assert original_kwargs == before
    assert original_kwargs["uf"] == "DF"


async def test_censo_consulta_a_primeira_uf_da_cobertura(monkeypatch: pytest.MonkeyPatch):
    name = "censo_agropecuario_municipal_1985"
    product = "efetivo_bubalinos"
    coverage = await ibge.cobertura_censo_agro_municipal_1985()
    assert "AC" not in coverage[product]
    fetcher = AsyncMock(
        return_value=(
            pd.DataFrame({"valor": [1]}),
            SimpleNamespace(selected_source="ibge", attempted_sources=["ibge"]),
        )
    )
    monkeypatch.setattr(type(datasets.get_dataset(name)), "fetch", fetcher)
    monkeypatch.setattr(helpers.asyncio, "sleep", AsyncMock())
    properties = {}
    await matrix.test_produto_live(name, product, properties.__setitem__, lambda *_: None)
    args, kwargs = matrix._product_query(name, product)
    fetcher.assert_awaited_once_with(*args, **kwargs, uf=coverage[product][0], return_meta=True)
    assert properties["uf_cobertura"] == properties["query_scope"] == coverage[product][0]
    assert properties["live_matrix_status"] == "validated"


@pytest.mark.parametrize(
    "reference,expected",
    [
        (date(2026, 1, 1), "2025-12-01"),
        (date(2024, 3, 31), "2024-02-01"),
        (date(2025, 3, 31), "2025-02-01"),
        (date(2026, 9, 16), "2026-08-01"),
    ],
)
def test_janela_sicar_primeiro_dia_mes_anterior(reference: date, expected: str):
    assert sicar_matrix._previous_month_start(reference) == expected


async def test_sicar_consulta_e_registra_janela_recente(monkeypatch: pytest.MonkeyPatch):
    fetcher = AsyncMock(
        return_value=(
            pd.DataFrame({"cod_imovel": ["BA-1"]}),
            SimpleNamespace(selected_source="sicar", attempted_sources=["sicar"]),
        )
    )
    monkeypatch.setattr(type(datasets.get_dataset("cadastro_rural")), "fetch", fetcher)
    monkeypatch.setattr(helpers.asyncio, "sleep", AsyncMock())
    monkeypatch.setattr(sicar_matrix, "CREATED_AFTER", "2026-08-01")
    properties = {}
    await sicar_matrix.test_produto_live(
        "cadastro_rural", "BA", properties.__setitem__, lambda *_: None
    )
    original_args, original_kwargs = matrix.test_datasets_live.LIVE_CASES["cadastro_rural"]
    expected = {**original_kwargs, "criado_apos": "2026-08-01"}
    fetcher.assert_awaited_once_with("BA", *original_args[1:], **expected, return_meta=True)
    assert properties["criado_apos"] == "2026-08-01"
    assert properties["collection_date"] == sicar_matrix.COLLECTION_DATE.isoformat()


def test_calendario_reconhece_rotulos_oficiais_e_aliases():
    observed, outside, transitions = matrix._calendar_products(
        ["Algodão", "Arroz", "Feijão 1ª", "Milho 1ª", "milho_2", "Soja", "Trigo"], 9
    )
    assert observed == set(datasets.list_products("progresso_safra"))
    assert not outside
    assert not transitions


def test_calendario_rejeita_cultura_oficial_desconhecida():
    with pytest.raises(AssertionError, match="Culturas oficiais fora do catálogo:.*Canola"):
        matrix._calendar_products(["Canola"], 9)


@pytest.mark.parametrize("product,month", [("trigo", 9), ("soja", 12), ("algodao", 1)])
def test_calendario_rejeita_produto_ausente_no_interior(product: str, month: int):
    products = [value for value in datasets.list_products("progresso_safra") if value != product]
    with pytest.raises(AssertionError, match=f"mês {month} ausentes:.*{product}"):
        matrix._calendar_products(products, month)


@pytest.mark.parametrize(
    "product,month",
    [("soja", 9), ("soja", 5), ("algodao", 11), ("algodao", 9), ("trigo", 5), ("trigo", 12)],
)
def test_calendario_aceita_ausencia_na_transicao(product: str, month: int):
    products = [value for value in datasets.list_products("progresso_safra") if value != product]
    observed, outside, transitions = matrix._calendar_products(products, month)
    assert observed == set(products)
    assert product not in outside
    assert transitions == {product}


@pytest.mark.parametrize("month", [1, 2])
def test_calendario_aceita_ausencia_fora_da_janela(month: int):
    products = [
        product for product in datasets.list_products("progresso_safra") if product != "trigo"
    ]
    observed, outside, transitions = matrix._calendar_products(products, month)
    assert observed == set(products)
    assert outside == {"trigo"}
    assert not transitions


async def test_calendario_consulta_boletim_mais_recente_uma_vez(monkeypatch: pytest.MonkeyPatch):
    payload = (
        Path(__file__).parent / "golden_data/conab_progresso/progresso_20260828.xlsx"
    ).read_bytes()
    latest = AsyncMock(return_value=(payload, "https://www.gov.br/conab/boletim.xlsx", "boletim"))
    monkeypatch.setattr(matrix.progresso_api.client, "fetch_latest", latest)
    monkeypatch.setattr(matrix.asyncio, "sleep", AsyncMock())
    monkeypatch.setattr(matrix, "COLLECTION_DATE", date(2026, 9, 16))
    properties = []
    await matrix.test_progresso_safra_calendario_live(
        lambda key, value: properties.append((key, value))
    )
    latest.assert_awaited_once_with()
    assert dict(properties)["query_scope"].startswith("fonte:")
    assert dict(properties)["records_count"] == 27
    assert dict(properties)["semana_atual"] == "2026-08-28"
    assert dict(properties)["observed_products"] == "algodao,milho_2,trigo"
    assert {value for key, value in properties if key == "fora_do_boletim"} == {
        "arroz",
        "feijao_1",
        "milho_1",
        "soja",
    }
    assert not any(key == "transicao" for key, _ in properties)
    assert dict(properties)["live_matrix_status"] == "validated"


async def test_calendario_rejeita_boletim_parcial_no_mes_da_semana(monkeypatch: pytest.MonkeyPatch):
    payload = (
        Path(__file__).parent / "golden_data/conab_progresso/progresso_sample/response.xlsx"
    ).read_bytes()
    frame = progresso_parser.parse_progresso_xlsx(payload)
    latest = AsyncMock(return_value=(payload, "https://www.gov.br/conab/boletim.xlsx", "boletim"))
    monkeypatch.setattr(matrix.progresso_api.client, "fetch_latest", latest)
    monkeypatch.setattr(matrix.asyncio, "sleep", AsyncMock())
    monkeypatch.setattr(matrix, "COLLECTION_DATE", date(2026, 9, 16))
    properties = {}
    missing = set(datasets.list_products("progresso_safra")) - {
        product
        for product in datasets.list_products("progresso_safra")
        if matrix.progresso_models.normalizar_cultura(product) in set(frame["cultura"])
    }
    assert missing, "Golden parcial deve exercitar a rejeição do boletim incompleto"
    with pytest.raises(AssertionError, match="Culturas no interior da janela no mês 2 ausentes"):
        await matrix.test_progresso_safra_calendario_live(properties.__setitem__)
    latest.assert_awaited_once_with()
    assert properties["query_scope"].startswith("fonte:")
    assert properties["records_count"] == len(frame)


async def test_calendario_registra_ausencia_de_transicao(monkeypatch: pytest.MonkeyPatch):
    payload = (
        Path(__file__).parent / "golden_data/conab_progresso/progresso_20260828.xlsx"
    ).read_bytes()
    frame = progresso_parser.parse_progresso_xlsx(payload)
    frame["semana_atual"] = "2026-09-11"
    source = AsyncMock(
        return_value=(frame, SimpleNamespace(selected_source="conab", attempted_sources=["conab"]))
    )
    monkeypatch.setattr(matrix.progresso_api, "progresso_safra", source)
    monkeypatch.setattr(matrix.asyncio, "sleep", AsyncMock())
    properties = []
    await matrix.test_progresso_safra_calendario_live(
        lambda key, value: properties.append((key, value))
    )
    assert {value for key, value in properties if key == "transicao"} == {
        "arroz",
        "feijao_1",
        "milho_1",
        "soja",
    }
    assert dict(properties)["live_matrix_status"] == "validated"


@pytest.mark.parametrize("invalid", ["empty", "contract"])
async def test_calendario_rejeita_boletim_invalido(invalid: str, monkeypatch: pytest.MonkeyPatch):
    frame = pd.DataFrame() if invalid == "empty" else pd.DataFrame({"cultura": ["Soja"]})
    source = AsyncMock(
        return_value=(frame, SimpleNamespace(selected_source="conab", attempted_sources=["conab"]))
    )
    monkeypatch.setattr(matrix.progresso_api, "progresso_safra", source)
    monkeypatch.setattr(matrix.asyncio, "sleep", AsyncMock())
    expected_error = AssertionError if invalid == "empty" else ContractViolationError
    with pytest.raises(expected_error):
        await matrix.test_progresso_safra_calendario_live(lambda *_: None)
