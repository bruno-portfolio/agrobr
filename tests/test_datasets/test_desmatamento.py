from __future__ import annotations

import copy
import warnings
from unittest.mock import AsyncMock

import pandas as pd
import pytest

from agrobr import deterministic
from agrobr.datasets import _desmatamento_aggregation
from agrobr.datasets.desmatamento import (
    DesmatamentoDataset,
)
from agrobr.exceptions import ContractViolationError, InvalidParameterError
from agrobr.normalize import municipalities, regions
from tests.helpers import (
    collect_failures,
    desmatamento_features,
    desmatamento_frame,
    desmatamento_meta,
    install_desmatamento_wfs,
    isolated_dataset_case,
    levanta_exatamente,
    sem_excecao,
)


@pytest.mark.parametrize("tipo,bioma", [("prodes", "Cerrado"), ("deter", "Amazônia")])
async def test_fetch_aggregates_all_occurrences_with_source_metadata(tipo, bioma):
    dataset = DesmatamentoDataset()
    source_frame = pd.concat(
        [desmatamento_frame(tipo), desmatamento_frame(tipo)], ignore_index=True
    )
    source_meta = desmatamento_meta(len(source_frame))
    source_meta.selected_source = f"terrabrasilis_{tipo}"
    source_meta.attempted_sources = [source_meta.selected_source]
    source_meta.fetch_timestamp = source_meta.fetched_at
    fetch = AsyncMock(return_value=(source_frame, source_meta))
    dataset.info.sources[0].fetch_fn = fetch
    frame, meta = await dataset.fetch(bioma, tipo=tipo, return_meta=True)
    assert len(frame) == 1
    assert frame["area_km2"].iloc[0] == (2469.12 if tipo == "prodes" else 10.84)
    assert meta.dataset == "desmatamento"
    assert meta.contract_version == meta.schema_version == ("2.1" if tipo == "deter" else "2.0")
    assert meta.records_count == len(frame)
    assert meta.fetch_timestamp == source_meta.fetched_at
    assert meta.selected_source == f"terrabrasilis_{tipo}"
    assert meta.attempted_sources == [meta.selected_source]
    assert meta.source_details["aggregation"]["input_occurrences"] == 2
    assert meta.source_details["aggregation"]["discarded_occurrences"] == 0
    assert not meta.source_details["aggregation"]["official_rate"]
    assert "aggregation" not in source_meta.source_details


@pytest.mark.parametrize("tipo", ["prodes", "deter"])
def test_aggregation_missing_area_propagates_and_zero_is_valid(tipo):
    source = pd.concat(
        [desmatamento_frame(tipo, area_km2=0.0), desmatamento_frame(tipo, area_km2=None)],
        ignore_index=True,
    )
    frame, details = _desmatamento_aggregation.aggregate(source, tipo)
    assert pd.isna(frame["area_km2"].iloc[0])
    assert details["missing_area_groups"] == 1
    zeros, _ = _desmatamento_aggregation.aggregate(source.iloc[:1], tipo)
    assert zeros["area_km2"].iloc[0] == 0.0


def test_desmatamento_casos_2():
    with collect_failures() as check:
        case = "test_aggregation_overflow_fails_explicitly"
        with check(case), isolated_dataset_case(case):
            source = pd.concat([desmatamento_frame(area_km2=1.0e308)] * 2, ignore_index=True)
            with levanta_exatamente(ContractViolationError, match="float64"):
                _desmatamento_aggregation.aggregate(source, "prodes")
        case = "test_aggregation_negative_area_is_not_masked_by_missing_area"
        with check(case), isolated_dataset_case(case):
            source = pd.concat(
                [desmatamento_frame(area_km2=-1.0), desmatamento_frame(area_km2=None)],
                ignore_index=True,
            )
            with pytest.raises(ContractViolationError):
                _desmatamento_aggregation.aggregate(source, "prodes")


@pytest.mark.parametrize(
    "kwargs",
    [
        {"tipo": "invalido"},
        {"tipo": "deter", "bioma": "Caatinga"},
        {"tipo": "deter", "ano": 2024},
        {"tipo": "prodes", "classe": "DESMATAMENTO_CR"},
        {"inicio": "2024-02-30"},
        {"uf": "ZZ"},
        {"ano": 2024.0},
        {"max_registros": 0},
        {"tamanho_pagina": True},
        {"as_polars": 1},
        {"return_meta": 1},
    ],
)
async def test_fetch_pure_guards_before_source(kwargs):
    dataset = DesmatamentoDataset()
    fetch = AsyncMock()
    dataset.info.sources[0].fetch_fn = fetch
    with levanta_exatamente(InvalidParameterError):
        await dataset.fetch(**kwargs)
    fetch.assert_not_awaited()


@pytest.mark.parametrize(
    ("value", "motivo"),
    [
        (None, "Chaves ausentes"),
        (2024.5, "anos integrais"),
        (0, "anos integrais"),
        (10000, "anos integrais"),
    ],
)
def test_aggregation_annual_key_requires_integral_year(value, motivo):
    frame = desmatamento_frame()
    frame["ano"] = [value]
    with levanta_exatamente(ContractViolationError, match=motivo):
        _desmatamento_aggregation.aggregate(frame, "prodes")


@pytest.mark.parametrize("meta,error", [(None, ContractViolationError), (object(), AttributeError)])
async def test_fetch_missing_coverage_cannot_be_aggregated(meta, error):
    dataset = DesmatamentoDataset()
    dataset.info.sources[0].fetch_fn = AsyncMock(return_value=(desmatamento_frame(), meta))
    with pytest.raises(error):
        await dataset.fetch()


async def test_fetch_deterministic_rejected_before_source():
    dataset = DesmatamentoDataset()
    fetch = AsyncMock()
    dataset.info.sources[0].fetch_fn = fetch
    async with deterministic("2020-01-01"):
        with levanta_exatamente(InvalidParameterError, match="deterministic"):
            await dataset.fetch()
    fetch.assert_not_awaited()


async def test_fetch_unknown_keyword_before_source():
    dataset = DesmatamentoDataset()
    fetch = AsyncMock()
    dataset.info.sources[0].fetch_fn = fetch
    with levanta_exatamente(TypeError, match="desconhecidos"):
        await dataset.fetch(unknown=True)
    fetch.assert_not_awaited()


async def test_selecao_acima_do_limite_recusada_antes_da_descarga(monkeypatch):
    calls = install_desmatamento_wfs(monkeypatch, desmatamento_features("prodes_cerrado"))

    with levanta_exatamente(
        ContractViolationError,
        match="a seleção tem 6 ocorrências no WFS, acima de max_registros=2",
    ):
        await DesmatamentoDataset().fetch("Cerrado", tipo="prodes", max_registros=2)

    assert [request.url.params.get("resultType") for request, _ in calls] == ["hits"]
    hits = []
    for limite in (6, None):
        calls.clear()
        with sem_excecao():
            frame = await DesmatamentoDataset().fetch(
                "Cerrado", tipo="prodes", max_registros=limite
            )
        assert not frame.empty
        hits.append(sum(request.url.params.get("resultType") == "hits" for request, _ in calls))
    assert hits[0] == hits[1] + 1


def _avisos_de_codigo(avisos: list[warnings.WarningMessage]) -> list[str]:
    return [str(aviso.message) for aviso in avisos if "cod_municipio" in str(aviso.message)]


async def _deter_cerrado(monkeypatch, features):
    install_desmatamento_wfs(monkeypatch, features)
    with warnings.catch_warnings(record=True) as avisos, sem_excecao():
        warnings.simplefilter("always")
        frame, meta = await DesmatamentoDataset().fetch("Cerrado", tipo="deter", return_meta=True)
    return frame, meta, _avisos_de_codigo(avisos)


async def test_deter_cerrado_resolve_cod_municipio_pelo_nome(monkeypatch):
    features = desmatamento_features("deter_cerrado")
    frame, meta, avisos = await _deter_cerrado(monkeypatch, features)
    assert frame["municipio_id"].isna().all() and frame["cod_municipio"].notna().all()
    for municipio, uf, codigo in frame[["municipio", "uf", "cod_municipio"]].itertuples(
        index=False
    ):
        info = municipalities.ibge_para_municipio(int(codigo))
        assert info is not None
        assert (regions.remover_acentos(info["nome"]).upper(), info["uf"]) == (
            regions.remover_acentos(municipio).upper(),
            uf,
        )
    assert meta.source_details["aggregation"]["cod_municipio_pelo_nome"] == {
        "pares_pelo_nome": 4,
        "pares_sem_codigo": [],
        "linhas_sem_codigo": 0,
    }
    assert avisos == [] and not [a for a in meta.validation_warnings if "cod_municipio" in a]


async def test_deter_sem_codigo_pelo_nome_avisa_uma_vez_com_a_contagem(monkeypatch):
    features = copy.deepcopy(desmatamento_features("deter_cerrado"))
    nomes = [
        "MUNICIPIO INEXISTENTE",
        "SANTO ANTÔNIO DO LEVERGER",
        "FORTALEZA DO TABOCÃO",
        "MUNICIPIO INEXISTENTE",
    ]
    for feature, nome in zip(features, nomes, strict=False):
        feature["properties"]["municipality"] = nome
    frame, meta, avisos = await _deter_cerrado(monkeypatch, features)
    codigos = dict(zip(zip(frame["municipio"], frame["uf"]), frame["cod_municipio"], strict=True))
    assert codigos[("SANTO ANTÔNIO DO LEVERGER", "MT")] == 5107800
    assert codigos[("FORTALEZA DO TABOCÃO", "TO")] == 1708254
    assert pd.isna(codigos[("MUNICIPIO INEXISTENTE", "MT")])
    assert pd.isna(codigos[("MUNICIPIO INEXISTENTE", "TO")])
    aviso = (
        "desmatamento: cod_municipio nulo em 2 linha(s) do DETER sem municipio_id, porque o nome "
        "não está no cadastro de municípios na UF: MUNICIPIO INEXISTENTE/MT, "
        "MUNICIPIO INEXISTENTE/TO"
    )
    assert avisos == [aviso]
    assert [a for a in meta.validation_warnings if "cod_municipio" in a] == [aviso]


def test_deter_com_municipio_id_nao_usa_o_nome_e_resolve_cada_par_uma_vez(monkeypatch):
    chamadas = []
    resolver = municipalities.resolver_municipio

    def contar(*argumentos):
        chamadas.append(argumentos)
        return resolver(*argumentos)

    monkeypatch.setattr(municipalities, "resolver_municipio", contar)
    source = pd.concat(
        [
            desmatamento_frame("deter", municipio="Nome Errado"),
            desmatamento_frame("deter", municipio_id=None),
            desmatamento_frame("deter", municipio_id=None, data=pd.Timestamp("2024-06-16")),
        ],
        ignore_index=True,
    )
    frame, details = _desmatamento_aggregation.aggregate(source, "deter")
    assert len(frame) == 3 and frame["cod_municipio"].tolist() == [1500602] * 3
    assert chamadas == [("Altamira", "PA")]
    assert details["cod_municipio_pelo_nome"] == {
        "pares_pelo_nome": 1,
        "pares_sem_codigo": [],
        "linhas_sem_codigo": 0,
    }
    assert _desmatamento_aggregation.aviso_cod_municipio(details) is None
