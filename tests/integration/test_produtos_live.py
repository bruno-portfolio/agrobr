from __future__ import annotations

import asyncio
import hashlib
import time
from collections.abc import Callable
from datetime import UTC, date, datetime
from typing import Any

import pytest

from agrobr import conab, contracts, datasets, ibge
from agrobr.conab.progresso import api as progresso_api
from agrobr.conab.progresso import models as progresso_models
from tests import helpers
from tests.integration import test_datasets_live

pytestmark = [pytest.mark.integration, pytest.mark.timeout(300)]

C7_DATASETS = {
    "serie_historica_safra",
    "custo_producao",
    "condicao_lavouras",
    "destinos_anec",
    "preco_diario",
}
ROTATING_DATASETS = {"embarques_anec", "preco_atacado"}
OFF_MATRIX_DATASETS = {"zoneamento_agricola"}
COLLECTION_DATE = datetime.now(UTC).date()
ISO_WEEK = COLLECTION_DATE.isocalendar().week
DESMATAMENTO_UF_SOURCE = (
    "https://agenciadenoticias.ibge.gov.br/agencia-sala-de-imprensa/"
    "2013-agencia-de-noticias/releases/"
    "25798-ibge-lanca-mapa-inedito-de-biomas-e-sistema-costeiro-marinho"
)
DESMATAMENTO_UFS = {
    "Amazônia": "PA",
    "Caatinga": "BA",
    "Cerrado": "MT",
    "Mata Atlântica": "MG",
    "Pampa": "RS",
    "Pantanal": "MS",
}
BULLETIN_SOURCE_BASE = (
    "https://www.gov.br/conab/pt-br/atuacao/informacoes-agropecuarias/safras/progresso-de-safra/"
)
BULLETIN_CAPTURES = {
    "2025-09-27": (
        "acompanhamento-das-lavouras-22-09-a-28-09-25/plantio-e-colheita-21-09-a-27-09",
        "ae5932cbef54b2b141b8b45adc1e66c6ff091104725989438f6583ff4fa5e050",
    ),
    "2025-10-25": (
        "acompanhamento-das-lavouras-20-10-a-26-10-25/plantio-e-colheita-19-10-a-25-10",
        "1bf677c392d1816209c098fd5e066fe528e4ecf6c5146ee3281922ce09e64e9a",
    ),
    "2025-11-29": (
        "acompanhamento-das-lavouras-24-11-a-30-11-25/plantio-e-colheita-23-11-a-29-11",
        "06c1a290394f1144e9666d8182e1654a2d6846877448440cd0ea998a1dc68551",
    ),
    "2025-12-27": (
        "acompanhamento-das-lavouras-22-12-a-28-12-25/plantio-e-colheita-21-12-a-27-12",
        "83c5fa234e34eb681c5c00f336b78d5750464bc30cf76a4d6056f7f37f28ae65",
    ),
    "2026-01-24": (
        "acompanhamento-das-lavouras-19-01-a-25-01-26/plantio-e-colheita-18-01-a-24-01",
        "5b93e41489d64ee927d92a5e8d81bac48a956f47c4e3715a594942aa7618e6e8",
    ),
    "2026-02-21": (
        "acompanhamento-das-lavouras-16-02-a-22-02-26/plantio-e-colheita-15-02-a-21-02",
        "d42d49715bb393c14f67855db7af2e79d08a3bd982201e7a0ce02f75e2ec8ff9",
    ),
    "2026-03-28": (
        "acompanhamento-das-lavouras-23-03-a-29-03-26/plantio-e-colheita-22-03-a-28-03.xlsx",
        "b9b81daf0a9a835411a5d10d25195cab8548a4d728c5b19870949cc99711a1d0",
    ),
    "2026-04-25": (
        "acompanhamento-das-lavouras-20-04-a-26-04-26/plantio-e-colheita-18-04-a-24-04.xlsx",
        "42f615045f6a52cd3222aa3dffa748160e8f2a07f41fbcaf1b9b590d4681b46d",
    ),
    "2026-05-29": (
        "acompanhamento-das-lavouras-25-05-a-31-05-26/plantio-e-colheita-23-05-a-29-05",
        "37dc352042422ed0972cdb4b1df1fd561551aad816961bbd0a4b5f7905ecd807",
    ),
    "2026-06-26": (
        "acompanhamento-das-lavouras-22-06-a-28-06-26/plantio-e-colheita-20-06-a-26-06",
        "d8a60e19366606f96778c593abbe08a78d6a3f67fb901f67a23d821786ca3b86",
    ),
    "2026-07-24": (
        "acompanhamento-das-lavouras-20-07-a-26-07-26/plantio-e-colheita-18-07-a-24-07",
        "8ee3626638bc8b576f7c281bb30e6a719948e8220dab1c17a27642ce9a5f2977",
    ),
    "2026-08-28": (
        "acompanhamento-das-lavouras-24-08-a-30-08-26/plantio-e-colheita-22-08-a-28-08",
        "6a4fadb57ddb38c5ae6bc989f29172469b568a14d4ef3f0b39955d6c911a7ba2",
    ),
    "2026-09-11": (
        "acompanhamento-das-lavouras-07-09-a-13-09-26/plantio-e-colheita-05-09-a-11-09",
        "cd1daadde92d1fcd90ea0b69ffac0424aef7dea4c2860f0495fa6cf88d4cbeff",
    ),
}
BULLETIN_WINDOWS = {
    "algodao": (
        "2025-11-29",
        "2025-12-27",
        "2026-01-24",
        "2026-02-21",
        "2026-03-28",
        "2026-04-25",
        "2026-05-29",
        "2026-06-26",
        "2026-07-24",
        "2026-08-28",
        "2026-09-11",
    ),
    "arroz": (
        "2025-09-27",
        "2025-10-25",
        "2025-11-29",
        "2025-12-27",
        "2026-01-24",
        "2026-02-21",
        "2026-03-28",
        "2026-04-25",
        "2026-05-29",
        "2026-06-26",
    ),
    "feijao_1": (
        "2025-09-27",
        "2025-10-25",
        "2025-11-29",
        "2025-12-27",
        "2026-01-24",
        "2026-02-21",
        "2026-03-28",
        "2026-04-25",
        "2026-05-29",
    ),
    "milho_1": (
        "2025-09-27",
        "2025-10-25",
        "2025-11-29",
        "2025-12-27",
        "2026-01-24",
        "2026-02-21",
        "2026-03-28",
        "2026-04-25",
        "2026-05-29",
        "2026-06-26",
        "2026-07-24",
    ),
    "milho_2": (
        "2026-01-24",
        "2026-02-21",
        "2026-03-28",
        "2026-04-25",
        "2026-05-29",
        "2026-06-26",
        "2026-07-24",
        "2026-08-28",
        "2026-09-11",
    ),
    "soja": (
        "2025-09-27",
        "2025-10-25",
        "2025-11-29",
        "2025-12-27",
        "2026-01-24",
        "2026-02-21",
        "2026-03-28",
        "2026-04-25",
        "2026-05-29",
    ),
    "trigo": (
        "2026-05-29",
        "2026-06-26",
        "2026-07-24",
        "2026-08-28",
        "2025-09-27",
        "2025-10-25",
        "2025-11-29",
        "2025-12-27",
    ),
}
RecordProperty = Callable[[str, Any], None]
LivePolicy = Callable[[str, dict[str, Any]], None]


def _uses_ibge(dataset_name: str) -> bool:
    return any(
        source.name == "ibge" or source.name.startswith("ibge_")
        for source in datasets.get_dataset(dataset_name).info.sources
    )


def _rotates(dataset_name: str) -> bool:
    return dataset_name in ROTATING_DATASETS or _uses_ibge(dataset_name)


def _weekly_products(products: list[str], week: int) -> list[str]:
    ordered = sorted(products, key=lambda value: (hashlib.sha256(value.encode()).digest(), value))
    if not ordered:
        return []
    groups = (len(ordered) + 4) // 5
    start = ((week - 1) % groups) * 5
    return ordered[start : start + 5]


def _product_cases(week: int) -> list[tuple[str, str]]:
    cases = []
    for name in datasets.list_datasets():
        if name in C7_DATASETS or name in OFF_MATRIX_DATASETS:
            continue
        products = datasets.list_products(name)
        selected = _weekly_products(products, week) if _rotates(name) else products
        cases.extend((name, product) for product in selected)
    return cases


def _product_query(dataset_name: str, product: str) -> tuple[tuple[Any, ...], dict[str, Any]]:
    """Preserva período/agregação; desmatamento usa UF por bioma, demais usam Brasil."""
    args, original = test_datasets_live.LIVE_CASES[dataset_name]
    kwargs = {key: value for key, value in original.items() if key not in {"uf", "estado"}}
    if dataset_name == "desmatamento":
        kwargs["uf"] = DESMATAMENTO_UFS[product]
    if dataset_name == "precos_diesel":
        kwargs["produto"] = product
        return args, kwargs
    assert args, f"Consulta de referência de {dataset_name!r} sem argumento de produto"
    return (product, *args[1:]), kwargs


PRODUCT_CASES = _product_cases(ISO_WEEK)
GENERAL_CASES = [
    (name, product)
    for name, product in PRODUCT_CASES
    if name not in {"cadastro_rural", "progresso_safra"}
]


def _calendar_products(cultures: list[str], month: int) -> tuple[set[str], set[str], set[str]]:
    """Aplica a janela mensal heurística observada; as bordas não exigem presença."""
    catalogue = set(datasets.list_products("progresso_safra"))
    assert set(BULLETIN_WINDOWS) == catalogue, "Atualizar janelas para o catálogo"
    canonical = {progresso_models.normalizar_cultura(product): product for product in catalogue}
    labels = {progresso_models.normalizar_cultura(culture) for culture in cultures}
    unknown = labels - canonical.keys()
    assert not unknown, f"Culturas oficiais fora do catálogo: {sorted(unknown)}"
    observed = {canonical[label] for label in labels}
    windows = {
        product: tuple(date.fromisoformat(day).month for day in days)
        for product, days in BULLETIN_WINDOWS.items()
    }
    expected = {product for product, months in windows.items() if month in months[1:-1]}
    transitions = {
        product for product, months in windows.items() if month in (months[0], months[-1])
    }
    missing = expected - observed
    assert not missing, f"Culturas no interior da janela no mês {month} ausentes: {sorted(missing)}"
    return observed, catalogue - expected - transitions, transitions - observed


@pytest.mark.parametrize(
    "dataset_name,produto",
    GENERAL_CASES,
    ids=[f"{name}-{product}" for name, product in GENERAL_CASES],
)
async def test_produto_live(
    dataset_name: str,
    produto: str,
    record_property: RecordProperty,
    apply_live_policy: LivePolicy,
):
    sampled = [product for name, product in PRODUCT_CASES if name == dataset_name]
    record_property("dataset", dataset_name)
    record_property("produto", produto)
    record_property("iso_week", ISO_WEEK)
    record_property("sampled_products", ",".join(sampled))
    args, kwargs = _product_query(dataset_name, produto)
    if dataset_name == "censo_agropecuario_municipal_1985":
        coverage = await ibge.cobertura_censo_agro_municipal_1985()
        kwargs["uf"] = coverage[produto][0]
        record_property("uf_cobertura", kwargs["uf"])
    if dataset_name == "custo_sociobiodiversidade":
        inventory = await conab.catalogo_sociobiodiversidade(produto)
        identified = inventory.loc[inventory.status == "identified"].sort_values(
            ["ano", "indice_aba"], ascending=[False, True], kind="stable"
        )
        assert not identified.empty, f"Nenhuma aba identificada de sociobiodiversidade: {produto}"
        selected = identified.iloc[0]
        kwargs.update(ano=int(selected.ano), uf=str(selected.uf), aba=str(selected.aba))
        for field in ("ano", "uf", "aba"):
            record_property(field, kwargs[field])
    if "uf" in kwargs:
        record_property("query_scope", kwargs["uf"])
    elif {"uf", "estado"} & test_datasets_live.LIVE_CASES[dataset_name][1].keys():
        record_property("query_scope", "Brasil")
    if dataset_name == "desmatamento":
        record_property("uf", kwargs["uf"])
        record_property("uf_source", DESMATAMENTO_UF_SOURCE)
    apply_live_policy(dataset_name, kwargs)
    await helpers.assert_live_product_result(
        dataset_name, produto, args, kwargs, 3 if _uses_ibge(dataset_name) else 0.5, record_property
    )


async def test_progresso_safra_calendario_live(record_property: RecordProperty):
    record_property("dataset", "progresso_safra")
    record_property("produto", "calendario")
    record_property("query_scope", "fonte: conab.progresso.api.progresso_safra; boletim completo")
    record_property("collection_date", COLLECTION_DATE.isoformat())
    record_property("iso_week", ISO_WEEK)
    record_property("calendar_source", BULLETIN_SOURCE_BASE)
    record_property("calendar_rule", "observed_months_2025_2026; transition_edges")
    record_property("sampled_products", ",".join(datasets.list_products("progresso_safra")))
    await asyncio.sleep(0.5)
    started = time.perf_counter()
    try:
        frame, meta = await progresso_api.progresso_safra(produto=None, return_meta=True)
    finally:
        record_property("seconds", round(time.perf_counter() - started, 3))
    record_property("records_count", len(frame))
    record_property("selected_source", meta.selected_source)
    record_property("attempted_sources", ",".join(meta.attempted_sources))
    assert len(frame) > 0, "Boletim mais recente de progresso_safra vazio"
    contracts.validate_dataset(frame, "progresso_safra")
    week = date.fromisoformat(max(frame["semana_atual"]))
    record_property("semana_atual", week.isoformat())
    observed, outside, transitions = _calendar_products(frame["cultura"].tolist(), week.month)
    record_property("observed_products", ",".join(sorted(observed)))
    for product in sorted(outside):
        record_property("fora_do_boletim", product)
    for product in sorted(transitions):
        record_property("transicao", product)
    record_property("live_matrix_status", "validated")
