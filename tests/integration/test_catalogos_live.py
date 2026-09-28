from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from typing import Any

import pytest

from agrobr import cepea, conab, constants, datasets, deral, zarc
from agrobr.zarc import models as zarc_models

pytestmark = [pytest.mark.integration, pytest.mark.timeout(300)]

SAFRA_REFERENCIA = "2025/2026"
RecordProperty = Callable[[str, Any], None]
LivePolicy = Callable[[str, dict[str, Any]], None]


@pytest.fixture(autouse=True)
async def _intervalo_entre_consultas() -> None:
    await asyncio.sleep(0.5)


@pytest.mark.parametrize("produto", datasets.list_products("serie_historica_safra"))
async def test_serie_historica_produto_publicado(
    produto: str, record_property: RecordProperty, apply_live_policy: LivePolicy
):
    kwargs = {"produto": produto, "inicio": 2023, "fim": 2024}
    apply_live_policy("serie_historica_safra", kwargs)
    frame = await datasets.get_dataset("serie_historica_safra").fetch(**kwargs)
    record_property("records_count", len(frame))
    assert len(frame) > 0, f"Série histórica sem dados para {produto!r} em 2023–2024"


@pytest.mark.parametrize("cultura", datasets.list_products("custo_producao"))
async def test_custo_producao_recursos_publicados(
    cultura: str, record_property: RecordProperty, apply_live_policy: LivePolicy
):
    apply_live_policy("custo_producao", {"cultura": cultura})
    frame = await conab.catalogo_custos(cultura)
    record_property("records_count", len(frame))
    assert len(frame) > 0, f"Catálogo de custos sem recurso para {cultura!r}"


async def test_condicao_lavouras_produtos_observados(
    record_property: RecordProperty, apply_live_policy: LivePolicy
):
    apply_live_policy("condicao_lavouras", {})
    frame = await deral.condicao_lavouras()
    assert len(frame) > 0, "DERAL não retornou condições de lavouras"
    observed = set(frame["produto"])
    advertised = set(datasets.list_products("condicao_lavouras"))
    record_property("produtos_observados", ",".join(sorted(observed)))
    record_property("records_count", len(frame))
    assert observed <= advertised, (
        f"Produtos DERAL fora do catálogo: {sorted(observed - advertised)}"
    )


@pytest.mark.parametrize("produto", datasets.list_products("destinos_anec"))
async def test_destinos_anec_produto_publicado(
    produto: str, record_property: RecordProperty, apply_live_policy: LivePolicy
):
    kwargs = {"produto": produto, "ano": 2026, "semana": 13, "use_cache": False}
    apply_live_policy("destinos_anec", kwargs)
    frame = await datasets.destinos_anec(**kwargs)
    record_property("records_count", len(frame))
    assert len(frame) > 0, f"Destinos ANEC sem dados para {produto!r} em W13/2026"


async def _culturas_zarc_observadas(
    cultura: str,
    uf: str,
    safra: str,
    record_property: RecordProperty,
    apply_live_policy: LivePolicy,
) -> set[str]:
    kwargs = {"cultura": cultura, "uf": uf, "safra": safra, "use_cache": False}
    apply_live_policy("zoneamento_agricola", kwargs)
    start = time.perf_counter()
    try:
        frame, meta = await datasets.zoneamento_agricola(**kwargs, return_meta=True)
    finally:
        record_property("segundos", round(time.perf_counter() - start, 3))
    observed = set(meta.source_details["parser"]["culturas_observadas"])
    record_property("culturas_observadas", ",".join(sorted(observed)))
    record_property("records_count", len(frame))
    assert len(frame) > 0, f"ZARC sem registros para {cultura!r}, {uf!r}, {safra!r}"
    return observed


@pytest.mark.timeout(300)
async def test_zarc_culturas_anuais_publicadas(
    record_property: RecordProperty, apply_live_policy: LivePolicy
):
    observed = await _culturas_zarc_observadas(
        "soja", "MT", SAFRA_REFERENCIA, record_property, apply_live_policy
    )
    expected = {
        cultura
        for cultura in zarc_models._CULTURAS_ANUAIS.values()
        if zarc_models.SAFRAS_POR_CULTURA[cultura][0]
        <= SAFRA_REFERENCIA
        <= zarc_models.SAFRAS_POR_CULTURA[cultura][1]
    }
    advertised = set(zarc.culturas())
    assert expected <= observed, f"Culturas anuais ausentes: {sorted(expected - observed)}"
    assert observed <= advertised, (
        f"Culturas anuais fora do catálogo: {sorted(observed - advertised)}"
    )


@pytest.mark.timeout(300)
async def test_zarc_culturas_perenes_publicadas(
    record_property: RecordProperty, apply_live_policy: LivePolicy
):
    observed = await _culturas_zarc_observadas(
        "cafe_arabica", "MG", "perene", record_property, apply_live_policy
    )
    expected = zarc_models.CULTURAS_PERENES
    advertised = set(zarc.culturas())
    assert expected <= observed, f"Culturas perenes ausentes: {sorted(expected - observed)}"
    assert observed <= advertised, (
        f"Culturas perenes fora do catálogo: {sorted(observed - advertised)}"
    )


@pytest.mark.parametrize("produto", datasets.list_products("preco_diario"))
async def test_preco_diario_produto_publicado(
    produto: str, record_property: RecordProperty, apply_live_policy: LivePolicy
):
    kwargs = {"produto": produto, "force_refresh": True}
    apply_live_policy("preco_diario", kwargs)
    frame = await datasets.preco_diario(**kwargs)
    record_property("records_count", len(frame))
    assert len(frame) > 0, f"Preço diário sem dados para {produto!r}"


@pytest.mark.parametrize("produto", sorted(constants.CEPEA_PRODUTOS))
async def test_cepea_indicador_publicado(produto: str, record_property: RecordProperty):
    frame = await cepea.indicador(produto, force_refresh=True)
    record_property("records_count", len(frame))
    assert len(frame) > 0, f"Indicador CEPEA sem dados para {produto!r}"


@pytest.mark.parametrize("produto", sorted(constants.CONAB_PRODUTOS))
async def test_conab_safras_produto_publicado(produto: str, record_property: RecordProperty):
    frame = await conab.safras(produto)
    record_property("records_count", len(frame))
    assert len(frame) > 0, f"Safras CONAB sem dados para {produto!r}"
