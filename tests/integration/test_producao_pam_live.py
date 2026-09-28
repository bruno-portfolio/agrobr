from __future__ import annotations

from typing import Any

import httpx
import pytest

from agrobr import contracts, datasets
from agrobr.sync import datasets as sync_datasets

pytestmark = [pytest.mark.integration, pytest.mark.timeout(300)]

SIDRA_PAM_2024_BRASIL = (
    "https://apisidra.ibge.gov.br/values/t/5457/n1/all/v/214/p/2024/c782/{codigo}"
)


@pytest.mark.parametrize(
    "produto,codigo,nome",
    [
        ("cana", "40106", "Cana-de-açúcar"),
        ("mandioca", "40119", "Mandioca"),
        ("laranja", "40151", "Laranja"),
    ],
)
async def test_producao_anual_novas_culturas_brasil(produto: str, codigo: str, nome: str):
    async with httpx.AsyncClient(timeout=60) as client:
        response = await client.get(SIDRA_PAM_2024_BRASIL.format(codigo=codigo))
    assert response.status_code == 200
    sidra = response.json()[1]
    assert (sidra["D4N"], sidra["D2N"], sidra["MN"]) == (nome, "Quantidade produzida", "Toneladas")
    producao = int(sidra["V"])
    frame, meta = await datasets.producao_anual(produto, ano=2024, nivel="brasil", return_meta=True)
    assert len(frame) == 1
    assert frame.loc[0, "producao"] == producao
    assert frame.loc[0, "localidade"] == "Brasil"
    assert frame.loc[0, "unidade_producao"] == "ton"
    assert meta.selected_source == "ibge_pam"
    assert meta.attempted_sources == ["ibge_pam"]
    assert meta.records_count == 1
    assert not meta.from_cache
    contracts.validate_dataset(frame, "producao_anual")


@pytest.mark.parametrize(
    "produto,producao",
    [
        ("cana", 418569112),
        ("mandioca", 1666659),
        ("laranja", 12002362),
    ],
)
async def test_producao_anual_novas_culturas_uf(produto: str, producao: int):
    frame, meta = await datasets.producao_anual(produto, ano=2024, uf="SP", return_meta=True)
    assert len(frame) == 1
    assert frame.loc[0, "localidade"] == "São Paulo"
    assert frame.loc[0, "producao"] == producao
    assert meta.selected_source == "ibge_pam"
    contracts.validate_dataset(frame, "producao_anual")


@pytest.mark.parametrize(
    "produto,producao,zeros",
    [
        ("cana", 19095, 5),
        ("mandioca", 361016, 0),
        ("laranja", 4043, 12),
    ],
)
async def test_producao_anual_novas_culturas_municipios(produto: str, producao: int, zeros: int):
    frame, meta = await datasets.producao_anual(
        produto, ano=2024, nivel="municipio", uf="RO", return_meta=True
    )
    assert len(frame) == 52
    assert frame["producao"].sum() == producao
    assert (frame["producao"] == 0).sum() == zeros
    assert frame["localidade"].str.endswith(" - RO").all()
    assert meta.selected_source == "ibge_pam"
    assert meta.records_count == 52
    contracts.validate_dataset(frame, "producao_anual")


@pytest.mark.parametrize(
    "ano,producao,unidade",
    [
        (1974, 29594708, "mil_frutos"),
        (2000, 106651289, "mil_frutos"),
        (2001, 16983436, "ton"),
    ],
)
async def test_producao_anual_laranja_historica(ano: int, producao: int, unidade: str):
    frame, meta = await datasets.producao_anual(
        "laranja", ano=ano, nivel="brasil", return_meta=True
    )
    assert len(frame) == 1
    assert frame.loc[0, "producao"] == producao
    assert frame.loc[0, "unidade_producao"] == unidade
    if ano == 1974:
        assert frame["area_plantada"].isna().all()
    assert meta.selected_source == "ibge_pam"
    contracts.validate_dataset(frame, "producao_anual")


async def test_producao_anual_nova_cultura_ultimo_periodo():
    frame, meta = await datasets.producao_anual("mandioca", nivel="brasil", return_meta=True)
    assert len(frame) == 1
    assert frame.loc[0, "ano"] >= 2024
    assert frame.loc[0, "producao"] > 0
    assert meta.selected_source == "ibge_pam"
    contracts.validate_dataset(frame, "producao_anual")


def test_producao_anual_nova_cultura_sync():
    result: Any = sync_datasets.producao_anual("cana", ano=2024, uf="SP", return_meta=True)
    frame, meta = result
    assert len(frame) == 1
    assert frame.loc[0, "producao"] == 418569112
    assert meta.selected_source == "ibge_pam"
    contracts.validate_dataset(frame, "producao_anual")
