from __future__ import annotations

import warnings
from unittest.mock import AsyncMock

import pandas as pd
import pytest

from agrobr.ibge import api, censo_api, client, pesquisas_api

SIDRA_VAZIA = pd.DataFrame(
    columns=["NC", "NN", "MC", "MN", "V", *(f"D{i}{c}" for i in range(1, 6) for c in "CN")]
)

CONSULTAS = {
    "pam": (api.pam, ("soja",), {"ano": 2023}),
    "ppm": (api.ppm, ("bovino",), {"ano": 2023}),
    "silvicultura": (pesquisas_api.silvicultura, ("carvao",), {"ano": 2023}),
    "extracao_vegetal": (pesquisas_api.extracao_vegetal, ("acai",), {"ano": 2023}),
    "censo_agro": (censo_api.censo_agro, ("efetivo_rebanho",), {}),
    "censo_agro_historico": (censo_api.censo_agro_historico, ("uso_terra",), {}),
    "abate": (api.abate, ("bovino",), {"trimestre": "202301"}),
    "leite_trimestral": (pesquisas_api.leite_trimestral, (), {"trimestre": "202301"}),
}


async def _cache_key(monkeypatch, nome, **recorte):
    monkeypatch.setattr(client, "fetch_sidra", AsyncMock(return_value=SIDRA_VAZIA.copy()))
    funcao, args, kwargs = CONSULTAS[nome]
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        _, meta = await funcao(*args, **kwargs, **recorte, return_meta=True)
    return meta.cache_key


@pytest.mark.parametrize("nome", ["pam", "ppm", "silvicultura", "extracao_vegetal", "censo_agro"])
async def test_cache_key_muda_com_uf_e_nivel(monkeypatch, nome):
    chaves = [
        await _cache_key(monkeypatch, nome, nivel="brasil"),
        await _cache_key(monkeypatch, nome, nivel="uf"),
        await _cache_key(monkeypatch, nome, uf="MT", nivel="uf"),
        await _cache_key(monkeypatch, nome, uf="MT", nivel="municipio"),
    ]
    assert len(set(chaves)) == 4
    assert await _cache_key(monkeypatch, nome, uf="mt", nivel="uf") == chaves[2]


@pytest.mark.parametrize("nome", ["abate", "leite_trimestral"])
async def test_cache_key_muda_com_uf(monkeypatch, nome):
    brasil = await _cache_key(monkeypatch, nome)
    mt = await _cache_key(monkeypatch, nome, uf="MT")
    assert brasil != mt
    assert await _cache_key(monkeypatch, nome, uf="mt") == mt


async def test_cache_key_da_pam_muda_com_as_variaveis(monkeypatch):
    producao = await _cache_key(monkeypatch, "pam", variaveis=["producao"])
    assert producao != await _cache_key(monkeypatch, "pam")


async def test_cache_key_do_censo_historico_muda_com_uf_e_nivel(monkeypatch):
    chaves = [
        await _cache_key(monkeypatch, "censo_agro_historico", nivel=nivel)
        for nivel in ("brasil", "regiao", "uf")
    ]
    chaves.append(await _cache_key(monkeypatch, "censo_agro_historico", uf="MT", nivel="uf"))
    assert len(set(chaves)) == 4
