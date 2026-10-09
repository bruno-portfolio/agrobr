from __future__ import annotations

import asyncio
import json
import threading
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import pytest

from agrobr import ibama
from agrobr.constants import CacheSettings
from agrobr.ibama import _cache
from tests import helpers
from tests.test_ibama import oficial
from tests.test_ibama.test_oficial import instalar


def _manifesto() -> dict[str, object]:
    return json.loads((CacheSettings().cache_dir / "ibama" / "termo_embargo.json").read_text())


async def test_coleta_dentro_do_ttl_sai_do_cache_com_a_hora_da_coleta(monkeypatch):
    pytest.importorskip("geopandas")
    pedidos = instalar(monkeypatch)

    primeiro, meta1 = await ibama.embargos(uf="DF", return_meta=True)
    segundo, meta2 = await ibama.embargos(uf="MT", return_meta=True)
    geo, meta3 = await ibama.embargos_geo(uf="MT", return_meta=True)

    assert len(pedidos) == 1
    assert [meta.from_cache for meta in (meta1, meta2, meta3)] == [False, True, True]
    assert meta1.fetched_at == meta2.fetched_at == meta3.fetched_at
    assert meta2.fetch_timestamp == meta1.fetched_at
    assert meta1.raw_content_hash == meta2.raw_content_hash == _manifesto()["sha256"]
    assert meta2.source_url == meta1.source_url == oficial.manifest()["url"]
    assert not primeiro.empty and not segundo.empty and not geo.empty


async def test_use_cache_false_baixa_de_novo_sem_ler_nem_gravar(monkeypatch):
    pytest.importorskip("geopandas")
    pedidos = instalar(monkeypatch)

    _, meta1 = await ibama.embargos(use_cache=False, return_meta=True)
    assert not (CacheSettings().cache_dir / "ibama").exists()
    await ibama.embargos()
    _, meta3 = await ibama.embargos_geo(uf="DF", use_cache=False, return_meta=True)

    assert len(pedidos) == 3
    assert (meta1.from_cache, meta3.from_cache) == (False, False)


@pytest.mark.parametrize("estrago", ["vencido", "futuro", "hash", "manifesto"])
async def test_cache_vencido_ou_estragado_baixa_de_novo(monkeypatch, estrago):
    pedidos = instalar(monkeypatch)
    await ibama.embargos()
    manifesto_path = CacheSettings().cache_dir / "ibama" / "termo_embargo.json"
    manifesto = _manifesto()
    agora = datetime.now(UTC)
    if estrago == "vencido":
        manifesto["fetched_at"] = (agora - timedelta(hours=1, seconds=1)).isoformat()
    elif estrago == "futuro":
        manifesto["fetched_at"] = (agora + timedelta(minutes=5)).isoformat()
    elif estrago == "hash":
        manifesto["sha256"] = "0" * 64
    manifesto_path.write_text(
        "{corrompido" if estrago == "manifesto" else json.dumps(manifesto), encoding="utf-8"
    )

    _, meta = await ibama.embargos(return_meta=True)

    assert len(pedidos) == 2
    assert meta.from_cache is False
    assert _manifesto()["fetched_at"] == meta.fetched_at.isoformat()


async def test_chamadas_simultaneas_baixam_uma_vez(monkeypatch):
    pedidos = instalar(monkeypatch)
    _cache._LOCKS.clear()

    resultados = await asyncio.gather(*(ibama.embargos(uf=uf) for uf in ("DF", "MT", "PR")))

    assert len(pedidos) == 1
    assert all(not df.empty for df in resultados)


async def test_geo_vazio_tem_os_dtypes_do_geo_cheio(monkeypatch):
    pytest.importorskip("geopandas")
    instalar(monkeypatch)

    with helpers.sem_excecao():
        cheio = await ibama.embargos_geo(uf="MT")
        vazio = await ibama.embargos_geo(bbox=(-30.0, -20.0, -29.9, -19.9))

    assert not cheio.empty and vazio.empty
    assert vazio.dtypes.astype(str).to_dict() == cheio.dtypes.astype(str).to_dict()
    assert vazio.crs == cheio.crs


async def test_cancelar_a_gravacao_nao_solta_o_lock_com_a_thread_gravando(monkeypatch):
    loop = asyncio.get_running_loop()
    comecou = asyncio.Event()
    liberar = threading.Event()
    gravando = threading.Event()
    sobreposicoes: list[bool] = []

    def gravar(_coleta):
        if not comecou.is_set():
            gravando.set()
            loop.call_soon_threadsafe(comecou.set)
            liberar.wait(5)
            gravando.clear()
        else:
            sobreposicoes.append(gravando.is_set())

    coleta = _cache.Coleta(b"csv", "https://exemplo/cache", datetime.now(UTC), False)
    monkeypatch.setattr(_cache, "_ler", lambda *_: None)
    monkeypatch.setattr(_cache, "_baixar", AsyncMock(return_value=coleta))
    monkeypatch.setattr(_cache, "_gravar", gravar)

    primeira = asyncio.create_task(_cache.obter_embargos_csv())
    await asyncio.wait_for(comecou.wait(), 2)
    primeira.cancel()
    segunda = asyncio.create_task(_cache.obter_embargos_csv())
    await asyncio.sleep(0.1)
    liberar.set()

    with pytest.raises(asyncio.CancelledError):
        await primeira
    assert await segunda == coleta
    assert sobreposicoes == [False]
