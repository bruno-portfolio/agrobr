from __future__ import annotations

import hashlib
import json
import warnings
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, unquote, urlsplit

import httpx
import pytest

from agrobr import bcb, datasets
from agrobr.bcb import client, models
from tests.helpers import assert_replay_served, install_replay_http, sem_excecao

ORACULO = Path(__file__).parents[1] / "golden_data" / "bcb" / "oraculo_20260923"
MANIFEST = json.loads((ORACULO / "manifest.json").read_bytes())
CASO = next(caso for caso in MANIFEST["cases"] if caso["id"] == "sicor_custeio_milho_mt")
ARQUIVO = CASO["requests"][0]["file"]
RECIBO = next(recurso for recurso in MANIFEST["resources"] if recurso["file"] == ARQUIVO)
ASYNC_CLIENT_REAL = httpx.AsyncClient


def _parametros(url: str) -> dict[str, str]:
    return dict(parse_qsl(urlsplit(url).query, keep_blank_values=True))


def _consulta_esperada() -> dict[str, str]:
    return {
        "$format": "json",
        **CASO["requests"][0]["match"]["params"],
    }


def _manifesto(consulta: str, recursos: list[dict[str, Any]]) -> bytes:
    return json.dumps(
        {"query": consulta, "resources": recursos},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _confere_topo(meta: Any, pedidos: list[tuple[str, bytes]]) -> None:
    detalhes = meta.source_details
    assert _parametros(meta.source_url) == _consulta_esperada()
    assert urlsplit(meta.source_url).path == urlsplit(CASO["requests"][0]["match"]["path"]).path
    recursos = detalhes["resources"]
    assert [
        (unquote(recurso["url"]), recurso["sha256"], recurso["bytes"]) for recurso in recursos
    ] == [(unquote(url), hashlib.sha256(corpo).hexdigest(), len(corpo)) for url, corpo in pedidos]
    manifesto = _manifesto(meta.source_url, recursos)
    assert (meta.raw_content_hash, meta.raw_content_size) == (
        hashlib.sha256(manifesto).hexdigest(),
        len(manifesto),
    )
    assert detalhes["hash_kind"] == "resource_manifest_sha256"
    assert detalhes["resource_bytes"] == sum(len(corpo) for _, corpo in pedidos)
    horas = [datetime.fromisoformat(recurso["fetched_at"]) for recurso in recursos]
    assert meta.fetched_at == meta.fetch_timestamp == max(horas)


async def test_credito_rural_de_uma_pagina_publica_o_manifesto(monkeypatch):
    corpo = (ORACULO / ARQUIVO).read_bytes()
    assert hashlib.sha256(corpo).hexdigest() == RECIBO["sha256"]
    visto = install_replay_http(monkeypatch, CASO, ORACULO)
    with sem_excecao(), warnings.catch_warnings():
        warnings.simplefilter("ignore")
        _, meta = await bcb.credito_rural("milho", safra="2024/25", uf="MT", return_meta=True)
    assert_replay_served(visto)
    assert len(meta.source_details.get("resources", [])) == 1
    [pedido] = meta.source_details["resources"]
    assert _parametros(pedido["url"])["$top"] == str(client.SICOR_RECORD_LIMIT)
    _confere_topo(meta, [(pedido["url"], corpo)])


def _servir_fatiado(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, bytes]]:
    registros = json.loads((ORACULO / ARQUIVO).read_bytes())["value"]
    monkeypatch.setattr(client, "SICOR_RECORD_LIMIT", len(registros))
    pedidos: list[tuple[str, bytes]] = []

    def responder(request: httpx.Request) -> httpx.Response:
        filtro = _parametros(str(request.url))["$filter"]
        marca = "MesEmissao eq '"
        if filtro.endswith("'") and filtro.rsplit(" and ", 1)[-1].startswith(marca):
            mes = filtro.rsplit(" and ", 1)[-1][len(marca) : -1]
            corpo = json.dumps({"value": [r for r in registros if r["MesEmissao"] == mes]}).encode()
        else:
            corpo = (ORACULO / ARQUIVO).read_bytes()
        pedidos.append((str(request.url), corpo))
        return httpx.Response(200, content=corpo, headers={"content-type": "application/json"})

    monkeypatch.setattr(
        client.httpx,
        "AsyncClient",
        lambda **kwargs: ASYNC_CLIENT_REAL(transport=httpx.MockTransport(responder), **kwargs),
    )
    return pedidos


async def test_credito_rural_fatiado_por_mes_lista_cada_pagina(monkeypatch):
    pedidos = _servir_fatiado(monkeypatch)
    with sem_excecao(), warnings.catch_warnings():
        warnings.simplefilter("ignore")
        _, meta = await bcb.credito_rural("milho", safra="2024/25", uf="MT", return_meta=True)
    assert len(pedidos) == 13
    assert [_parametros(url)["$filter"].rsplit(" and ", 1)[-1] for url, _ in pedidos[1:]] == [
        f"MesEmissao eq '{mes:02d}'" for mes in range(1, 13)
    ]
    _confere_topo(meta, pedidos)


async def test_dataset_credito_rural_herda_a_proveniencia(monkeypatch):
    pedidos = _servir_fatiado(monkeypatch)
    with sem_excecao(), warnings.catch_warnings():
        warnings.simplefilter("ignore")
        _, meta = await datasets.credito_rural("milho", safra="2024/25", uf="MT", return_meta=True)
    _confere_topo(meta, pedidos)


async def test_credito_rural_total_publica_o_manifesto_e_os_meses(monkeypatch):
    registros = [
        dict.fromkeys(models.SICOR_TOTAL_CAMPOS, 0)
        | {"nomeUF": "MT", "AnoEmissao": "2024", "MesEmissao": mes, "cdPrograma": "0001"}
        for mes in ("08", "09")
    ]
    corpo = json.dumps({"value": registros}).encode()
    pedidos: list[tuple[str, bytes]] = []

    def responder(request: httpx.Request) -> httpx.Response:
        pedidos.append((str(request.url), corpo))
        return httpx.Response(200, content=corpo, headers={"content-type": "application/json"})

    monkeypatch.setattr(
        client.httpx,
        "AsyncClient",
        lambda **kwargs: ASYNC_CLIENT_REAL(transport=httpx.MockTransport(responder), **kwargs),
    )
    with sem_excecao(), warnings.catch_warnings():
        warnings.simplefilter("ignore")
        _, meta = await bcb.credito_rural_total(safra="2024/25", uf="MT", return_meta=True)
    [(url, _)] = pedidos
    assert meta.source_details.get("hash_kind") == "resource_manifest_sha256"
    assert urlsplit(meta.source_url).path.endswith(f"/{client.TOTAL_ENDPOINT}")
    assert _parametros(meta.source_url) == {
        k: v for k, v in _parametros(url).items() if k != "$top"
    }
    manifesto = _manifesto(meta.source_url, meta.source_details["resources"])
    assert meta.raw_content_hash == hashlib.sha256(manifesto).hexdigest()
    assert meta.source_details["meses"] == {
        "primeiro": "2024-08",
        "ultimo": "2024-09",
        "quantidade": 2,
    }
    assert [
        (recurso["sha256"], recurso["bytes"]) for recurso in meta.source_details["resources"]
    ] == [(hashlib.sha256(corpo).hexdigest(), len(corpo))]
