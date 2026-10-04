from __future__ import annotations

import json
import ssl
from collections.abc import Callable, Coroutine
from typing import Any
from unittest.mock import AsyncMock
from urllib.parse import parse_qs, urlsplit

import httpx
import pytest

from agrobr.alt.sicar import client, models
from agrobr.alt.sicar.models import PAGE_SIZE, WFS_BASE
from agrobr.exceptions import ParseError
from tests.helpers import collect_failures

from .test_api import geo_capture

CQL_MUNICIPIO = "municipio ILIKE '%Cabrobó%'"


def pagina(start: int, count: int) -> bytes:
    features = [
        {
            "type": "Feature",
            "id": f"sicar_imoveis_df.{index}",
            "properties": {
                "cod_imovel": f"DF-{index}",
                "status_imovel": "AT",
                "dat_criacao": "2020-01-01T00:00:00Z",
                "area": 1,
                "uf": "DF",
                "municipio": "Brasilia",
                "cod_municipio_ibge": 5300108,
                "tipo_imovel": "IRU",
            },
        }
        for index in range(start, start + count)
    ]
    return json.dumps(
        {"type": "FeatureCollection", "numberReturned": count, "features": features}
    ).encode()


def consulta(url: str) -> dict[str, str]:
    assert url.startswith(WFS_BASE + "?")
    return {name: values[0] for name, values in parse_qs(urlsplit(url).query).items()}


def servidor_geo(
    total: int, pedidos: list[dict[str, str]]
) -> Callable[..., Coroutine[Any, Any, bytes]]:
    async def fetch_wfs(url: str, **_kwargs: Any) -> bytes:
        query = consulta(url)
        pedidos.append(query)
        if query.get("resultType") == "hits":
            return f'<wfs:FeatureCollection numberMatched="{total}"/>'.encode()
        return pagina(int(query.get("startIndex", "0")), int(query["count"]))

    return fetch_wfs


def test_propriedades_pedidas_seguem_o_schema_de_cada_uf():
    sem_atualizacao = {"PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"}
    with collect_failures() as check:
        for uf in sorted(models.UFS_VALIDAS):
            for geo in (False, True):
                with check((uf, geo)):
                    query = consulta(
                        client._build_wfs_url(uf, property_names=models.property_names(uf, geo=geo))
                    )
                    pedidas = query.get("propertyName", "").split(",")
                    assert query.get("typeNames") == f"sicar:sicar_imoveis_{uf.lower()}"
                    assert ("data_atualizacao" in pedidas) == (uf not in sem_atualizacao)
                    assert ("geo_area_imovel" in pedidas) == geo
                    assert set(pedidas) - {"data_atualizacao", "geo_area_imovel"} == {
                        "cod_imovel",
                        "status_imovel",
                        "dat_criacao",
                        "area",
                        "condicao",
                        "uf",
                        "municipio",
                        "cod_municipio_ibge",
                        "m_fiscal",
                        "tipo_imovel",
                    }
                    assert len(pedidas) == len(set(pedidas))


async def test_sondagem_le_number_matched_e_pede_hits(monkeypatch: pytest.MonkeyPatch):
    casos = [
        (b'<?xml version="1.0"?><wfs:FeatureCollection numberMatched="21006"/>', 21006),
        (b"<wfs:FeatureCollection numberMatched=100 numberReturned=0/>", 100),
    ]
    with collect_failures() as check:
        for resposta, esperado in casos:
            with check(esperado):
                fetch = AsyncMock(return_value=resposta)
                monkeypatch.setattr(client, "fetch_wfs", fetch)
                assert await client.fetch_hits("DF", CQL_MUNICIPIO) == esperado
                query = consulta(fetch.await_args.args[0])
                assert query.get("resultType") == "hits"
                assert query.get("CQL_FILTER") == CQL_MUNICIPIO
        with check("sem numberMatched"), pytest.raises(ParseError, match="numberMatched"):
            monkeypatch.setattr(
                client, "fetch_wfs", AsyncMock(return_value=b"<wfs:FeatureCollection/>")
            )
            await client.fetch_hits("SP")


async def test_varredura_tabular_pagina_ordena_e_pausa_depois_da_quinta(
    monkeypatch: pytest.MonkeyPatch,
):
    pedidos: list[dict[str, str]] = []
    pausas: list[float] = []

    async def fetch_wfs(url: str, **_kwargs: Any) -> bytes:
        query = consulta(url)
        pedidos.append(query)
        if query.get("resultType") == "hits":
            return b'<wfs:FeatureCollection numberMatched="7"/>'
        inicio = int(query.get("startIndex", "0"))
        return pagina(inicio, 1 if inicio < 7 else 0)

    async def sleep(delay: float) -> None:
        pausas.append(delay)

    monkeypatch.setattr(client, "PAGE_SIZE", 1)
    monkeypatch.setattr(client, "fetch_wfs", fetch_wfs)
    monkeypatch.setattr(client.asyncio, "sleep", sleep)
    detalhes: dict[str, Any] = {}

    pages, url = await client.fetch_imoveis("DF", "status_imovel='AT'", source_details=detalhes)

    assert len(pages) == 7
    assert [query.get("startIndex") for query in pedidos[1:]] == [str(i) for i in range(7)]
    assert {query.get("count") for query in pedidos[1:]} == {"1"}
    assert {query.get("sortBy") for query in pedidos} == {"cod_imovel"}
    assert {query.get("CQL_FILTER") for query in pedidos} == {"status_imovel='AT'"}
    assert pausas == [client.THROTTLE_DELAY, client.THROTTLE_DELAY]
    assert detalhes == {"anunciados": 7, "features_unicas": 7}
    assert (consulta(url).get("startIndex"), consulta(url).get("count")) == (None, None)
    monkeypatch.setattr(
        client, "fetch_wfs", AsyncMock(return_value=b'<wfs:FeatureCollection numberMatched="0"/>')
    )
    vazio, url_vazio = await client.fetch_imoveis("DF")
    assert vazio == [] and consulta(url_vazio).get("typeNames") == "sicar:sicar_imoveis_df"


async def test_geo_pagina_pelo_limite_e_pede_geometria(monkeypatch: pytest.MonkeyPatch):
    casos: list[tuple[int | None, int, list[int], list[int], int]] = [
        (5_000, 50_000, [5_000], [1], 0),
        (PAGE_SIZE, 50_000, [PAGE_SIZE], [1], 0),
        (PAGE_SIZE + 1, 20_000, [PAGE_SIZE, 1], [2], 1),
        (15_000, 50_000, [PAGE_SIZE, 5_000], [2], 1),
        (50_000, 10_500, [PAGE_SIZE, 500], [2], 1),
        (None, 12_345, [PAGE_SIZE, 2_345], [1, 1], 1),
        (None, 0, [], [0], 1),
    ]
    with collect_failures() as check:
        for limite, total, contagens, lotes, sondagens in casos:
            with check((limite, total)):
                pedidos: list[dict[str, str]] = []
                monkeypatch.setattr(client, "fetch_wfs", servidor_geo(total, pedidos))
                batches = [
                    batch async for batch in client.stream_imoveis_geo("MT", max_features=limite)
                ]
                paginas = [query for query in pedidos if query.get("resultType") != "hits"]
                assert len(pedidos) - len(paginas) == sondagens
                assert [query.get("count") for query in paginas] == [str(c) for c in contagens]
                assert [query.get("startIndex") for query in paginas] == [
                    str(index * PAGE_SIZE) for index in range(len(contagens))
                ]
                assert all("geo_area_imovel" in query.get("propertyName", "") for query in paginas)
                assert all(query.get("outputFormat") == "application/json" for query in paginas)
                assert all(query.get("srsName") == "EPSG:4326" for query in paginas)
                assert all("srsName" not in query for query in pedidos if query not in paginas)
                assert [len(pages) for pages, _url in batches] == lotes
                assert all(url.startswith(WFS_BASE) for _pages, url in batches)
                acumulado, url = await client.fetch_imoveis_geo("MT", max_features=limite)
                assert len(acumulado) == len(contagens)
                assert url.startswith(WFS_BASE)


async def test_geo_sem_limite_pausa_depois_da_quinta_pagina(monkeypatch: pytest.MonkeyPatch):
    paginas = client.THROTTLE_AFTER_PAGE + 2
    pausas: list[float] = []

    async def fetch_wfs(url: str, **_kwargs: Any) -> bytes:
        if "resultType=hits" in url:
            return f'<wfs:FeatureCollection numberMatched="{paginas}"/>'.encode()
        return pagina(int(consulta(url)["startIndex"]), 1)

    async def sleep(delay: float) -> None:
        pausas.append(delay)

    monkeypatch.setattr(client, "PAGE_SIZE", 1)
    monkeypatch.setattr(client, "fetch_wfs", fetch_wfs)
    monkeypatch.setattr(client.asyncio, "sleep", sleep)
    lotes = [len(pages) async for pages, _url in client.stream_imoveis_geo("MT", max_features=None)]

    assert lotes == [1] * paginas
    assert pausas == [client.THROTTLE_DELAY, client.THROTTLE_DELAY]


def _sem_total(corpo: bytes) -> bytes:
    documento = json.loads(corpo)
    documento.pop("numberMatched")
    return json.dumps(documento).encode()


@pytest.mark.parametrize(
    ("pedido", "preparar", "levanta"),
    [(4, bytes, True), (3, bytes, False), (4, _sem_total, False)],
    ids=["curta_com_total", "completa", "curta_sem_total"],
)
async def test_pagina_unica_confere_as_feicoes_contra_o_total_publicado(
    monkeypatch: pytest.MonkeyPatch, pedido: int, preparar: Callable[[bytes], bytes], levanta: bool
):
    corpo = preparar(geo_capture("df_geo_srs4326_count3.json"))
    monkeypatch.setattr(client, "fetch_wfs", AsyncMock(return_value=corpo))
    if levanta:
        with pytest.raises(ParseError, match="3 unicas x 4 anunciadas"):
            await client.fetch_imoveis_geo("DF", max_features=pedido)
    else:
        pages, _url = await client.fetch_imoveis_geo("DF", max_features=pedido)
        assert pages == [corpo]


async def test_sessoes_do_sicar_usam_tls_e_timeout_proprios(monkeypatch: pytest.MonkeyPatch):
    sessions: list[Any] = []

    async def fetch_wfs(url: str, **kwargs: Any) -> bytes:
        sessions.append(kwargs["client"])
        if "resultType=hits" in url:
            return b'<wfs:FeatureCollection numberMatched="2"/>'
        return pagina(0, 2)

    monkeypatch.setattr(client, "fetch_wfs", fetch_wfs)
    await client.fetch_imoveis("DF")
    await client.fetch_imoveis_geo("DF", max_features=10)

    assert len(sessions) == 3
    for session in sessions:
        assert isinstance(session, httpx.AsyncClient)
        assert session._transport._pool._ssl_context is client._ssl_ctx  # type: ignore[attr-defined]
        assert session.timeout.read == 180.0
    assert client._ssl_ctx.verify_mode == ssl.CERT_REQUIRED
    assert client._ssl_ctx.check_hostname is True
    assert "AES256-GCM-SHA384" in {cipher["name"] for cipher in client._ssl_ctx.get_ciphers()}
