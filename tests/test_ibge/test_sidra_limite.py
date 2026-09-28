from __future__ import annotations

import re
from datetime import datetime
from typing import Any
from urllib.parse import unquote

import httpx
import pytest

from agrobr.exceptions import InvalidParameterError, ParseError
from agrobr.ibge import _helpers, agregados, client
from agrobr.models import MetaInfo
from tests.helpers import levanta_exatamente, sem_excecao

TABELA = "6957"
LIMITE = 50_000
MUNICIPIOS_PR = [f"41{indice:05d}" for indice in range(399)]
MUNICIPIOS_SC = [f"42{indice:05d}" for indice in range(10)]
MUNICIPIOS_MG = [f"31{indice:05d}" for indice in range(853)]
TODOS = MUNICIPIOS_PR + MUNICIPIOS_SC + MUNICIPIOS_MG
WAF = 4000
REJEITADO = "<html><head><title>Request Rejected</title></head><body>The requested URL was rejected.</body></html>"


def _instalar(
    monkeypatch: pytest.MonkeyPatch,
    *,
    por_localidade: int,
    recusa: str | None = None,
    url_max: int | None = None,
    rejeitados: list[str] | None = None,
) -> list[str]:
    """SIDRA simulada: cada variável vale ``por_localidade`` valores em cada município pedido.

    Acima de 50.000 valores, responde o 400 com a conta, como a SIDRA; ``recusa`` troca o 400 por outro motivo.
    Com ``url_max``, a URL mais longa recebe o HTML "Request Rejected" do WAF, com 200, e vai para ``rejeitados``.
    """
    pedidos: list[str] = []

    async def send(
        _client: httpx.AsyncClient, request: httpx.Request, **_kwargs: Any
    ) -> httpx.Response:
        url = unquote(str(request.url))
        pedidos.append(url)
        if request.url.host != "apisidra.ibge.gov.br":
            if url.endswith(f"/{TABELA}/localidades/N6"):
                return httpx.Response(
                    200,
                    json=[{"id": codigo} for codigo in TODOS],
                    request=request,
                )
            return httpx.Response(200, json=[], request=request)
        if recusa is not None:
            return httpx.Response(400, text=recusa, request=request)
        if url_max is not None and len(str(request.url)) > url_max:
            (rejeitados if rejeitados is not None else []).append(url)
            return httpx.Response(
                200, text=REJEITADO, headers={"content-type": "text/html"}, request=request
            )
        variaveis = re.search(r"/v/([^/]+)", url).group(1).split(",")
        codigo = re.search(r"/n6/([^/]+)", url).group(1)
        localidades = {"in N3 41": MUNICIPIOS_PR, "in N3 31": MUNICIPIOS_MG, "all": TODOS}.get(
            codigo, codigo.split(",")
        )
        categorias = re.search(r"/c226/([^/]+)", url).group(1).split(",")
        valores = len(variaveis) * len(localidades) * len(categorias) * por_localidade
        if valores > LIMITE:
            texto = f"Quantidade de valores solicitados: {valores} excedeu o limite: {LIMITE}"
            return httpx.Response(400, text=texto, request=request)
        linhas = [
            {"D1C": localidade, "D3C": variavel, "V": "1"}
            for variavel in variaveis
            for localidade in localidades
        ]
        linhas.append({"D1C": "comum", "D3C": "comum", "V": "1"})
        return httpx.Response(200, json=linhas, request=request)

    monkeypatch.setattr(httpx.AsyncClient, "send", send)
    return pedidos


def _sidra(pedidos: list[str]) -> list[str]:
    return [url for url in pedidos if "apisidra.ibge.gov.br" in url]


async def test_pedido_acima_do_limite_divide_por_variavel_e_junta_sem_duplicar(monkeypatch):
    pedidos = _instalar(monkeypatch, por_localidade=55)

    with sem_excecao():
        frame = await client.fetch_sidra(
            TABELA, "6", "in N3 41", ["10084", "10085", "10089"], "all", {"226": "all"}
        )

    sidra = _sidra(pedidos)
    assert [re.search(r"/v/([^/]+)", url).group(1) for url in sidra] == [
        "10084,10085,10089",
        "10084,10085",
        "10089",
    ]
    assert sorted(set(frame["D3C"])) == ["10084", "10085", "10089", "comum"]
    assert len(frame) == 3 * 399 + 1
    assert [unquote(fatia.get("url", "")) for fatia in frame.attrs.get("fatias", [])] == sidra[1:]
    meta = MetaInfo(
        source="ibge", source_url="", source_method="httpx", fetched_at=datetime(2026, 9, 27)
    )
    _helpers.registrar_canal(meta, frame)
    consultas = meta.source_details.get("consultas", [])
    assert [unquote(consulta.get("url", "")) for consulta in consultas] == sidra[1:]
    assert all(len(consulta.get("sha256", "")) == 64 for consulta in consultas)
    assert (meta.raw_content_hash, meta.raw_content_size) == (None, 0)


async def test_pedido_acima_do_limite_divide_pelos_municipios_da_uf(monkeypatch):
    pedidos = _instalar(monkeypatch, por_localidade=300)

    with sem_excecao():
        frame = await client.fetch_sidra(TABELA, "6", "in N3 41", "10084", "all", {"226": "all"})

    fatias = [re.search(r"/n6/([^/]+)", url).group(1).split(",") for url in _sidra(pedidos)[1:]]
    assert [len(fatia) for fatia in fatias] == [166, 166, 67]
    assert sorted(codigo for fatia in fatias for codigo in fatia) == MUNICIPIOS_PR
    assert set(frame["D1C"]) - {"comum"} == set(MUNICIPIOS_PR)


async def test_outro_400_da_sidra_e_erro_de_parametro_com_o_motivo(monkeypatch):
    motivo = "Classificação 999 não existe na tabela 6957"
    pedidos = _instalar(monkeypatch, por_localidade=1, recusa=motivo)

    with levanta_exatamente(InvalidParameterError, "Classificação 999 não existe"):
        await client.fetch_sidra(TABELA, "6", "4106902", "10084", "all", {"999": "all"})

    assert [url for url in pedidos if "servicodados" in url] == []


async def test_pedido_acima_do_limite_sem_o_que_dividir_orienta(monkeypatch):
    _instalar(monkeypatch, por_localidade=60_000)

    with levanta_exatamente(
        InvalidParameterError, "não tem variável, categoria listada nem localidade"
    ):
        await client.fetch_sidra(TABELA, "6", "4106902", "10084", "all", {"226": "all"})


async def test_pedido_acima_do_limite_divide_pelas_categorias_listadas(monkeypatch):
    pedidos = _instalar(monkeypatch, por_localidade=60)
    categorias = [str(codigo) for codigo in range(46500, 46503)]

    with sem_excecao():
        frame = await client.fetch_sidra(
            TABELA, "6", "in N3 41", "10084", "all", {"226": categorias}
        )

    assert [re.search(r"/c226/([^/]+)", url).group(1) for url in _sidra(pedidos)] == [
        ",".join(categorias),
        "46500,46501",
        "46502",
    ]
    assert len(frame) == 399 + 1


async def test_pedido_de_todas_as_localidades_divide_pela_lista_da_tabela(monkeypatch):
    pedidos = _instalar(monkeypatch, por_localidade=150)

    with sem_excecao():
        frame = await client.fetch_sidra(TABELA, "6", "all", "10084", "all", {"226": "all"})

    assert set(frame["D1C"]) - {"comum"} == set(TODOS)
    assert [url for url in pedidos if url.endswith(f"/{TABELA}/localidades/N6")]


async def test_lista_de_localidades_fora_do_formato_e_erro_de_layout(monkeypatch):
    async def send(
        _client: httpx.AsyncClient, request: httpx.Request, **_kwargs: Any
    ) -> httpx.Response:
        return httpx.Response(200, json={"erro": "sem lista"}, request=request)

    monkeypatch.setattr(httpx.AsyncClient, "send", send)
    with levanta_exatamente(ParseError, "sem a lista de localidades"):
        await agregados.fetch_localidades(TABELA, "6")


async def test_parte_por_localidade_cabe_no_teto_da_url(monkeypatch):
    rejeitados: list[str] = []
    pedidos = _instalar(monkeypatch, por_localidade=59, url_max=WAF, rejeitados=rejeitados)

    with sem_excecao():
        frame = await client.fetch_sidra(TABELA, "6", "in N3 31", "10084", "all", {"226": "all"})

    partes = _sidra(pedidos)[1:]
    assert rejeitados == []
    assert len(partes) == 3
    assert max(len(parte) for parte in partes) <= client.SIDRA_URL_MAX
    assert set(frame["D1C"]) - {"comum"} == set(MUNICIPIOS_MG)


async def test_url_longa_rejeitada_pelo_waf_divide_por_localidade(monkeypatch):
    rejeitados: list[str] = []
    pedidos = _instalar(monkeypatch, por_localidade=1, url_max=WAF, rejeitados=rejeitados)

    with sem_excecao():
        frame = await client.fetch_sidra(
            TABELA, "6", ",".join(MUNICIPIOS_MG), ["10084", "10085"], "all", {"226": "all"}
        )

    sidra = _sidra(pedidos)
    assert rejeitados == sidra[:1]
    assert max(len(parte) for parte in sidra[1:]) <= client.SIDRA_URL_MAX
    assert all("/v/10084,10085/" in parte for parte in sidra[1:])
    assert frame.attrs.get("canal") == "sidra"
    assert set(frame["D1C"]) - {"comum"} == set(MUNICIPIOS_MG)


async def test_url_curta_rejeitada_nao_se_divide(monkeypatch):
    rejeitados: list[str] = []
    pedidos = _instalar(monkeypatch, por_localidade=1, url_max=10, rejeitados=rejeitados)

    with sem_excecao():
        frame = await client.fetch_sidra(TABELA, "6", "4106902", "10084", "all", {"226": "all"})

    assert set(_sidra(pedidos)) == set(rejeitados) == {rejeitados[0]}
    assert frame.attrs.get("canal") == "servicodados"
