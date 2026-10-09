from __future__ import annotations

import json
import sys
import warnings
from collections.abc import Callable
from types import SimpleNamespace
from typing import Any

import httpx
import pandas as pd
import pytest

from agrobr import deterministic, embrapa_solos
from agrobr.exceptions import InvalidParameterError, ParseError
from agrobr.http import wfs_transport
from tests.test_embrapa_solos import oficial

pytest.importorskip("geopandas")

SC = (-49.4, -28.8, -49.2, -28.6)
FLORIPA = (-48.52, -27.62, -48.5, -27.6)
OCEANO = (-30.0, -20.0, -29.9, -19.9)
PREFIXO_PERFIS = {"max_registros": 120, "tamanho_pagina": 50}
PREFIXO_MAPA = {"max_registros": 60, "tamanho_pagina": 25}
Transformar = Callable[[dict[str, Any], bytes], bytes]
Alterar = Callable[[dict[str, Any]], None]


def instalar(
    monkeypatch: pytest.MonkeyPatch, transformar: Transformar | None = None
) -> tuple[dict[str, Any], list[str]]:
    data = oficial.manifest()
    pedidos: list[str] = []

    def responder(request: httpx.Request) -> httpx.Response:
        pedidos.append(str(request.url))
        entry = oficial.entrada(data, request.url.query.decode())
        if entry is None:
            return httpx.Response(404, request=request)
        body = (oficial.GOLDEN / entry["file"]).read_bytes()
        if transformar is not None:
            body = transformar(entry, body)
        return httpx.Response(200, content=body, request=request)

    namespace = SimpleNamespace(**vars(httpx))
    namespace.AsyncClient = lambda **kwargs: httpx.AsyncClient(
        transport=httpx.MockTransport(responder), **kwargs
    )
    monkeypatch.setattr(wfs_transport, "httpx", namespace)
    return data, pedidos


def alterar_feicoes(alterar: Alterar) -> Transformar:
    def transformar(entry: dict[str, Any], body: bytes) -> bytes:
        if entry["role"] != "page":
            return body
        page = json.loads(body)
        for feature in page["features"]:
            alterar(feature)
        return json.dumps(page, ensure_ascii=False).encode()

    return transformar


def trocar(identificador: str, campo: str, valor: Any) -> Alterar:
    def alterar(feature: dict[str, Any]) -> None:
        if feature["id"] == identificador:
            feature["properties"][campo] = valor

    return alterar


def sem_argila(feature: dict[str, Any]) -> None:
    if feature["id"] == "perfis_pronasolos_2020.30":
        del feature["properties"]["argila"]


def crs_sirgas(entry: dict[str, Any], body: bytes) -> bytes:
    if entry["role"] != "page":
        return body
    return body.replace(b"urn:ogc:def:crs:EPSG::4326", b"urn:ogc:def:crs:EPSG::4674")


def hits_com_observacoes(entry: dict[str, Any], body: bytes) -> bytes:
    if entry["role"] != "hits_before":
        return body
    return body.replace(b'numberReturned="0"', b'numberReturned="1"')


def hits_mudou() -> Transformar:
    vistos: list[str] = []

    def transformar(entry: dict[str, Any], body: bytes) -> bytes:
        if not entry["role"].startswith("hits"):
            return body
        vistos.append(entry["file"])
        if vistos.count(entry["file"]) < 2:
            return body
        return body.replace(b'numberMatched="34464"', b'numberMatched="34465"')

    return transformar


def ultima_pagina_repetida(entry: dict[str, Any], body: bytes) -> bytes:
    if entry["role"] != "page" or entry["params"]["startIndex"] != "99":
        return body
    page = json.loads(body)
    page["features"] = [page["features"][0]] * len(page["features"])
    return json.dumps(page, ensure_ascii=False).encode()


def duas_ufs(feature: dict[str, Any]) -> None:
    trocar("perfis_pronasolos_2020.10", "uf", " sc ")(feature)
    trocar("perfis_pronasolos_2020.20", "uf", "XX")(feature)


@pytest.fixture
def servidor(monkeypatch: pytest.MonkeyPatch) -> tuple[dict[str, Any], list[str]]:
    return instalar(monkeypatch)


async def chamar(call: Callable[..., Any], **kwargs: Any) -> tuple[Any, list[str]]:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        try:
            result = await call(**kwargs)
        except Exception as exc:
            result = exc
    return result, [str(item.message) for item in caught]


def esperado(data: dict[str, Any], produto: str, caso: str) -> list[dict[str, Any]]:
    return [oficial.linha(produto, feature) for feature in oficial.feicoes(data, caso)]


async def test_perfis_publica_cada_atributo_da_fonte(servidor: tuple[dict[str, Any], list[str]]):
    data, _ = servidor
    linhas = esperado(data, "perfis", "perfis_prefixo")
    assert len(linhas) == 120
    resultado, avisos = await chamar(embrapa_solos.perfis, return_meta=True, **PREFIXO_PERFIS)
    assert isinstance(resultado, tuple) and len(resultado) == 2, resultado
    frame, meta = resultado
    assert list(frame.columns) == [coluna for coluna, _ in oficial.colunas("perfis")]
    assert oficial.publicado(frame) == linhas
    assert all(frame[medida].dtype == pd.Series([""]).dtype for medida in oficial.MEDIDAS)
    assert "NULL" in set(frame["carbono_organico"].dropna())
    assert meta.records_count == 120
    assert meta.fetch_timestamp == meta.fetched_at
    assert any("prefixo remoto de 120 de 34464" in aviso for aviso in avisos), avisos


async def test_filtro_de_uf_atua_no_prefixo_remoto(servidor: tuple[dict[str, Any], list[str]]):
    data, _ = servidor
    linhas = [linha for linha in esperado(data, "perfis", "perfis_prefixo") if linha["uf"] == "SC"]
    assert linhas
    resultado, avisos = await chamar(embrapa_solos.perfis, uf="sc", **PREFIXO_PERFIS)
    assert not isinstance(resultado, Exception), resultado
    assert oficial.publicado(resultado) == linhas
    assert any(f"filtro local retornou {len(linhas)} linhas" in aviso for aviso in avisos), avisos


async def test_mapa_publica_cada_atributo_da_fonte(servidor: tuple[dict[str, Any], list[str]]):
    data, _ = servidor
    linhas = esperado(data, "mapa", "mapa_prefixo")
    assert len(linhas) == 60
    resultado, avisos = await chamar(embrapa_solos.mapa_solos, return_meta=True, **PREFIXO_MAPA)
    assert isinstance(resultado, tuple) and len(resultado) == 2, resultado
    frame, meta = resultado
    assert list(frame.columns) == [coluna for coluna, _ in oficial.colunas("mapa")]
    assert oficial.publicado(frame) == linhas
    assert str(frame["area_km2"].dtype) == "float64"
    assert meta.records_count == 60
    assert any("prefixo remoto de 60 de 2852" in aviso for aviso in avisos), avisos


async def test_filtro_de_ordem_usa_a_primeira_ordem(servidor: tuple[dict[str, Any], list[str]]):
    data, _ = servidor
    linhas = [
        linha for linha in esperado(data, "mapa", "mapa_prefixo") if linha["ordem1"] == "LATOSSOLOS"
    ]
    assert linhas
    for pedido in ("latossolo", "Latossolos", " LATOSSOLOS "):
        resultado, _ = await chamar(embrapa_solos.mapa_solos, ordem=pedido, **PREFIXO_MAPA)
        assert not isinstance(resultado, Exception), resultado
        assert oficial.publicado(resultado) == linhas


@pytest.mark.parametrize("funcao", [embrapa_solos.mapa_solos, embrapa_solos.mapa_solos_geo])
async def test_ordem_sem_correspondencia_em_leitura_completa_avisa(servidor, funcao):
    data, _ = servidor
    publicado = sorted(
        {feature["properties"]["ordem1"] for feature in oficial.feicoes(data, "mapa_bbox_floripa")}
    )
    assert publicado == ["ARGISSOLOS", "NEOSSOLOS"]
    resultado, avisos = await chamar(funcao, ordem="vertissolos", bbox=FLORIPA, return_meta=True)
    assert isinstance(resultado, tuple), resultado
    frame, meta = resultado
    assert frame.empty
    assert any("ordem='VERTISSOLOS'" in aviso and "leitura completa" in aviso for aviso in avisos)
    assert all(ordem in meta.validation_warnings[-1] for ordem in publicado)
    assert meta.validation_warnings[-1] in avisos


async def test_ordem_sem_correspondencia_em_prefixo_nao_afirma_leitura_completa(servidor):
    _, pedidos = servidor
    resultado, avisos = await chamar(
        embrapa_solos.mapa_solos, ordem="gleissolos", **PREFIXO_MAPA, return_meta=True
    )
    assert pedidos
    assert isinstance(resultado, tuple), resultado
    frame, meta = resultado
    assert frame.empty
    assert any("prefixo remoto" in aviso for aviso in avisos)
    assert not any("leitura completa" in aviso for aviso in [*avisos, *meta.validation_warnings])


async def test_perfis_com_bbox_saem_os_pontos_do_recorte(
    servidor: tuple[dict[str, Any], list[str]],
):
    data, _ = servidor
    dentro = [
        f for f in oficial.feicoes(data, "perfis_bbox_sc") if oficial.intersecta(f["geometry"], SC)
    ]
    assert dentro
    resultado, _ = await chamar(embrapa_solos.perfis_geo, bbox=SC, return_meta=True)
    assert isinstance(resultado, tuple) and len(resultado) == 2, resultado
    frame, meta = resultado
    assert oficial.publicado(frame) == [oficial.linha("perfis", f) for f in dentro]
    assert [oficial.coordenadas(g) for g in frame.geometry] == [
        f["geometry"]["coordinates"] for f in dentro
    ]
    assert frame.crs.to_epsg() == 4326
    assert meta.records_count == len(dentro)
    tabular, _ = await chamar(embrapa_solos.perfis, bbox=SC)
    assert oficial.publicado(tabular) == oficial.publicado(frame)
    assert "geometry" not in tabular.columns


async def test_perfis_geo_sem_bbox_publica_os_pontos_da_fonte(
    servidor: tuple[dict[str, Any], list[str]],
):
    data, _ = servidor
    features = oficial.feicoes(data, "perfis_geo_prefixo")
    assert len(features) == 5
    resultado, avisos = await chamar(embrapa_solos.perfis_geo, max_registros=5)
    assert not isinstance(resultado, Exception), resultado
    assert oficial.publicado(resultado) == [oficial.linha("perfis", f) for f in features]
    assert [oficial.coordenadas(g) for g in resultado.geometry] == [
        f["geometry"]["coordinates"] for f in features
    ]
    assert resultado.crs.to_epsg() == 4326
    assert any("prefixo remoto de 5 de 34464" in aviso for aviso in avisos), avisos


async def test_mapa_com_bbox_usa_a_intersecao_da_geometria(
    servidor: tuple[dict[str, Any], list[str]],
):
    data, _ = servidor
    candidatos = oficial.feicoes(data, "mapa_bbox_floripa")
    dentro = [f for f in candidatos if oficial.intersecta(f["geometry"], FLORIPA)]
    assert dentro and len(candidatos) == 2
    resultado, _ = await chamar(embrapa_solos.mapa_solos_geo, bbox=FLORIPA)
    assert not isinstance(resultado, Exception), resultado
    assert oficial.publicado(resultado) == [oficial.linha("mapa", f) for f in dentro]
    assert [oficial.coordenadas(g) for g in resultado.geometry] == [
        f["geometry"]["coordinates"] for f in dentro
    ]
    assert resultado.crs.to_epsg() == 4326


async def test_recorte_vazio_sai_com_o_esquema(servidor: tuple[dict[str, Any], list[str]]):
    _, pedidos = servidor
    colunas = [coluna for coluna, _ in oficial.colunas("perfis")]
    tabular, _ = await chamar(embrapa_solos.perfis, bbox=OCEANO)
    assert not isinstance(tabular, Exception), tabular
    assert tabular.empty and list(tabular.columns) == colunas
    geo, _ = await chamar(embrapa_solos.perfis_geo, bbox=OCEANO)
    assert not isinstance(geo, Exception), geo
    assert geo.empty and list(geo.columns) == [*colunas, "geometry"]
    assert geo.crs.to_epsg() == 4326
    assert len(pedidos) == 4


@pytest.mark.parametrize(
    ("opcoes", "erro"),
    [
        ({"uf": "XX"}, InvalidParameterError),
        ({"uf": 1}, InvalidParameterError),
        ({"bbox": (-47.0, -16.0, -48.0, -15.0)}, InvalidParameterError),
        ({"tamanho_pagina": 1001}, InvalidParameterError),
        ({"return_meta": "sim"}, InvalidParameterError),
        ({"formato": "csv"}, TypeError),
    ],
    ids=["uf_desconhecida", "uf_numero", "bbox_invertida", "pagina_grande", "meta_texto", "kwarg"],
)
async def test_parametro_invalido_recusado_antes_da_rede(
    servidor: tuple[dict[str, Any], list[str]], opcoes: dict[str, Any], erro: type[Exception]
):
    _, pedidos = servidor
    resultado, _ = await chamar(embrapa_solos.perfis, **opcoes)
    assert type(resultado) is erro, resultado
    assert pedidos == []


async def test_modo_deterministico_recusado_antes_da_rede(
    servidor: tuple[dict[str, Any], list[str]],
):
    _, pedidos = servidor
    async with deterministic("2026-09-07"):
        resultado, _ = await chamar(embrapa_solos.perfis, **PREFIXO_PERFIS)
    assert type(resultado) is InvalidParameterError, resultado
    assert pedidos == []


async def test_polars_ausente_recusado_antes_da_rede(
    servidor: tuple[dict[str, Any], list[str]], monkeypatch: pytest.MonkeyPatch
):
    _, pedidos = servidor
    monkeypatch.setitem(sys.modules, "polars", None)
    resultado, _ = await chamar(embrapa_solos.perfis, as_polars=True, **PREFIXO_PERFIS)
    assert type(resultado) is ImportError, resultado
    assert pedidos == []


async def test_as_polars_publica_as_mesmas_celulas(servidor: tuple[dict[str, Any], list[str]]):
    pl = pytest.importorskip("polars")
    data, _ = servidor
    resultado, _ = await chamar(embrapa_solos.perfis, as_polars=True, **PREFIXO_PERFIS)
    assert isinstance(resultado, pl.DataFrame), type(resultado)
    assert resultado.columns == [coluna for coluna, _ in oficial.colunas("perfis")]
    assert oficial.publicado(resultado.to_pandas()) == esperado(data, "perfis", "perfis_prefixo")


@pytest.mark.parametrize(
    ("produto", "caso", "opcoes", "alterar", "diagnostico"),
    [
        ("perfis", "perfis_prefixo", PREFIXO_PERFIS, duas_ufs, "unknown_uf"),
        (
            "perfis",
            "perfis_prefixo",
            PREFIXO_PERFIS,
            trocar("perfis_pronasolos_2020.15", "gcs_latitu", 95.0),
            "latitude_outside_range",
        ),
        (
            "mapa",
            "mapa_prefixo",
            PREFIXO_MAPA,
            trocar("brasil_solos_5m_20201104.10", "area_km2", -1.5),
            "negative_area",
        ),
    ],
    ids=["uf", "latitude", "area"],
)
async def test_valor_fora_do_padrao_sai_preservado_com_diagnostico(
    monkeypatch: pytest.MonkeyPatch,
    produto: str,
    caso: str,
    opcoes: dict[str, Any],
    alterar: Alterar,
    diagnostico: str,
):
    data, _ = instalar(monkeypatch, alterar_feicoes(alterar))
    features = oficial.feicoes(data, caso)
    for feature in features:
        alterar(feature)
    chamada = embrapa_solos.perfis if produto == "perfis" else embrapa_solos.mapa_solos
    resultado, _ = await chamar(chamada, return_meta=True, **opcoes)
    assert isinstance(resultado, tuple) and len(resultado) == 2, resultado
    frame, meta = resultado
    assert oficial.publicado(frame) == [oficial.linha(produto, feature) for feature in features]
    avisos = [aviso for aviso in meta.validation_warnings if diagnostico in aviso]
    assert avisos and avisos[0].endswith(f"Diagnóstico Embrapa {diagnostico}: 1 ocorrência(s)"), (
        meta.validation_warnings
    )


async def test_janela_repetida_sai_preservada_com_aviso(monkeypatch: pytest.MonkeyPatch):
    data, _ = instalar(monkeypatch, ultima_pagina_repetida)
    features = oficial.feicoes(data, "perfis_prefixo")
    repetida = features[99]
    features = [*features[:100], *[repetida] * 20]
    resultado, _ = await chamar(embrapa_solos.perfis, return_meta=True, **PREFIXO_PERFIS)
    assert isinstance(resultado, tuple) and len(resultado) == 2, resultado
    frame, meta = resultado
    assert oficial.publicado(frame) == [oficial.linha("perfis", feature) for feature in features]
    assert any("indistinguíveis preservadas" in aviso for aviso in meta.validation_warnings), (
        meta.validation_warnings
    )


@pytest.mark.parametrize(
    ("chamada", "opcoes", "transformar", "trecho"),
    [
        ("perfis", PREFIXO_PERFIS, lambda: alterar_feicoes(sem_argila), "argila"),
        ("perfis_geo", {"bbox": SC}, lambda: crs_sirgas, "CRS observado"),
        ("perfis", PREFIXO_PERFIS, lambda: hits_com_observacoes, "Hits"),
        ("perfis", PREFIXO_PERFIS, hits_mudou, "Contagem hits mudou"),
    ],
    ids=["propriedade_ausente", "crs_trocado", "hits_com_observacoes", "hits_mudou"],
)
async def test_pagina_fora_do_layout_vira_erro_de_layout(
    monkeypatch: pytest.MonkeyPatch,
    chamada: str,
    opcoes: dict[str, Any],
    transformar: Callable[[], Transformar],
    trecho: str,
):
    instalar(monkeypatch, transformar())
    resultado, _ = await chamar(getattr(embrapa_solos, chamada), **opcoes)
    assert isinstance(resultado, ParseError), repr(resultado)
    assert trecho in str(resultado), resultado
