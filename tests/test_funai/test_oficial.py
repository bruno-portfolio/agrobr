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

from agrobr import constants, deterministic, funai
from agrobr.exceptions import (
    InvalidParameterError,
    ParseError,
    ResourceLimitError,
)
from agrobr.http import retry, wfs_transport
from tests.test_funai import oficial

pytest.importorskip("geopandas")

DENTRO = (-66.852, -2.627, -66.812, -2.587)
CANTO = (-66.9038, -2.6766, -66.8998, -2.6726)
OCEANO = (-30.0, -20.0, -29.9, -19.9)
PREFIXO = {"max_registros": 60, "tamanho_pagina": 25}
GEO_1 = {"max_registros": 1}
Transformar = Callable[[dict[str, Any], bytes], bytes]


def instalar(
    monkeypatch: pytest.MonkeyPatch,
    transformar: Transformar | None = None,
    falhas: dict[str, httpx.Response] | None = None,
) -> tuple[dict[str, Any], list[str]]:
    data = oficial.manifest()
    pedidos: list[str] = []
    pendentes = dict(falhas or {})

    def responder(request: httpx.Request) -> httpx.Response:
        pedidos.append(str(request.url))
        entry = oficial.entrada(data, request.url.query.decode())
        if entry is None:
            return httpx.Response(404, request=request)
        if entry["file"] in pendentes:
            return pendentes.pop(entry["file"])
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


def sem_nome(entry: dict[str, Any], body: bytes) -> bytes:
    if entry["role"] != "page":
        return body
    page = json.loads(body)
    del page["features"][3]["properties"]["terrai_nome"]
    return json.dumps(page, ensure_ascii=False).encode()


def crs_sirgas(entry: dict[str, Any], body: bytes) -> bytes:
    if entry["role"] != "page":
        return body
    return body.replace(b"urn:ogc:def:crs:EPSG::4326", b"urn:ogc:def:crs:EPSG::4674")


def contagem_mudou() -> Transformar:
    vistas: list[str] = []

    def transformar(entry: dict[str, Any], body: bytes) -> bytes:
        if not entry["role"].startswith("count"):
            return body
        vistas.append(entry["file"])
        if vistas.count(entry["file"]) < 2:
            return body
        page = json.loads(body)
        page["numberMatched"] = page["totalFeatures"] = page["numberMatched"] + 1
        return json.dumps(page, ensure_ascii=False).encode()

    return transformar


Alterar = Callable[[dict[str, Any]], None]


def pagina(alterar: Alterar, inicio: str = "0") -> Transformar:
    def transformar(entry: dict[str, Any], body: bytes) -> bytes:
        if entry["role"] != "page" or entry["params"]["startIndex"] != inicio:
            return body
        page = json.loads(body)
        alterar(page)
        return json.dumps(page, ensure_ascii=False).encode()

    return transformar


def na_feicao(indice: int, campo: str, valor: Any) -> Alterar:
    def alterar(page: dict[str, Any]) -> None:
        page["features"][indice]["properties"][campo] = valor

    return alterar


def anel(transformar_anel: Callable[[list[Any]], list[Any]]) -> Alterar:
    def alterar(page: dict[str, Any]) -> None:
        coordenadas = page["features"][0]["geometry"]["coordinates"]
        coordenadas[0][0] = transformar_anel(coordenadas[0][0])

    return alterar


def poligonos(extra: list[Any]) -> Alterar:
    def alterar(page: dict[str, Any]) -> None:
        page["features"][0]["geometry"]["coordinates"].append(extra)

    return alterar


def geometria(valor: Any) -> Alterar:
    def alterar(page: dict[str, Any]) -> None:
        page["features"][0]["geometry"] = valor

    return alterar


def ordem_trocada(page: dict[str, Any]) -> None:
    features = page["features"]
    features[5], features[6] = features[6], features[5]


def id_em_branco(page: dict[str, Any]) -> None:
    page["features"][7]["id"] = "   "


def probe_sem_contagem(entry: dict[str, Any], body: bytes) -> bytes:
    if entry["role"] != "count_before":
        return body
    page = json.loads(body)
    del page["numberMatched"], page["totalFeatures"]
    return json.dumps(page, ensure_ascii=False).encode()


def ultima_pagina_repetida(page: dict[str, Any]) -> None:
    page["features"] = [page["features"][0]] * len(page["features"])


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


def esperado(data: dict[str, Any], caso: str) -> list[dict[str, Any]]:
    return [oficial.linha(feature) for feature in oficial.feicoes(data, caso)]


async def test_terras_publicam_cada_atributo_da_fonte(servidor: tuple[dict[str, Any], list[str]]):
    data, _ = servidor
    linhas = esperado(data, "tis_prefixo")
    assert len(linhas) == 60
    resultado, avisos = await chamar(funai.terras_indigenas, return_meta=True, **PREFIXO)
    assert isinstance(resultado, tuple) and len(resultado) == 2, resultado
    frame, meta = resultado
    assert list(frame.columns) == [coluna for coluna, _ in oficial.colunas()]
    assert oficial.publicado(frame) == linhas
    assert str(frame["data_atualizacao"].dtype) == "datetime64[ns]"
    assert frame["nome"].dtype == pd.Series([""]).dtype
    assert str(frame["area_ha"].dtype) == "float64"
    assert not any("viraram NaT" in aviso for aviso in meta.validation_warnings)
    assert "AM,PA" in set(frame["uf"])
    assert meta.records_count == 60
    assert meta.fetch_timestamp == meta.fetched_at
    assert any("prefixo remoto de 60 de 665" in aviso for aviso in avisos), avisos


@pytest.mark.parametrize("ilegivel", ["31/02/2020", "2020-05-01", "05/2020"])
async def test_data_ilegivel_vira_nat_com_aviso_no_meta(monkeypatch: pytest.MonkeyPatch, ilegivel):
    instalar(monkeypatch, pagina(na_feicao(4, "data_atualizacao", ilegivel)))

    resultado, avisos = await chamar(funai.terras_indigenas, return_meta=True, **PREFIXO)

    assert isinstance(resultado, tuple), resultado
    frame, meta = resultado
    assert pd.isna(frame.loc[4, "data_atualizacao"])
    aviso = "funai: 1 valor(es) de data_atualizacao viraram NaT"
    assert [a for a in meta.validation_warnings if a.startswith(aviso)]
    assert [a for a in avisos if a.startswith(aviso)]


async def test_filtro_de_uf_casa_qualquer_uf_da_terra(servidor: tuple[dict[str, Any], list[str]]):
    data, _ = servidor
    linhas = [linha for linha in esperado(data, "tis_prefixo") if "PA" in oficial.ufs(linha["uf"])]
    assert any("," in linha["uf"] for linha in linhas)
    resultado, avisos = await chamar(funai.terras_indigenas, uf="pa", **PREFIXO)
    assert not isinstance(resultado, Exception), resultado
    assert oficial.publicado(resultado) == linhas
    assert any(f"filtro local retornou {len(linhas)} linhas" in aviso for aviso in avisos), avisos


async def test_camada_inteira_confere_celulas_e_ufs_publicadas(
    servidor: tuple[dict[str, Any], list[str]],
):
    data, _ = servidor
    linhas = esperado(data, "tis_camada")
    assert len(linhas) == 665
    resultado, avisos = await chamar(funai.terras_indigenas, return_meta=True)
    assert isinstance(resultado, tuple) and len(resultado) == 2, resultado
    frame, meta = resultado
    assert oficial.publicado(frame) == linhas
    assert avisos == []
    assert [aviso for aviso in meta.validation_warnings if "Diagnóstico" in aviso] == []
    para = [linha for linha in linhas if "PA" in oficial.ufs(linha["uf"])]
    assert any(", " in linha["uf"] for linha in para)
    filtrado, _ = await chamar(funai.terras_indigenas, uf="PA")
    assert not isinstance(filtrado, Exception), filtrado
    assert oficial.publicado(filtrado) == para


async def test_filtro_de_fase_e_exato(servidor: tuple[dict[str, Any], list[str]]):
    data, _ = servidor
    publicadas = sorted({f["properties"]["fase_ti"] for f in oficial.feicoes(data, "tis_camada")})
    assert len(publicadas) == 6
    prefixo = esperado(data, "tis_prefixo")
    for fase in publicadas:
        resultado, _ = await chamar(funai.terras_indigenas, fase=fase, **PREFIXO)
        assert not isinstance(resultado, Exception), (fase, resultado)
        assert oficial.publicado(resultado) == [linha for linha in prefixo if linha["fase"] == fase]


async def test_geo_publica_a_geometria_da_fonte(servidor: tuple[dict[str, Any], list[str]]):
    data, _ = servidor
    features = oficial.feicoes(data, "tis_geo_prefixo")
    assert len(features) == 1
    resultado, _ = await chamar(funai.terras_indigenas_geo, max_registros=1, return_meta=True)
    assert isinstance(resultado, tuple) and len(resultado) == 2, resultado
    frame, meta = resultado
    assert oficial.publicado(frame) == [oficial.linha(f) for f in features]
    assert [oficial.coordenadas(g) for g in frame.geometry] == [
        f["geometry"]["coordinates"] for f in features
    ]
    assert frame.crs.to_epsg() == 4326
    assert meta.records_count == 1


@pytest.mark.parametrize(
    ("caso", "bbox", "publicadas"),
    [("tis_bbox_dentro", DENTRO, 1), ("tis_bbox_canto", CANTO, 0)],
    ids=["dentro", "canto_do_envelope"],
)
async def test_bbox_usa_a_intersecao_da_geometria(
    servidor: tuple[dict[str, Any], list[str]],
    caso: str,
    bbox: tuple[float, float, float, float],
    publicadas: int,
):
    data, _ = servidor
    candidatas = oficial.feicoes(data, caso)
    dentro = [f for f in candidatas if oficial.intersecta(f["geometry"], bbox)]
    assert len(candidatas) == 1 and len(dentro) == publicadas
    resultado, _ = await chamar(funai.terras_indigenas_geo, bbox=bbox)
    assert not isinstance(resultado, Exception), resultado
    assert oficial.publicado(resultado) == [oficial.linha(f) for f in dentro]
    assert [oficial.coordenadas(g) for g in resultado.geometry] == [
        f["geometry"]["coordinates"] for f in dentro
    ]
    assert resultado.crs.to_epsg() == 4326
    tabular, _ = await chamar(funai.terras_indigenas, bbox=bbox)
    assert oficial.publicado(tabular) == oficial.publicado(resultado)


async def test_recorte_vazio_sai_com_o_esquema(servidor: tuple[dict[str, Any], list[str]]):
    _, pedidos = servidor
    colunas = [coluna for coluna, _ in oficial.colunas()]
    tabular, _ = await chamar(funai.terras_indigenas, bbox=OCEANO)
    assert not isinstance(tabular, Exception), tabular
    assert tabular.empty and list(tabular.columns) == colunas
    geo, _ = await chamar(funai.terras_indigenas_geo, bbox=OCEANO)
    assert not isinstance(geo, Exception), geo
    assert geo.empty and list(geo.columns) == [*colunas, "geometry"]
    assert geo.crs.to_epsg() == 4326
    assert len(pedidos) == 4
    cheio, _ = await chamar(funai.terras_indigenas, **PREFIXO)
    assert not isinstance(cheio, Exception) and not cheio.empty, cheio
    assert tabular.dtypes.to_dict() == cheio.dtypes.to_dict()
    assert geo.drop(columns="geometry").dtypes.to_dict() == cheio.dtypes.to_dict()


@pytest.mark.parametrize(
    ("opcoes", "erro"),
    [
        ({"uf": "XX"}, InvalidParameterError),
        ({"uf": 1}, InvalidParameterError),
        ({"fase": "Regularizadas"}, InvalidParameterError),
        ({"bbox": (-47.0, -16.0, -48.0, -15.0)}, InvalidParameterError),
        ({"tamanho_pagina": 1001}, InvalidParameterError),
        ({"return_meta": "sim"}, InvalidParameterError),
        ({"formato": "csv"}, TypeError),
    ],
    ids=["uf", "uf_numero", "fase", "bbox_invertida", "pagina_grande", "meta_texto", "kwarg"],
)
async def test_parametro_invalido_recusado_antes_da_rede(
    servidor: tuple[dict[str, Any], list[str]], opcoes: dict[str, Any], erro: type[Exception]
):
    _, pedidos = servidor
    resultado, _ = await chamar(funai.terras_indigenas, **opcoes)
    assert type(resultado) is erro, resultado
    assert pedidos == []


async def test_modo_deterministico_recusado_antes_da_rede(
    servidor: tuple[dict[str, Any], list[str]],
):
    _, pedidos = servidor
    async with deterministic("2026-09-07"):
        resultado, _ = await chamar(funai.terras_indigenas, **PREFIXO)
    assert type(resultado) is InvalidParameterError, resultado
    assert pedidos == []


async def test_polars_ausente_recusado_antes_da_rede(
    servidor: tuple[dict[str, Any], list[str]], monkeypatch: pytest.MonkeyPatch
):
    _, pedidos = servidor
    monkeypatch.setitem(sys.modules, "polars", None)
    resultado, _ = await chamar(funai.terras_indigenas, as_polars=True, **PREFIXO)
    assert type(resultado) is ImportError, resultado
    assert pedidos == []


async def test_as_polars_publica_as_mesmas_celulas(servidor: tuple[dict[str, Any], list[str]]):
    pl = pytest.importorskip("polars")
    data, _ = servidor
    resultado, _ = await chamar(funai.terras_indigenas, as_polars=True, **PREFIXO)
    assert isinstance(resultado, pl.DataFrame), type(resultado)
    assert resultado.columns == [coluna for coluna, _ in oficial.colunas()]
    assert oficial.publicado(resultado.to_pandas()) == esperado(data, "tis_prefixo")


@pytest.mark.parametrize(
    ("chamada", "opcoes", "transformar", "trecho"),
    [
        ("terras_indigenas", PREFIXO, lambda: sem_nome, "terrai_nome"),
        ("terras_indigenas_geo", {"bbox": DENTRO}, lambda: crs_sirgas, "CRS"),
        ("terras_indigenas", PREFIXO, contagem_mudou, "mudou"),
        ("terras_indigenas", PREFIXO, lambda: probe_sem_contagem, "Probe"),
        ("terras_indigenas", PREFIXO, lambda: pagina(ordem_trocada), "ordenação"),
        ("terras_indigenas", PREFIXO, lambda: pagina(id_em_branco), "Feature.id"),
        (
            "terras_indigenas_geo",
            GEO_1,
            lambda: pagina(anel(lambda r: [r[0], r[1], r[0]])),
            "Anel incompleto",
        ),
        (
            "terras_indigenas_geo",
            GEO_1,
            lambda: pagina(anel(lambda r: [*r[:-1], r[1]])),
            "Anel aberto",
        ),
        ("terras_indigenas_geo", GEO_1, lambda: pagina(poligonos([])), "sem anéis"),
        (
            "terras_indigenas_geo",
            GEO_1,
            lambda: pagina(
                poligonos([[[0.0, 0.0, 1.0], [1.0, 0.0, 1.0], [1.0, 1.0, 1.0], [0.0, 0.0, 1.0]]])
            ),
            "Dimensões inconsistentes",
        ),
    ],
    ids=[
        "propriedade_ausente",
        "crs_trocado",
        "contagem_mudou",
        "probe_sem_contagem",
        "ordem_trocada",
        "id_em_branco",
        "anel_degenerado",
        "anel_aberto",
        "poligono_vazio",
        "dimensoes_mistas",
    ],
)
async def test_pagina_fora_do_layout_vira_erro_de_layout(
    monkeypatch: pytest.MonkeyPatch,
    chamada: str,
    opcoes: dict[str, Any],
    transformar: Callable[[], Transformar],
    trecho: str,
):
    instalar(monkeypatch, transformar())
    resultado, _ = await chamar(getattr(funai, chamada), **opcoes)
    assert isinstance(resultado, ParseError), repr(resultado)
    assert trecho in str(resultado), resultado


@pytest.mark.parametrize(
    ("chamada", "opcoes", "caso", "alterar", "diagnostico"),
    [
        (
            "terras_indigenas",
            PREFIXO,
            "tis_prefixo",
            na_feicao(10, "uf_sigla", "AM,XX"),
            "unknown_uf_token",
        ),
        (
            "terras_indigenas",
            PREFIXO,
            "tis_prefixo",
            na_feicao(11, "superficie_perimetro_ha", -1.5),
            "negative_area",
        ),
        ("terras_indigenas_geo", GEO_1, "tis_geo_prefixo", geometria(None), "null_geometry"),
        (
            "terras_indigenas_geo",
            GEO_1,
            "tis_geo_prefixo",
            geometria({"type": "MultiPolygon", "coordinates": []}),
            "empty_geometry",
        ),
    ],
    ids=["uf", "area", "geometria_nula", "geometria_vazia"],
)
async def test_valor_fora_do_padrao_sai_preservado_com_diagnostico(
    monkeypatch: pytest.MonkeyPatch,
    chamada: str,
    opcoes: dict[str, Any],
    caso: str,
    alterar: Alterar,
    diagnostico: str,
):
    data, _ = instalar(monkeypatch, pagina(alterar))
    features = oficial.feicoes(data, caso)
    page = {"features": features}
    alterar(page)
    resultado, _ = await chamar(getattr(funai, chamada), return_meta=True, **opcoes)
    assert isinstance(resultado, tuple) and len(resultado) == 2, resultado
    frame, meta = resultado
    assert oficial.publicado(frame) == [oficial.linha(feature) for feature in page["features"]]
    avisos = [aviso for aviso in meta.validation_warnings if diagnostico in aviso]
    assert avisos and avisos[0].endswith(f"Diagnóstico FUNAI {diagnostico}: 1 ocorrência(s)"), (
        meta.validation_warnings
    )


async def test_codigo_nulo_sai_preservado_sem_quebrar_a_ordem(monkeypatch: pytest.MonkeyPatch):
    alterar = na_feicao(10, "terrai_codigo", None)
    data, _ = instalar(monkeypatch, pagina(alterar))
    features = oficial.feicoes(data, "tis_prefixo")
    page = {"features": features}
    alterar(page)
    resultado, _ = await chamar(funai.terras_indigenas, **PREFIXO)
    assert not isinstance(resultado, Exception), resultado
    assert oficial.publicado(resultado) == [oficial.linha(feature) for feature in page["features"]]


async def test_janela_repetida_sai_preservada_com_aviso(monkeypatch: pytest.MonkeyPatch):
    data, _ = instalar(monkeypatch, pagina(ultima_pagina_repetida, inicio="49"))
    features = oficial.feicoes(data, "tis_prefixo")
    terceira = next(
        entry
        for entry in data["requests"]
        if entry["case"] == "tis_prefixo" and entry["params"].get("startIndex") == "49"
    )
    repetida = json.loads((oficial.GOLDEN / terceira["file"]).read_bytes())["features"][0]
    features = [*features[:50], *[repetida] * 10]
    resultado, _ = await chamar(funai.terras_indigenas, return_meta=True, **PREFIXO)
    assert isinstance(resultado, tuple) and len(resultado) == 2, resultado
    frame, meta = resultado
    assert oficial.publicado(frame) == [oficial.linha(feature) for feature in features]
    assert any("indistinguíveis preservadas" in aviso for aviso in meta.validation_warnings), (
        meta.validation_warnings
    )


@pytest.mark.parametrize(
    ("limite", "valor", "trecho", "feitos"),
    [
        ("FUNAI_MAX_PAGES", 2, "páginas excedido", 3),
        ("FUNAI_MAX_RETAINED_BYTES", 1, "retenção estimada excedido", 1),
    ],
    ids=["paginas", "retencao"],
)
async def test_limite_operacional_interrompe_a_coleta(
    servidor: tuple[dict[str, Any], list[str]],
    monkeypatch: pytest.MonkeyPatch,
    limite: str,
    valor: int,
    trecho: str,
    feitos: int,
):
    _, pedidos = servidor
    monkeypatch.setattr(constants, limite, valor)
    resultado, _ = await chamar(funai.terras_indigenas, **PREFIXO)
    assert isinstance(resultado, ResourceLimitError), resultado
    assert trecho in str(resultado)
    assert len(pedidos) == feitos


@pytest.mark.parametrize(
    ("retry_after", "espera"),
    [("7", 7.0), ("86400", 30.0)],
    ids=["valor", "teto"],
)
async def test_retry_after_do_servidor_define_a_espera(
    monkeypatch: pytest.MonkeyPatch, retry_after: str, espera: float
):
    esperas: list[float] = []

    async def esperar(segundos: float) -> None:
        esperas.append(segundos)

    monkeypatch.setenv("AGROBR_HTTP_RETRY_BASE_DELAY", "1")
    monkeypatch.setenv("AGROBR_HTTP_RETRY_MAX_DELAY", "30")
    monkeypatch.setattr(retry, "asyncio", SimpleNamespace(sleep=esperar))
    indisponivel = httpx.Response(503, headers={"Retry-After": retry_after}, content=b"503")
    data, pedidos = instalar(monkeypatch, falhas={"tis_prefixo/pagina_1.json": indisponivel})
    resultado, _ = await chamar(funai.terras_indigenas, **PREFIXO)
    assert not isinstance(resultado, Exception), resultado
    assert oficial.publicado(resultado) == esperado(data, "tis_prefixo")
    assert esperas == [espera]
    assert len(pedidos) == 6


async def test_fase_com_rotulo_novo_na_fonte_avisa_as_fases_lidas(monkeypatch: pytest.MonkeyPatch):
    def renomear(entry: dict[str, Any], body: bytes) -> bytes:
        if entry["role"] != "page":
            return body
        return body.replace(b'"Encaminhada RI"', b'"Encaminhada (RI)"')

    instalar(monkeypatch, renomear)
    resultado, avisos = await chamar(
        funai.terras_indigenas, fase="Encaminhada RI", return_meta=True
    )
    assert isinstance(resultado, tuple) and len(resultado) == 2, resultado
    frame, meta = resultado
    assert frame.empty
    [aviso] = [aviso for aviso in avisos if "não encontrou registros" in aviso]
    assert "'Encaminhada (RI)'" in aviso
    assert "'Encaminhada RI'" in aviso.split("presentes na leitura")[0]
    assert aviso in meta.validation_warnings
