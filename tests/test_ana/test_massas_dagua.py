from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any
from urllib.parse import parse_qsl

import httpx
import pandas as pd
import pytest

from agrobr import ana
from agrobr.ana import client, models
from agrobr.exceptions import InvalidParameterError, ParseError, SourceUnavailableError
from agrobr.utils import geo
from tests.helpers import conferir_corpo
from tests.test_ana import oficial

BBOX_DF = (-47.6, -16.0, -47.55, -15.95)
DIVISA = (-47.39, -15.58, -47.37, -15.56)
BARRAGEM = (-47.466, -15.993, -47.462, -15.988)
OCEANO = (-30.0, -20.0, -29.9, -19.9)
CASES = [
    ("bbox_df", {"bbox": BBOX_DF}),
    ("divisa_df_go", {"bbox": DIVISA}),
    ("barragem_df", {"bbox": BARRAGEM}),
    ("uf_df_3", {"uf": "DF", "max_registros": 3}),
]
COLUNAS = [column for column, _, _ in oficial.MASSAS_SCHEMA]


def instalar(
    monkeypatch: pytest.MonkeyPatch, handler: Callable[[httpx.Request], httpx.Response]
) -> None:
    original = httpx.AsyncClient

    def factory(**kwargs: Any) -> httpx.AsyncClient:
        return original(transport=httpx.MockTransport(handler), **kwargs)

    monkeypatch.setattr(geo.httpx, "AsyncClient", factory)
    monkeypatch.setattr(client.httpx, "AsyncClient", factory)


@pytest.fixture
def servidor(monkeypatch: pytest.MonkeyPatch) -> tuple[dict[str, Any], list[httpx.URL]]:
    data = oficial.manifest(oficial.MASSAS)
    requested: list[httpx.URL] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requested.append(request.url)
        body = oficial.body_for(data, request.url.path, request.url.query, oficial.MASSAS)
        if body is None:
            return httpx.Response(404)
        return httpx.Response(200, content=body, headers={"Content-Type": "application/json"})

    instalar(monkeypatch, handler)
    return data, requested


async def outcome(call: Callable[..., Any], **kwargs: Any) -> Any:
    try:
        return await call(**kwargs)
    except Exception as exc:
        return exc


@pytest.mark.parametrize(("case", "options"), CASES)
async def test_tabela_igual_a_fonte(
    servidor: tuple[dict[str, Any], list[httpx.URL]], case: str, options: dict[str, Any]
):
    data, requested = servidor
    source = oficial.massas_features(data, case, "json")
    expected = [oficial.massa(item) for item in source]
    limite = options.get("max_registros")
    assert [item["attributes"]["FID"] for item in source] == oficial.massas_ids(case)[:limite]
    result = await outcome(ana.massas_dagua, return_meta=True, **options)
    assert isinstance(result, tuple) and len(result) == 2, result
    frame, meta = result
    assert list(frame.columns) == COLUNAS
    assert oficial.published(frame) == expected
    assert meta.records_count == len(expected)
    assert meta.selected_source == meta.attempted_sources[0] == "ana_massas_dagua"
    assert meta.source_url.startswith(f"{data['base']}/{data['layer']}/query?")
    pages = [url for url in requested if "returnCountOnly" not in url.params]
    assert [url.params.get("returnIdsOnly") for url in pages] == ["true", None]
    conferir_corpo(
        meta, oficial.body_for(data, pages[1].path, pages[1].query, oficial.MASSAS) or b""
    )


@pytest.mark.parametrize(("case", "options"), CASES)
async def test_geo_igual_a_fonte(
    servidor: tuple[dict[str, Any], list[httpx.URL]], case: str, options: dict[str, Any]
):
    pytest.importorskip("geopandas")
    data, _ = servidor
    source = oficial.massas_features(data, case, "geojson")
    result = await outcome(ana.massas_dagua_geo, return_meta=True, **options)
    assert isinstance(result, tuple) and len(result) == 2, result
    frame, meta = result
    assert list(frame.columns) == [*COLUNAS, "geometry"]
    assert oficial.published(frame) == [oficial.massa(item) for item in source]
    assert [oficial.coordinates(item) for item in frame.geometry] == [
        oficial.source_coordinates(item) for item in source
    ]
    assert frame.crs.to_epsg() == 4326
    assert meta.selected_source == "ana_massas_dagua_geo"
    plain = await outcome(ana.massas_dagua_geo, **options)
    assert type(plain) is type(frame), type(plain)
    assert oficial.published(plain) == oficial.published(frame)


async def test_feicao_em_duas_ufs_lista_as_siglas(
    servidor: tuple[dict[str, Any], list[httpx.URL]],
):
    frame = await outcome(ana.massas_dagua, bbox=DIVISA)
    assert isinstance(frame, pd.DataFrame), frame
    publicadas = [
        item["attributes"]["nmufe"]
        for item in oficial.massas_features(servidor[0], "divisa_df_go", "json")
    ]
    assert "DISTRITO FEDERAL, GOIÁS" in publicadas
    assert sorted(frame["uf"]) == ["DF/GO", "GO", "GO"]


@pytest.mark.usefixtures("servidor")
async def test_barragem_com_nome_data_volume_e_snisb():
    frame = await outcome(ana.massas_dagua, bbox=BARRAGEM)
    assert isinstance(frame, pd.DataFrame), frame
    linha = oficial.published(frame)[0]
    assert (linha["nome"], linha["data_construcao"], linha["codigo_snisb"]) == (
        "Barragem 048/2016",
        pd.Timestamp(2011, 12, 1),
        480,
    )
    assert linha["volume_hm3"] == 0.12 and linha["uf"] == "DF"


async def test_uf_filtra_pelo_nome_inteiro_na_lista_nmufe(
    servidor: tuple[dict[str, Any], list[httpx.URL]],
):
    _, requested = servidor
    frame = await outcome(ana.massas_dagua, uf="df", max_registros=3)
    assert isinstance(frame, pd.DataFrame), frame
    assert requested[0].params["where"] == (
        "(nmufe = 'DISTRITO FEDERAL' OR nmufe LIKE 'DISTRITO FEDERAL, %' "
        "OR nmufe LIKE '%, DISTRITO FEDERAL' OR nmufe LIKE '%, DISTRITO FEDERAL, %')"
    )
    assert len(oficial.massas_ids("uf_df_3")) == 228 and len(frame) == 3


async def test_recorte_sem_feicoes_sai_vazio_com_os_mesmos_tipos(
    servidor: tuple[dict[str, Any], list[httpx.URL]],
):
    _, requested = servidor
    cheio = await outcome(ana.massas_dagua, bbox=BARRAGEM)
    requested.clear()
    result = await outcome(ana.massas_dagua, bbox=OCEANO, return_meta=True)
    assert isinstance(result, tuple) and len(result) == 2, result
    vazio, meta = result
    assert vazio.empty and list(vazio.columns) == COLUNAS
    assert vazio.dtypes.equals(cheio.dtypes)
    assert meta.records_count == 0 and meta.source_url.endswith("/Massa_dagua/MapServer/0/query")
    assert len(requested) == 1
    assert cheio.dtypes.to_dict() == {
        **dict.fromkeys(COLUNAS, pd.Series([""]).dtype),
        **dict.fromkeys(["codigo", "codigo_snisb", "codigo_trecho"], pd.Int64Dtype()),
        **dict.fromkeys(["volume_hm3", "area_km2", "area_ha", "perimetro_km"], "float64"),
        "data_construcao": "datetime64[ns]",
    }
    pytest.importorskip("geopandas")
    geo_vazio = await outcome(ana.massas_dagua_geo, bbox=OCEANO)
    geo_cheio = await outcome(ana.massas_dagua_geo, bbox=BARRAGEM)
    assert geo_vazio.empty and list(geo_vazio.columns) == [*COLUNAS, "geometry"]
    assert geo_vazio.drop(columns="geometry").dtypes.equals(
        geo_cheio.drop(columns="geometry").dtypes
    )
    assert geo_vazio.crs.to_epsg() == 4326


async def test_as_polars_publica_as_mesmas_celulas(
    servidor: tuple[dict[str, Any], list[httpx.URL]],
):
    pl = pytest.importorskip("polars")
    frame = await outcome(ana.massas_dagua, bbox=BARRAGEM, as_polars=True)
    assert isinstance(frame, pl.DataFrame), type(frame)
    assert frame.columns == COLUNAS
    expected = oficial.massas_features(servidor[0], "barragem_df", "json")
    assert oficial.published(frame.to_pandas()) == [oficial.massa(item) for item in expected]


@pytest.mark.parametrize("nome", ["massas_dagua", "massas_dagua_geo"])
@pytest.mark.parametrize(
    ("options", "fragmento"),
    [
        ({}, "exige uf ou bbox"),
        ({"uf": "XX"}, "UF inválida"),
        ({"bbox": (-47.5, -16.0, -47.6, -15.9)}, "BBOX minlon"),
        ({"bbox": BBOX_DF, "max_registros": 0}, "max_registros"),
    ],
)
async def test_parametro_invalido_recusado_antes_da_rede(
    servidor: tuple[dict[str, Any], list[httpx.URL]],
    nome: str,
    options: dict[str, Any],
    fragmento: str,
):
    if nome.endswith("_geo"):
        pytest.importorskip("geopandas")
    _, requested = servidor
    result = await outcome(getattr(ana, nome), **options)
    assert isinstance(result, InvalidParameterError) and fragmento in str(result), result
    assert requested == []


def pagina_sintetica(fids: list[int], nmufe: str = "DISTRITO FEDERAL") -> bytes:
    campos = dict.fromkeys(models.MASSAS_DAGUA["fields"].split(","))
    features = [
        {"attributes": {**campos, "FID": fid, "esp_cd": fid, "nmufe": nmufe, "dtreserv": " "}}
        for fid in fids
    ]
    return json.dumps({"features": features}).encode()


def servidor_sintetico(
    monkeypatch: pytest.MonkeyPatch,
    ids: list[int],
    *,
    pagina: Callable[[list[int]], bytes] = pagina_sintetica,
    contagem: int | None = None,
) -> list[dict[str, str]]:
    pedidos: list[dict[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        params = dict(parse_qsl(request.url.query.decode("ascii")))
        pedidos.append(params)
        if params.get("returnCountOnly") == "true":
            total = len(ids) if contagem is None else contagem
            return httpx.Response(200, content=json.dumps({"count": total}).encode())
        if params.get("returnIdsOnly") == "true":
            body = {"objectIdFieldName": "FID", "objectIds": list(reversed(ids))}
            return httpx.Response(200, content=json.dumps(body).encode())
        faixa = params["where"].split(" AND FID >= ")[1]
        inicio, fim = (int(parte) for parte in faixa.split(" AND FID <= "))
        return httpx.Response(200, content=pagina([f for f in ids if inicio <= f <= fim]))

    instalar(monkeypatch, handler)
    return pedidos


async def test_paginacao_por_faixa_de_fid(monkeypatch: pytest.MonkeyPatch):
    ids = list(range(10, 3010, 2))
    pedidos = servidor_sintetico(monkeypatch, ids)
    frame = await outcome(ana.massas_dagua, uf="DF")
    assert isinstance(frame, pd.DataFrame), frame
    assert frame["codigo"].tolist() == ids
    faixas = [p["where"].split(") AND ")[1] for p in pedidos if " AND FID >= " in p["where"]]
    assert faixas == [
        "FID >= 10 AND FID <= 2008",
        "FID >= 2010 AND FID <= 3008",
    ]
    assert all("resultRecordCount" not in p and "orderByFields" not in p for p in pedidos)
    limitado = await outcome(ana.massas_dagua, uf="DF", max_registros=1001)
    assert isinstance(limitado, pd.DataFrame), limitado
    assert limitado["codigo"].tolist() == ids[:1001]


async def test_faixa_incompleta_vira_fonte_indisponivel(monkeypatch: pytest.MonkeyPatch):
    servidor_sintetico(
        monkeypatch, list(range(1, 6)), pagina=lambda fids: pagina_sintetica(fids[:-1])
    )
    result = await outcome(ana.massas_dagua, uf="DF")
    assert isinstance(result, SourceUnavailableError), result
    assert "4 de 5 feicoes da faixa FID 1-5" in str(result)


async def test_ids_diferentes_da_contagem_viram_fonte_indisponivel(
    monkeypatch: pytest.MonkeyPatch,
):
    servidor_sintetico(monkeypatch, [1, 2, 3], contagem=4)
    result = await outcome(ana.massas_dagua, uf="DF")
    assert isinstance(result, SourceUnavailableError), result
    assert "3 FID para a contagem 4" in str(result)


@pytest.mark.parametrize(
    ("corpo", "fragmento"),
    [
        (b'{"features": [{"attributes"', "ilegivel"),
        (b'{"features": [{"attributes": {"esp_cd": 1}}]}', "FID"),
    ],
)
async def test_pagina_ilegivel_vira_erro_de_layout(
    monkeypatch: pytest.MonkeyPatch, corpo: bytes, fragmento: str
):
    servidor_sintetico(monkeypatch, [1], pagina=lambda _fids: corpo.ljust(60))
    result = await outcome(ana.massas_dagua, uf="DF")
    assert isinstance(result, ParseError) and fragmento in str(result), result


async def test_uf_fora_do_cadastro_fica_nula_com_aviso(monkeypatch: pytest.MonkeyPatch):
    servidor_sintetico(
        monkeypatch, [1, 2], pagina=lambda fids: pagina_sintetica(fids, "DISTRITO FEDERAL, XANADU")
    )
    result = await outcome(ana.massas_dagua, uf="DF", return_meta=True)
    assert isinstance(result, tuple), result
    frame, meta = result
    assert frame["uf"].isna().all()
    assert meta.validation_warnings == [
        "ana: UF fora do cadastro em nmufe (XANADU); a coluna uf dessas feições ficou nula"
    ]
