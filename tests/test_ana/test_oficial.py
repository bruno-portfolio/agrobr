from __future__ import annotations

import json
import re
import warnings
from collections.abc import Callable
from typing import Any

import httpx
import pytest

from agrobr import ana
from agrobr.ana.models import LAYERS
from agrobr.exceptions import InvalidParameterError, ParseError, SourceUnavailableError
from agrobr.utils import geo
from tests.helpers import capturar_logs, conferir_corpo
from tests.test_ana import oficial

pytest.importorskip("geopandas")

PAGINAS = (-48.8, -16.8, -46.8, -14.8)
DF = (-48.1, -16.1, -47.9, -15.9)
BRASILIA = (-47.5, -16.0, -47.0, -15.5)
OCEANO = (-30.0, -20.0, -29.9, -19.9)
CASES = [
    ("hidrografia_df", "hidrografia", {"bbox": DF}),
    ("demanda_df", "demanda_irrigacao", {"bbox": DF}),
    ("pivos_piaui", "pivos_irrigacao", {"uf": "PI"}),
    ("disponibilidade_brasilia", "disponibilidade_hidrica", {"bbox": BRASILIA}),
]


def instalar(
    monkeypatch: pytest.MonkeyPatch, handler: Callable[[httpx.Request], httpx.Response]
) -> None:
    original = httpx.AsyncClient

    def factory(**kwargs: Any) -> httpx.AsyncClient:
        return original(transport=httpx.MockTransport(handler), **kwargs)

    monkeypatch.setattr(geo.httpx, "AsyncClient", factory)


def oficial_ou_404(data: dict[str, Any], request: httpx.Request) -> httpx.Response:
    body = oficial.body_for(data, request.url.path, request.url.query)
    if body is None:
        return httpx.Response(404)
    return httpx.Response(200, content=body, headers={"Content-Type": "application/json"})


def trocar_segunda_pagina(monkeypatch: pytest.MonkeyPatch, body: bytes) -> None:
    data = oficial.manifest()

    def handler(request: httpx.Request) -> httpx.Response:
        if "AND OBJECTID >" in request.url.params.get("where", ""):
            return httpx.Response(200, content=body)
        return oficial_ou_404(data, request)

    instalar(monkeypatch, handler)


def segunda_pagina() -> dict[str, Any]:
    page: dict[str, Any] = json.loads(
        (oficial.GOLDEN / "hidrografia_paginas/chave_1.json").read_bytes()
    )
    return page


def pagina_cortada() -> bytes:
    return (oficial.GOLDEN / "hidrografia_paginas/chave_1.json").read_bytes()[:2000]


def pagina_de_proxy() -> bytes:
    return (
        b'<?xml version="1.0" encoding="iso-8859-1"?>\n'
        b'<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" '
        b'"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">\n'
        b"<html><head><title>503 Service Unavailable</title></head>"
        b"<body><h1>Service Unavailable</h1></body></html>\n"
    )


def pagina_sem_chave() -> bytes:
    page = segunda_pagina()
    for feature in page["features"]:
        del feature["attributes"]["OBJECTID"]
    return json.dumps(page).encode()


@pytest.fixture
def servidor(monkeypatch: pytest.MonkeyPatch) -> tuple[dict[str, Any], list[str]]:
    data = oficial.manifest()
    requested: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requested.append(str(request.url))
        return oficial_ou_404(data, request)

    instalar(monkeypatch, handler)
    return data, requested


async def outcome(call: Callable[..., Any], **kwargs: Any) -> Any:
    try:
        return await call(**kwargs)
    except Exception as exc:
        return exc


async def test_paginacao_da_hidrografia_entrega_todas_as_feicoes(
    servidor: tuple[dict[str, Any], list[str]],
):
    data, _ = servidor
    ids = oficial.official_ids("hidrografia_paginas")
    expected = [
        oficial.row("hidrografia", item)
        for item in oficial.features(data, "hidrografia_paginas", "json")
    ]
    assert len(ids) == 1047
    assert [item["OBJECTID"] for item in expected] == ids
    frame = await outcome(ana.hidrografia, bbox=PAGINAS)
    assert not isinstance(frame, Exception), frame
    assert frame["OBJECTID"].tolist() == ids
    assert oficial.published(frame) == expected
    limited = await outcome(ana.hidrografia, bbox=PAGINAS, max_registros=1020)
    assert not isinstance(limited, Exception), limited
    assert limited["OBJECTID"].tolist() == ids[:1020]


@pytest.mark.usefixtures("servidor")
async def test_corte_por_max_registros_avisa_com_cobertura():
    aviso = (
        "ANA hidrografia: retornadas 1020 de 1047 feições por limite local; "
        "restrinja filtros ou use max_registros=None."
    )
    with pytest.warns(UserWarning, match=re.escape(aviso)):
        frame, meta = await ana.hidrografia(bbox=PAGINAS, max_registros=1020, return_meta=True)
    assert len(frame) == meta.records_count == 1020
    assert meta.validation_warnings == [aviso]
    assert meta.source_details["coverage"] == {
        "expected_rows": 1047,
        "returned_rows": 1020,
        "local_limit": 1020,
        "truncated": True,
    }
    assert meta.source_details["query"]["max_registros"] == 1020


@pytest.mark.parametrize("limite", [1047, None])
async def test_sem_corte_nao_avisa_nem_publica_cobertura(
    servidor: tuple[dict[str, Any], list[str]], limite: int | None
):
    _, requested = servidor
    with warnings.catch_warnings(record=True) as avisos:
        warnings.simplefilter("always")
        frame, meta = await ana.hidrografia(bbox=PAGINAS, max_registros=limite, return_meta=True)
    assert len(frame) == 1047
    assert not [aviso for aviso in avisos if "limite local" in str(aviso.message)]
    assert meta.validation_warnings == []
    assert "coverage" not in meta.source_details
    assert sum("returnCountOnly" in url for url in requested) == 1


@pytest.mark.parametrize(
    ("limite", "outra_contagem", "devolvidas", "cortou"),
    [
        (1020, {"count": 1020}, 1020, True),
        (1100, {"count": 2000}, 1047, False),
        (1100, {}, 1047, False),
    ],
)
async def test_corte_avaliado_pela_contagem_que_guiou_a_coleta(
    monkeypatch: pytest.MonkeyPatch,
    limite: int,
    outra_contagem: dict[str, int],
    devolvidas: int,
    cortou: bool,
):
    data = oficial.manifest()
    contagens: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.params.get("returnCountOnly") == "true":
            contagens.append(str(request.url))
            if len(contagens) > 1:
                return httpx.Response(200, json=outra_contagem)
        return oficial_ou_404(data, request)

    instalar(monkeypatch, handler)
    aviso = (
        f"ANA hidrografia: retornadas {devolvidas} de 1047 feições por limite local; "
        "restrinja filtros ou use max_registros=None."
    )
    with warnings.catch_warnings(record=True) as avisos:
        warnings.simplefilter("always")
        frame, meta = await ana.hidrografia(bbox=PAGINAS, max_registros=limite, return_meta=True)
    assert len(frame) == devolvidas
    assert (
        [str(item.message) for item in avisos]
        == meta.validation_warnings
        == ([aviso] if cortou else [])
    )
    assert meta.source_details.get("coverage") == (
        {"expected_rows": 1047, "returned_rows": 1020, "local_limit": 1020, "truncated": True}
        if cortou
        else None
    )
    assert len(contagens) == 1


@pytest.mark.parametrize(("case", "layer", "options"), CASES)
async def test_camadas_tabulares_iguais_a_fonte(
    servidor: tuple[dict[str, Any], list[str]], case: str, layer: str, options: dict[str, Any]
):
    data, requested = servidor
    expected = [oficial.row(layer, item) for item in oficial.features(data, case, "json")]
    assert [item["OBJECTID"] for item in expected] == oficial.official_ids(case)
    result = await outcome(getattr(ana, layer), return_meta=True, **options)
    assert isinstance(result, tuple) and len(result) == 2, result
    frame, meta = result
    assert list(frame.columns) == [column for column, _, _ in oficial.SCHEMA[layer]]
    assert oficial.published(frame) == expected
    assert meta.records_count == len(expected)
    assert meta.selected_source == f"ana_{layer}"
    path = next(entry["path"] for entry in data["requests"] if entry["case"] == case)
    assert meta.source_url.startswith(f"{data['base']}/{path}/query?")
    paginas = [httpx.URL(u) for u in requested if "returnCountOnly" not in httpx.URL(u).params]
    assert len(paginas) == 1
    conferir_corpo(meta, oficial.body_for(data, paginas[0].path, paginas[0].query) or b"")


@pytest.mark.parametrize(("case", "layer", "options"), CASES)
async def test_camadas_geo_iguais_a_fonte(
    servidor: tuple[dict[str, Any], list[str]], case: str, layer: str, options: dict[str, Any]
):
    data, _ = servidor
    source = oficial.features(data, case, "geojson")
    assert [item["properties"]["OBJECTID"] for item in source] == oficial.official_ids(case)
    result = await outcome(getattr(ana, f"{layer}_geo"), return_meta=True, **options)
    assert isinstance(result, tuple) and len(result) == 2, result
    frame, meta = result
    assert list(frame.columns) == [*(column for column, _, _ in oficial.SCHEMA[layer]), "geometry"]
    assert oficial.published(frame) == [oficial.row(layer, item) for item in source]
    assert [item.geom_type for item in frame.geometry] == [
        item["geometry"]["type"] for item in source
    ]
    assert [oficial.coordinates(item) for item in frame.geometry] == [
        oficial.source_coordinates(item) for item in source
    ]
    assert frame.crs.to_epsg() == 4326
    assert meta.records_count == len(source)
    assert meta.selected_source == f"ana_{layer}_geo"
    plain = await outcome(getattr(ana, f"{layer}_geo"), **options)
    assert type(plain) is type(frame), type(plain)
    assert oficial.published(plain) == oficial.published(frame)


def test_pagina_cheia_da_paginacao_nao_loga_truncamento():
    source = oficial.features(oficial.manifest(), "pivos_piaui", "geojson")
    pagina = json.dumps({"type": "FeatureCollection", "features": source}).encode()
    config = {**LAYERS["pivos_irrigacao"], "max_record_count": len(source)}
    with capturar_logs() as logs:
        frame = geo.parse_arcgis_geojson(
            [pagina], source="ana", layer_config=config, parser_version=1
        )
    assert len(frame) == len(source)
    assert [log for log in logs if log["event"] == "ana_truncated"] == []


async def test_pagina_que_para_antes_do_total_vira_fonte_indisponivel(
    monkeypatch: pytest.MonkeyPatch,
):
    empty = segunda_pagina()
    empty["features"] = []
    trocar_segunda_pagina(monkeypatch, json.dumps(empty).encode())
    result = await outcome(ana.hidrografia, bbox=PAGINAS)
    assert isinstance(result, SourceUnavailableError) and "999 de 1047" in str(result), result


async def test_pagina_sem_campo_obrigatorio_vira_erro_de_layout(monkeypatch: pytest.MonkeyPatch):
    later = segunda_pagina()
    for feature in later["features"]:
        del feature["attributes"]["COCURSODAG"]
    trocar_segunda_pagina(monkeypatch, json.dumps(later).encode())
    result = await outcome(ana.hidrografia, bbox=PAGINAS)
    assert isinstance(result, ParseError), result
    assert "pagina 1" in str(result) and "COCURSODAG" in str(result), result


@pytest.mark.parametrize(
    ("body", "fragment"),
    [
        pytest.param(pagina_cortada, "ilegivel", id="json_cortado"),
        pytest.param(pagina_de_proxy, "ilegivel", id="xhtml_de_proxy"),
        pytest.param(pagina_sem_chave, "OBJECTID", id="sem_objectid"),
    ],
)
async def test_pagina_ilegivel_no_meio_da_paginacao_vira_erro_de_layout(
    monkeypatch: pytest.MonkeyPatch, body: Callable[[], bytes], fragment: str
):
    trocar_segunda_pagina(monkeypatch, body())
    result = await outcome(ana.hidrografia, bbox=PAGINAS)
    assert isinstance(result, ParseError), repr(result)
    assert fragment in str(result), result


async def test_servidor_que_repete_a_pagina_e_recusado(monkeypatch: pytest.MonkeyPatch):
    page = json.loads((oficial.GOLDEN / "hidrografia_df/chave_0.json").read_bytes())
    page["features"] = page["features"][:1]
    requests: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request.url.params.get("where", ""))
        if request.url.params.get("returnCountOnly") == "true":
            return httpx.Response(200, content=json.dumps({"count": 3}).encode())
        return httpx.Response(200, content=json.dumps(page).encode())

    instalar(monkeypatch, handler)
    result = await outcome(ana.hidrografia, bbox=DF)
    assert isinstance(result, SourceUnavailableError) and "nao avancou" in str(result), result
    assert len(requests) == 3


async def test_as_polars_publica_as_mesmas_celulas(servidor: tuple[dict[str, Any], list[str]]):
    pl = pytest.importorskip("polars")
    data, _ = servidor
    expected = [
        oficial.row("demanda_irrigacao", item)
        for item in oficial.features(data, "demanda_df", "json")
    ]
    frame = await outcome(ana.demanda_irrigacao, bbox=DF, as_polars=True)
    assert isinstance(frame, pl.DataFrame), type(frame)
    assert frame.columns == [column for column, _, _ in oficial.SCHEMA["demanda_irrigacao"]]
    assert oficial.published(frame.to_pandas()) == expected


async def test_recorte_sem_feicoes_sai_vazio_com_o_esquema(
    servidor: tuple[dict[str, Any], list[str]],
):
    data, requested = servidor
    assert oficial.official_ids("hidrografia_oceano") == []
    columns = [column for column, _, _ in oficial.SCHEMA["hidrografia"]]
    url = f"{data['base']}/Hidrografia/MapServer/0/query"
    result = await outcome(ana.hidrografia, bbox=OCEANO, return_meta=True)
    assert isinstance(result, tuple) and len(result) == 2, result
    frame, meta = result
    assert frame.empty and list(frame.columns) == columns
    assert (meta.records_count, meta.source_url) == (0, url)
    geo_result = await outcome(ana.hidrografia_geo, bbox=OCEANO, return_meta=True)
    assert isinstance(geo_result, tuple) and len(geo_result) == 2, geo_result
    geo_frame, geo_meta = geo_result
    assert geo_frame.empty and list(geo_frame.columns) == [*columns, "geometry"]
    assert (geo_meta.records_count, geo_meta.source_url) == (0, url)
    assert len(requested) == 2
    assert geo_frame.crs is not None and geo_frame.crs.to_epsg() == 4326, geo_frame.crs


@pytest.mark.parametrize(
    ("layer", "options", "error"),
    [
        ("pivos_irrigacao", {"uf": "XX"}, InvalidParameterError),
        ("hidrografia", {"bbox": (-47.9, -16.1, -48.1, -15.9)}, InvalidParameterError),
        ("hidrografia", {"bbox": (-48.1, -15.9, -47.9, -16.1)}, InvalidParameterError),
    ],
)
async def test_parametro_invalido_recusado_antes_da_rede(
    servidor: tuple[dict[str, Any], list[str]],
    layer: str,
    options: dict[str, Any],
    error: type[Exception],
):
    _, requested = servidor
    result = await outcome(getattr(ana, layer), **options)
    assert type(result) is error, result
    assert requested == []
