from __future__ import annotations

import asyncio
import hashlib
import json
import struct
import tempfile
import warnings
from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx
import pandas as pd
import pytest

from agrobr import acervo_fundiario, constants
from agrobr.acervo_fundiario import client, models, parser
from agrobr.exceptions import (
    InvalidParameterError,
    ParseError,
    ResourceLimitError,
    SourceUnavailableError,
)
from tests import helpers
from tests.test_acervo_fundiario import oficial

pytest.importorskip("pyogrio")
pytest.importorskip("geopandas")

GOLDEN = Path(__file__).parents[1] / "golden_data/acervo_fundiario"
BBOX_DF = (-48.25, -15.90, -48.10, -15.60)
SIGEF_DF = {"publico": "sigef_publico_df_20261001", "privado": "sigef_privado_df_20261001"}
NATUREZAS = ("publico", "privado")

pytestmark = pytest.mark.usefixtures("isolated_cache")


def golden(name: str) -> tuple[bytes, dict[str, Any]]:
    folder = GOLDEN / name
    archive = (folder / "response.zip").read_bytes()
    metadata = json.loads((folder / "metadata.json").read_text(encoding="utf-8"))
    assert hashlib.sha256(archive).hexdigest() == metadata["sha256"]
    return archive, metadata


def publicar_sigef(
    published: dict[str, bytes], naturezas: tuple[str, ...] = NATUREZAS
) -> dict[str, tuple[bytes, dict[str, Any]]]:
    goldens = {natureza: golden(SIGEF_DF[natureza]) for natureza in naturezas}
    for archive, metadata in goldens.values():
        published[metadata["url"]] = archive
    return goldens


def registros_sigef(
    goldens: dict[str, tuple[bytes, dict[str, Any]]], naturezas: tuple[str, ...] = NATUREZAS
) -> list[dict[str, Any]]:
    return [
        oficial.sigef(record, natureza)
        for natureza in naturezas
        for record in oficial.dbf(goldens[natureza][0])
    ]


@pytest.fixture
def servidor(monkeypatch: pytest.MonkeyPatch) -> tuple[dict[str, bytes], list[str]]:
    published: dict[str, bytes] = {}
    requested: list[str] = []
    original = httpx.AsyncClient

    def handler(request: httpx.Request) -> httpx.Response:
        requested.append(f"{request.method} {request.url}")
        body = published.get(str(request.url))
        if body is None:
            return httpx.Response(404)
        headers = {"Last-Modified": "Tue, 22 Sep 2026 16:39:50 GMT", "ETag": '"golden"'}
        if request.method == "HEAD":
            return httpx.Response(200, headers={**headers, "Content-Length": str(len(body))})
        return httpx.Response(200, headers=headers, content=body)

    def factory(**kwargs: Any) -> httpx.AsyncClient:
        return original(transport=httpx.MockTransport(handler), **kwargs)

    monkeypatch.setattr(client.httpx, "AsyncClient", factory)
    return published, requested


async def outcome(call: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    try:
        return await call(*args, **kwargs)
    except Exception as exc:
        return exc


def envelopes(archive: bytes) -> list[tuple[float, float, float, float] | None]:
    raw = oficial.member(archive, ".shp")
    boxes: list[tuple[float, float, float, float] | None] = []
    offset = 100
    while offset < len(raw):
        length, shape_type = (
            struct.unpack_from(">i", raw, offset + 4)[0],
            struct.unpack_from("<i", raw, offset + 8)[0],
        )
        boxes.append(None if shape_type == 0 else struct.unpack_from("<4d", raw, offset + 12))
        offset += 8 + 2 * length
    return boxes


def inside(box: tuple[float, float, float, float] | None, bbox: tuple[float, ...]) -> bool:
    return box is not None and not (
        box[2] < bbox[0] or box[0] > bbox[2] or box[3] < bbox[1] or box[1] > bbox[3]
    )


async def test_snci_publicado_pelo_incra_sai_igual_a_fonte(
    servidor: tuple[dict[str, bytes], list[str]],
):
    archive, metadata = golden("snci_rr_20260922")
    published, requested = servidor
    published[metadata["url"]] = archive
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        result = await outcome(acervo_fundiario.snci, "RR", return_meta=True)
    assert not isinstance(result, Exception), result
    assert not any("vedado o uso comercial" in str(item.message) for item in caught)
    frame, meta = result
    assert meta.license == "livre"
    expected = [oficial.snci(record) for record in oficial.dbf(archive)]
    assert list(frame.columns) == list(expected[0])
    assert oficial.published(frame) == expected
    assert meta.source_url == metadata["url"]
    assert meta.records_count == len(metadata["source_positions"])
    assert meta.selected_source == "acervo_fundiario_snci"
    assert meta.attempted_sources == ["acervo_fundiario_snci"]
    assert requested == [f"GET {metadata['url']}", f"HEAD {metadata['url']}"]


async def test_sigef_le_os_arquivos_publico_e_privado(
    servidor: tuple[dict[str, bytes], list[str]],
):
    published, requested = servidor
    goldens = publicar_sigef(published)
    urls = {natureza: metadata["url"] for natureza, (_, metadata) in goldens.items()}
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        result = await outcome(acervo_fundiario.sigef, "DF", return_meta=True)
    assert not isinstance(result, Exception), result
    assert not any("vedado o uso comercial" in str(item.message) for item in caught)
    frame, meta = result
    assert meta.license == "livre"
    expected = registros_sigef(goldens)
    assert list(frame.columns) == list(expected[0])
    assert oficial.published(frame) == expected
    assert frame["natureza"].tolist() == ["publico"] * 4 + ["privado"] * 4
    assert frame["natureza"].dtype == frame["uf"].dtype == pd.Series([""]).dtype
    assert requested == [
        f"GET {urls['publico']}",
        f"HEAD {urls['publico']}",
        f"GET {urls['privado']}",
        f"HEAD {urls['privado']}",
    ]
    assert meta.source_url == urls["publico"]
    assert meta.schema_version == "1.1"
    assert meta.records_count == 8
    assert meta.selected_source == "acervo_fundiario_sigef"
    assert meta.attempted_sources == [
        "acervo_fundiario_sigef_publico",
        "acervo_fundiario_sigef_privado",
    ]
    assert meta.raw_content_hash is None
    assert meta.raw_content_size == sum(len(archive) for archive, _ in goldens.values())
    assert meta.from_cache is False
    assert meta.validation_warnings == []
    arquivos = meta.source_details["arquivos"]
    assert list(arquivos) == ["publico", "privado"]
    for natureza, (archive, metadata) in goldens.items():
        assert arquivos[natureza]["url"] == metadata["url"]
        assert arquivos[natureza]["sha256"] == metadata["sha256"]
        assert arquivos[natureza]["size_bytes"] == len(archive)
        assert arquivos[natureza]["from_cache"] is False
        assert arquivos[natureza]["last_modified"] == "Tue, 22 Sep 2026 16:39:50 GMT"


@pytest.mark.parametrize("funcao", ["sigef", "sigef_geo"])
async def test_sigef_com_geometria_valida_nao_publica_contagem(
    servidor: tuple[dict[str, bytes], list[str]], funcao: str
):
    published, _ = servidor
    publicar_sigef(published)
    result = await outcome(getattr(acervo_fundiario, funcao), "DF", return_meta=True)
    assert not isinstance(result, Exception), result
    _, meta = result
    assert not {"topology_repaired", "geometrias_invalidas", "geometrias"} & set(
        meta.source_details
    )


@pytest.mark.parametrize(("natureza", "pedida"), [("publico", "publico"), ("Privado", "privado")])
async def test_sigef_com_natureza_baixa_so_o_arquivo_dela(
    servidor: tuple[dict[str, bytes], list[str]], natureza: str, pedida: str
):
    published, requested = servidor
    goldens = publicar_sigef(published)
    archive, metadata = goldens[pedida]
    result = await outcome(acervo_fundiario.sigef, "DF", natureza=natureza, return_meta=True)
    assert not isinstance(result, Exception), result
    frame, meta = result
    assert oficial.published(frame) == registros_sigef(goldens, (pedida,))
    assert requested == [f"GET {metadata['url']}", f"HEAD {metadata['url']}"]
    assert meta.source_url == metadata["url"]
    assert meta.attempted_sources == [f"acervo_fundiario_sigef_{pedida}"]
    assert meta.raw_content_hash == metadata["sha256"]
    assert meta.raw_content_size == len(archive)
    assert list(meta.source_details["arquivos"]) == [pedida]


@pytest.mark.parametrize("function", ["sigef", "sigef_geo"])
@pytest.mark.parametrize("natureza", ["particular", "", 1, ["publico"]])
async def test_natureza_invalida_recusada_antes_da_rede(
    servidor: tuple[dict[str, bytes], list[str]], function: str, natureza: Any
):
    _, requested = servidor
    result = await outcome(getattr(acervo_fundiario, function), "DF", natureza=natureza)
    assert isinstance(result, InvalidParameterError), result
    assert str(result) == (f"natureza inválida: {natureza!r}. Valores válidos: publico, privado"), (
        result
    )
    assert requested == []


async def test_parcela_nos_dois_arquivos_fica_com_aviso(
    servidor: tuple[dict[str, bytes], list[str]],
):
    published, _ = servidor
    goldens = publicar_sigef(published)
    archive, _ = goldens["publico"]
    published[goldens["privado"][1]["url"]] = archive
    result = await outcome(acervo_fundiario.sigef, "DF", return_meta=True)
    assert not isinstance(result, Exception), result
    frame, meta = result
    registros = oficial.dbf(archive)
    assert oficial.published(frame) == [
        oficial.sigef(record, natureza) for natureza in NATUREZAS for record in registros
    ]
    assert meta.validation_warnings == [
        "acervo_fundiario: 4 codigo_parcela aparece(m) nos arquivos público e privado do SIGEF; "
        "as linhas dos dois foram mantidas"
    ]


def fontes_publicadas(
    published: dict[str, bytes], tema: str, partes: tuple[str, ...]
) -> list[tuple[bytes, dict[str, Any], Callable[[dict[str, str]], dict[str, Any]]]]:
    if tema == "snci":
        archive, metadata = golden(partes[0])
        published[metadata["url"]] = archive
        return [(archive, metadata, oficial.snci)]
    goldens = publicar_sigef(published)
    return [
        (*goldens[natureza], lambda record, natureza=natureza: oficial.sigef(record, natureza))
        for natureza in partes
    ]


def recorte_oficial(
    fontes: list[tuple[bytes, dict[str, Any], Callable[[dict[str, str]], dict[str, Any]]]],
    bbox: tuple[float, float, float, float] | None,
) -> tuple[list[dict[str, Any]], list[Any]]:
    expected: list[dict[str, Any]] = []
    rings: list[Any] = []
    for archive, _, build in fontes:
        records = oficial.dbf(archive)
        source = oficial.source_rings(archive)
        chosen = [
            index
            for index, box in enumerate(envelopes(archive))
            if bbox is None or inside(box, bbox)
        ]
        assert len(chosen) <= len(records)
        expected += [build(records[index]) for index in chosen]
        rings += [source[index] for index in chosen]
    return expected, rings


@pytest.mark.parametrize(
    ("tema", "uf", "partes", "bbox", "opcoes"),
    [
        ("sigef", "DF", NATUREZAS, None, {}),
        ("sigef", "DF", NATUREZAS, BBOX_DF, {}),
        ("sigef", "DF", ("privado",), BBOX_DF, {"natureza": "privado"}),
        ("snci", "RR", ("snci_rr_20260922",), None, {}),
    ],
)
async def test_geometria_publicada_igual_ao_shapefile(
    servidor: tuple[dict[str, bytes], list[str]],
    tema: str,
    uf: str,
    partes: tuple[str, ...],
    bbox: tuple[float, float, float, float] | None,
    opcoes: dict[str, Any],
):
    published, _ = servidor
    fontes = fontes_publicadas(published, tema, partes)
    geo = getattr(acervo_fundiario, f"{tema}_geo")
    result = await outcome(geo, uf, bbox=bbox, return_meta=True, **opcoes)
    assert isinstance(result, tuple) and len(result) == 2, type(result)
    frame, meta = result
    tabular = await outcome(getattr(acervo_fundiario, tema), uf, bbox=bbox, **opcoes)
    assert not isinstance(tabular, Exception), tabular
    bare = await outcome(geo, uf, bbox=bbox, **opcoes)
    assert not isinstance(bare, Exception), bare
    expected, rings = recorte_oficial(fontes, bbox)
    archive, metadata, build = fontes[0]
    columns = list(build(oficial.dbf(archive)[0]))
    assert expected
    assert list(frame.columns) == [*columns, "geometry"]
    assert list(tabular.columns) == columns
    assert oficial.published(frame) == expected
    assert oficial.published(tabular) == expected
    assert [oficial.rings(item) for item in frame.geometry] == rings
    assert oficial.published(bare) == oficial.published(frame)
    assert [oficial.rings(item) for item in bare.geometry] == rings
    assert meta.source_url == metadata["url"]
    assert meta.records_count == len(expected)
    for archive, _, _ in fontes:
        assert oficial.member(archive, ".prj").decode("ascii") == oficial.SIRGAS_2000
    assert frame.crs.to_epsg() == 4674


async def test_sigef_com_bbox_nao_depende_do_filtro_espacial_sem_geometria(
    servidor: tuple[dict[str, bytes], list[str]], monkeypatch: pytest.MonkeyPatch
):
    pyogrio = parser.check_pyogrio()
    ler = pyogrio.read_dataframe

    def gdal_sem_filtro_sem_geometria(*args: Any, **kwargs: Any) -> Any:
        frame = ler(*args, **kwargs)
        if kwargs.get("bbox") is not None and kwargs.get("read_geometry") is False:
            return frame.iloc[0:0]
        return frame

    monkeypatch.setattr(pyogrio, "read_dataframe", gdal_sem_filtro_sem_geometria)
    published, _ = servidor
    fontes = fontes_publicadas(published, "sigef", NATUREZAS)
    tabular = await outcome(acervo_fundiario.sigef, "DF", bbox=BBOX_DF)
    assert not isinstance(tabular, Exception), tabular
    expected, _ = recorte_oficial(fontes, BBOX_DF)
    assert [row["natureza"] for row in expected] == ["publico"] * 3 + ["privado"]
    assert list(tabular.columns) == list(expected[0])
    assert oficial.published(tabular) == expected


async def test_sigef_com_recorte_vazio_num_arquivo_mantem_o_esquema(
    servidor: tuple[dict[str, bytes], list[str]],
):
    published, _ = servidor
    fontes = fontes_publicadas(published, "sigef", NATUREZAS)
    bbox = (-48.20, -15.70, -48.10, -15.60)
    expected, _ = recorte_oficial(fontes, bbox)
    assert [row["natureza"] for row in expected] == ["publico"]
    cheio = await outcome(acervo_fundiario.sigef, "DF")
    recorte = await outcome(acervo_fundiario.sigef, "DF", bbox=bbox)
    vazio = await outcome(acervo_fundiario.sigef, "DF", natureza="privado", bbox=bbox)
    for frame in (cheio, recorte, vazio):
        assert isinstance(frame, pd.DataFrame), frame
    assert oficial.published(recorte) == expected
    assert vazio.empty and list(vazio.columns) == list(cheio.columns)
    assert recorte.dtypes.equals(cheio.dtypes)
    assert vazio["natureza"].dtype == cheio["natureza"].dtype


async def test_assentamentos_com_registro_sem_geometria(
    servidor: tuple[dict[str, bytes], list[str]],
):
    archive, metadata = golden("assentamentos_20260922")
    published, _ = servidor
    published[metadata["url"]] = archive
    expected = [oficial.assentamento(record) for record in oficial.dbf(archive)]
    source = oficial.source_rings(archive)
    tabular = await outcome(acervo_fundiario.assentamentos, return_meta=True)
    assert not isinstance(tabular, Exception), tabular
    frame, meta = tabular
    assert list(frame.columns) == list(expected[0])
    assert oficial.published(frame) == expected
    assert meta.source_url == metadata["url"]
    for uf in ("MT", "MG"):
        rows = [index for index, row in enumerate(expected) if row["uf"] == uf]
        subset = await outcome(acervo_fundiario.assentamentos, uf=uf)
        assert not isinstance(subset, Exception), subset
        assert oficial.published(subset) == [expected[index] for index in rows]
        geo = await outcome(acervo_fundiario.assentamentos_geo, uf=uf)
        assert not isinstance(geo, Exception), geo
        assert [oficial.rings(item) for item in geo.geometry] == [source[index] for index in rows]
    result = await outcome(acervo_fundiario.assentamentos_geo, return_meta=True)
    assert isinstance(result, tuple) and len(result) == 2, type(result)
    geo, geo_meta = result
    assert geo_meta.source_url == metadata["url"]
    assert geo_meta.records_count == len(expected)
    assert list(geo.columns) == [*expected[0], "geometry"]
    assert oficial.published(geo) == expected
    assert [oficial.rings(item) for item in geo.geometry] == source
    assert source.count(None) == 1
    assert geo.crs.to_epsg() == 4674


@pytest.mark.parametrize(
    ("body", "message"),
    [
        (None, "HTTP 404"),
        (b"PK", "muito pequena"),
        (b"<!DOCTYPE html>" + b" " * 600, "não é um ZIP válido"),
    ],
)
async def test_resposta_ruim_vira_fonte_indisponivel_sem_cache(
    servidor: tuple[dict[str, bytes], list[str]], body: bytes | None, message: str
):
    published, requested = servidor
    url = "https://certificacao.incra.gov.br/csv_shp/zip/Sigef%20P%C3%BAblico_SE.zip"
    if body is not None:
        published[url] = body
    result = await outcome(acervo_fundiario.sigef, "SE")
    assert isinstance(result, SourceUnavailableError) and message in str(result), result
    assert requested == [f"GET {url}"]
    assert not client._zip_path("sigef_publico", "SE").exists()


@pytest.mark.parametrize("folga", [-1, 0])
async def test_orcamento_do_download(
    servidor: tuple[dict[str, bytes], list[str]], monkeypatch: pytest.MonkeyPatch, folga: int
):
    published, _ = servidor
    goldens = publicar_sigef(published)
    maior = max(len(archive) for archive, _ in goldens.values())
    monkeypatch.setattr(constants, "ACERVO_MAX_DOWNLOAD_BYTES", maior + folga)
    result = await outcome(acervo_fundiario.sigef, "DF")
    if folga < 0:
        assert isinstance(result, ResourceLimitError), result
        assert not client._zip_path("sigef_publico", "DF").exists()
    else:
        assert not isinstance(result, Exception), result
        assert len(result) == len(registros_sigef(goldens))


async def test_codigo_ibge_desconhecido_vira_uf_nula(
    servidor: tuple[dict[str, bytes], list[str]],
):
    archive, metadata = golden(SIGEF_DF["publico"])
    altered = oficial.replace_cell(archive, 2, "uf_id", "99")
    published, _ = servidor
    published[metadata["url"]] = altered
    frame = await outcome(acervo_fundiario.sigef, "DF", natureza="publico")
    assert not isinstance(frame, Exception), frame
    expected = [oficial.sigef(record, "publico") for record in oficial.dbf(altered)]
    assert [row["uf"] for row in expected] == ["DF", "DF", None, "DF"]
    assert oficial.published(frame) == expected


async def test_geometria_invalida_sai_como_publicada_com_aviso(
    servidor: tuple[dict[str, bytes], list[str]], tmp_path: Path
):
    import zipfile

    import geopandas as gpd
    from shapely.geometry import Polygon

    bowtie = Polygon(
        [(-47.9, -15.8), (-47.8, -15.7), (-47.8, -15.8), (-47.9, -15.7), (-47.9, -15.8)]
    )
    columns = {
        **{campo: ["x"] for campo in models.SIGEF_RENAME_MAP},
        **{campo: ["01/01/2024"] for campo in ("data_submi", "data_aprov", "registro_d")},
        "parcela_co": ["p"],
        "status": ["CERTIFICADA"],
        "municipio_": [5300108],
        "uf_id": [53],
    }
    folder = tmp_path / "camada"
    folder.mkdir()
    gpd.GeoDataFrame(columns, geometry=[bowtie], crs="EPSG:4674").to_file(
        folder / "sigef.shp", engine="pyogrio", encoding="latin1"
    )
    archive = tmp_path / "sigef.zip"
    with zipfile.ZipFile(archive, "w") as handle:
        for item in folder.iterdir():
            handle.write(item, item.name)
    published, _ = servidor
    _, metadata = golden(SIGEF_DF["publico"])
    published[metadata["url"]] = archive.read_bytes()
    aviso = (
        "Acervo Fundiário SIGEF: 1 de 1 geometrias inválidas como publicadas pela fonte; "
        "use make_valid antes de operações espaciais."
    )
    with warnings.catch_warnings(record=True) as avisos:
        warnings.simplefilter("always")
        result = await outcome(
            acervo_fundiario.sigef_geo, "DF", natureza="publico", return_meta=True
        )
    assert not isinstance(result, Exception), result
    frame, meta = result
    geometry = frame.geometry.iloc[0]
    assert not geometry.is_valid
    assert sorted(geometry.exterior.coords) == sorted(bowtie.exterior.coords)
    assert [str(a.message) for a in avisos if "geometrias" in str(a.message)] == [aviso]
    assert meta.validation_warnings.count(aviso) == 1
    assert (meta.source_details["geometrias_invalidas"], meta.source_details["geometrias"]) == (
        1,
        1,
    )
    assert "topology_repaired" not in meta.source_details
    assert not [a for a in meta.validation_warnings if "make_valid;" in a]


@pytest.mark.parametrize("function", ["sigef", "sigef_geo", "snci", "snci_geo"])
async def test_uf_ausente_recusada_antes_da_rede(
    servidor: tuple[dict[str, bytes], list[str]], function: str
):
    _, requested = servidor
    result = await outcome(getattr(acervo_fundiario, function), None)
    assert isinstance(result, InvalidParameterError), result
    assert str(result).startswith("UF inválida: None. Valores válidos: AC, AL,"), result
    assert requested == []


@pytest.mark.parametrize("desligado", ["use_cache", "variavel"])
@pytest.mark.parametrize("desfecho", ["sucesso", "erro_no_parse", "cancelamento"])
async def test_cache_desligado_nao_grava_e_apaga_o_temporario(
    servidor: tuple[dict[str, bytes], list[str]],
    isolated_cache: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    desligado: str,
    desfecho: str,
):
    published, requested = servidor
    archive, metadata = publicar_sigef(published, ("publico",))["publico"]
    temporarios = tmp_path / "temporarios"
    temporarios.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", str(temporarios))
    lidos: list[Path] = []
    parse_real = parser.parse_sigef

    def parse_espiando(zip_path: Path, **kwargs: Any) -> Any:
        lidos.append(zip_path)
        assert zip_path.read_bytes() == archive
        if desfecho == "erro_no_parse":
            raise ParseError(source="acervo_fundiario", parser_version=1, reason="teste")
        if desfecho == "cancelamento":
            raise asyncio.CancelledError
        return parse_real(zip_path, **kwargs)

    monkeypatch.setattr(parser, "parse_sigef", parse_espiando)
    opcoes: dict[str, Any] = {"natureza": "publico", "use_cache": False}
    if desligado == "variavel":
        monkeypatch.setenv("AGROBR_ACERVO_FUNDIARIO_CACHE_DISABLED", "true")
        opcoes = {"natureza": "publico"}

    try:
        resultado = await acervo_fundiario.sigef("DF", **opcoes)
    except (ParseError, asyncio.CancelledError) as erro:
        resultado = erro

    esperado = {"sucesso": pd.DataFrame, "erro_no_parse": ParseError}.get(
        desfecho, asyncio.CancelledError
    )
    assert isinstance(resultado, esperado), resultado
    assert requested == [f"GET {metadata['url']}", f"HEAD {metadata['url']}"]
    assert len(lidos) == 1 and lidos[0].is_relative_to(temporarios)
    assert list(temporarios.iterdir()) == []
    assert [p for p in isolated_cache.rglob("*") if p.is_file()] == []


async def test_cache_reusa_revalida_e_se_recupera(
    servidor: tuple[dict[str, bytes], list[str]], monkeypatch: pytest.MonkeyPatch
):
    published, requested = servidor
    goldens = publicar_sigef(published)
    urls = {natureza: metadata["url"] for natureza, (_, metadata) in goldens.items()}
    for natureza, (archive, _) in goldens.items():
        published[urls[natureza].replace("_DF.zip", "_SE.zip")] = archive
    expected = registros_sigef(goldens)
    download = [f"{verbo} {urls[n]}" for n in NATUREZAS for verbo in ("GET", "HEAD")]

    async def rodada(**options: Any) -> list[str]:
        requested.clear()
        result = await outcome(acervo_fundiario.sigef, "DF", return_meta=True, **options)
        assert not isinstance(result, Exception), result
        frame, meta = result
        assert oficial.published(frame) == expected
        assert meta.from_cache is (requested == [f"HEAD {urls[n]}" for n in NATUREZAS])
        return list(requested)

    zip_path = client._zip_path("sigef_publico", "DF")
    meta_path = client._meta_path("sigef_publico", "DF")
    assert await rodada() == download
    assert await rodada() == [f"HEAD {urls[n]}" for n in NATUREZAS]
    requested.clear()
    privado = await outcome(acervo_fundiario.sigef, "DF", natureza="privado", return_meta=True)
    assert not isinstance(privado, Exception), privado
    assert requested == [f"HEAD {urls['privado']}"] and privado[1].from_cache is True
    assert await rodada(use_cache=False) == download
    stored = zip_path.read_bytes()
    zip_path.write_bytes(b"XXXX" + stored[4:])
    assert await rodada() == [*download[:2], f"HEAD {urls['privado']}"]
    meta_path.write_text("{corrompido", encoding="utf-8")
    assert await rodada() == [*download[:2], f"HEAD {urls['privado']}"]
    zip_path.unlink()
    assert await rodada() == [*download[:2], f"HEAD {urls['privado']}"]
    monkeypatch.setenv("AGROBR_ACERVO_FUNDIARIO_CACHE_DISABLED", "1")
    assert await rodada() == download
    monkeypatch.delenv("AGROBR_ACERVO_FUNDIARIO_CACHE_DISABLED")
    frame = await outcome(acervo_fundiario.sigef, "SE")
    assert not isinstance(frame, Exception), frame
    assert not client._zip_path("sigef", "DF").exists()
    for natureza, (archive, metadata) in goldens.items():
        for uf in ("DF", "SE"):
            saved = client._load_meta(client._meta_path(f"sigef_{natureza}", uf))
            assert saved is not None
            assert saved["source_url"] == urls[natureza].replace("_DF.zip", f"_{uf}.zip")
            assert saved["sha256"] == metadata["sha256"]
            assert saved["size_bytes"] == len(archive)
            assert saved["last_modified"] == "Tue, 22 Sep 2026 16:39:50 GMT"
            assert saved["etag"] == '"golden"'


async def test_uf_minuscula_normalizada(servidor: tuple[dict[str, bytes], list[str]]):
    archive, metadata = golden("assentamentos_20260922")
    altered = oficial.replace_cell(archive, 6, "uf", "mt")
    published, _ = servidor
    published[metadata["url"]] = altered
    expected = [oficial.assentamento(record) for record in oficial.dbf(altered)]
    assert expected[6]["uf"] == "mt"
    expected[6]["uf"] = "MT"
    frame = await outcome(acervo_fundiario.assentamentos)
    assert not isinstance(frame, Exception), frame
    assert oficial.published(frame) == expected
    assert frame["uf"].dtype == frame["municipio"].dtype == pd.Series([""]).dtype
    subset = await outcome(acervo_fundiario.assentamentos, uf="MT")
    assert not isinstance(subset, Exception), subset
    assert oficial.published(subset) == [expected[6], expected[8]]


async def test_uf_fora_das_27_siglas_fica_com_aviso(servidor: tuple[dict[str, bytes], list[str]]):
    archive, metadata = golden("assentamentos_20260922")
    altered = oficial.replace_cell(archive, 0, "uf", "MB")
    published, _ = servidor
    published[metadata["url"]] = altered
    expected = [oficial.assentamento(record) for record in oficial.dbf(altered)]
    with helpers.capturar_logs() as logs:
        frame = await outcome(acervo_fundiario.assentamentos)
    assert not isinstance(frame, Exception), frame
    assert oficial.published(frame) == expected
    dirty = [item for item in logs if item["event"] == "acervo_fundiario_dirty_uf_data"]
    assert [(item["n_invalid"], item["invalid_ufs"]) for item in dirty] == [(1, {"MB": 1})]
    subset = await outcome(acervo_fundiario.assentamentos, uf="MS")
    assert not isinstance(subset, Exception), subset
    assert oficial.published(subset) == [expected[1], expected[2]]


async def test_as_polars_publica_as_mesmas_celulas(servidor: tuple[dict[str, bytes], list[str]]):
    pl = pytest.importorskip("polars")
    archive, metadata = golden("snci_rr_20260922")
    published, _ = servidor
    published[metadata["url"]] = archive
    frame = await outcome(acervo_fundiario.snci, "RR", as_polars=True)
    assert isinstance(frame, pl.DataFrame), type(frame)
    expected = [oficial.snci(record) for record in oficial.dbf(archive)]
    assert frame.columns == list(expected[0])
    assert oficial.published(frame.to_pandas()) == expected
