from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import httpx
import pandas as pd
import pytest

from agrobr import acervo_fundiario, sync
from agrobr.acervo_fundiario import api, client, parser
from agrobr.acervo_fundiario.models import (
    SNCI_COLUNAS_SAIDA,
    SNCI_COLUNAS_SAIDA_GEO,
    SNCI_COLUNAS_SAIDA_NATUREZA,
    SNCI_COLUNAS_SAIDA_NATUREZA_GEO,
)
from agrobr.exceptions import InvalidParameterError
from tests.test_acervo_fundiario import oficial

pytest.importorskip("pyogrio")

GOLDEN = Path(__file__).parents[1] / "golden_data" / "acervo_fundiario"
FORA_DE_AL = (-60.0, -5.0, -59.0, -4.0)

pytestmark = pytest.mark.usefixtures("isolated_cache")


def golden(nome: str) -> tuple[bytes, dict[str, Any], dict[str, Any] | None]:
    pasta = GOLDEN / nome
    esperado = pasta / "expected.json"
    return (
        (pasta / "response.zip").read_bytes(),
        json.loads((pasta / "metadata.json").read_text("utf-8")),
        json.loads(esperado.read_text("utf-8")) if esperado.exists() else None,
    )


@pytest.fixture
def incra(monkeypatch: pytest.MonkeyPatch) -> tuple[dict[str, bytes], list[str]]:
    publicados: dict[str, bytes] = {}
    pedidos: list[str] = []
    original = httpx.AsyncClient

    def responder(request: httpx.Request) -> httpx.Response:
        pedidos.append(f"{request.method} {request.url}")
        corpo = publicados.get(str(request.url))
        if corpo is None:
            return httpx.Response(404)
        cabecalhos = {"Last-Modified": "Fri, 02 Oct 2026 07:07:47 GMT", "ETag": '"golden"'}
        if request.method == "HEAD":
            return httpx.Response(200, headers={**cabecalhos, "Content-Length": str(len(corpo))})
        return httpx.Response(200, headers=cabecalhos, content=corpo)

    def fabrica(**kwargs: Any) -> httpx.AsyncClient:
        return original(transport=httpx.MockTransport(responder), **kwargs)

    monkeypatch.setattr(client.httpx, "AsyncClient", fabrica)
    return publicados, pedidos


async def test_snci_sem_natureza_segue_no_arquivo_brasil_com_cache_e_schema_1_0(
    incra, isolated_cache
):
    pytest.importorskip("geopandas")
    publicados, pedidos = incra
    arquivo, metadata, _ = golden("snci_rr_20260922")
    publicados[metadata["url"]] = arquivo

    frame, meta = await acervo_fundiario.snci("RR", return_meta=True)

    assert pedidos == [f"GET {metadata['url']}", f"HEAD {metadata['url']}"]
    assert (isolated_cache / "acervo_fundiario" / "snci" / "RR.zip").read_bytes() == arquivo
    assert list(frame.columns) == SNCI_COLUNAS_SAIDA
    pd.testing.assert_frame_equal(
        frame, parser.parse_snci(isolated_cache / "acervo_fundiario" / "snci" / "RR.zip")
    )
    assert (meta.schema_version, meta.selected_source) == ("1.0", "acervo_fundiario_snci")


@pytest.mark.parametrize("natureza", ["publico", "privado"])
async def test_snci_com_natureza_le_so_o_arquivo_dela_com_cache_proprio_e_schema_1_1(
    incra, isolated_cache, natureza
):
    pytest.importorskip("geopandas")
    publicados, pedidos = incra
    arquivo, metadata, esperado = golden(f"bruto_snci_{natureza}_al_20261003")
    publicados[metadata["url"]] = arquivo
    assert esperado is not None

    frame, meta = await acervo_fundiario.snci("AL", natureza=natureza, return_meta=True)

    assert pedidos == [f"GET {metadata['url']}", f"HEAD {metadata['url']}"]
    cache = isolated_cache / "acervo_fundiario"
    assert (cache / f"snci_{natureza}" / "AL.zip").read_bytes() == arquivo
    assert not (cache / "snci").exists()
    assert list(frame.columns) == SNCI_COLUNAS_SAIDA_NATUREZA
    assert frame["natureza"].tolist() == [natureza] * len(esperado["registros"])
    assert frame["natureza"].dtype == frame["uf"].dtype == pd.Series([""]).dtype
    publicado = oficial.published(frame)
    assert [(r["num_processo"], r["cod_imovel_rural"]) for r in publicado] == [
        (r["num_proces"], r["cod_imovel"]) for r in esperado["registros"]
    ]
    assert [(r["area_peca_tecnica"], r["data_certificacao"], r["uf"]) for r in publicado] == [
        (oficial.decimal(r["qtd_area_p"]), oficial.dbf_date(r["data_certi"]), r["uf_municip"])
        for r in esperado["registros"]
    ]
    assert meta.schema_version == "1.1"
    assert meta.selected_source == meta.attempted_sources[0] == f"acervo_fundiario_snci_{natureza}"
    assert (meta.raw_content_hash, meta.source_url) == (metadata["sha256"], metadata["url"])


async def test_snci_aceita_natureza_com_acento_e_caixa(incra):
    pytest.importorskip("geopandas")
    publicados, pedidos = incra
    arquivo, metadata, _ = golden("bruto_snci_publico_al_20261003")
    publicados[metadata["url"]] = arquivo

    frame = await acervo_fundiario.snci("AL", natureza="Público")

    assert set(frame["natureza"]) == {"publico"}
    assert pedidos[0] == f"GET {metadata['url']}"


async def test_snci_vazio_conserva_as_colunas_de_cada_modalidade(incra):
    pytest.importorskip("geopandas")
    publicados, _ = incra
    for nome in ("bruto_snci_publico_al_20261003", "snci_rr_20260922"):
        arquivo, metadata, _ = golden(nome)
        publicados[metadata["url"]] = arquivo

    sem = await acervo_fundiario.snci("RR", bbox=FORA_DE_AL)
    com = await acervo_fundiario.snci("AL", natureza="publico", bbox=FORA_DE_AL)

    assert (len(sem), list(sem.columns)) == (0, SNCI_COLUNAS_SAIDA)
    assert (len(com), list(com.columns)) == (0, SNCI_COLUNAS_SAIDA_NATUREZA)
    assert com["natureza"].dtype == pd.Series([""]).dtype


async def test_snci_geo_com_e_sem_natureza(incra):
    pytest.importorskip("geopandas")
    publicados, _ = incra
    for nome in ("bruto_snci_privado_al_20261003", "snci_rr_20260922"):
        arquivo, metadata, _ = golden(nome)
        publicados[metadata["url"]] = arquivo

    com, meta = await acervo_fundiario.snci_geo("AL", natureza="privado", return_meta=True)
    sem = await acervo_fundiario.snci_geo("RR")

    assert list(com.columns) == SNCI_COLUNAS_SAIDA_NATUREZA_GEO
    assert set(com["natureza"]) == {"privado"} and com.crs.to_epsg() == 4674
    assert meta.schema_version == "1.1"
    assert list(sem.columns) == SNCI_COLUNAS_SAIDA_GEO and sem.crs.to_epsg() == 4674


@pytest.mark.parametrize("funcao", [acervo_fundiario.snci, acervo_fundiario.snci_geo])
@pytest.mark.parametrize("natureza", ["ambos", "", 1, "brasil"])
async def test_natureza_invalida_recusada_antes_da_rede_e_do_extra_geo(
    incra, monkeypatch, funcao, natureza
):
    _, pedidos = incra

    def sem_leitor(**_kwargs: Any) -> None:
        raise AssertionError("o extra geo não pode ser conferido antes da natureza")

    monkeypatch.setattr(api, "_check_readers", sem_leitor)

    with pytest.raises(InvalidParameterError, match="natureza inválida"):
        await funcao("AL", natureza=natureza)

    assert pedidos == []


def test_sync_snci_com_natureza(incra):
    pytest.importorskip("geopandas")
    publicados, _ = incra
    arquivo, metadata, _ = golden("bruto_snci_privado_al_20261003")
    publicados[metadata["url"]] = arquivo

    frame = sync.acervo_fundiario.snci("AL", natureza="privado")

    assert list(frame.columns) == SNCI_COLUNAS_SAIDA_NATUREZA
    assert len(frame) == 26
