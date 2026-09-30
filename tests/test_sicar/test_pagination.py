from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock
from urllib.parse import parse_qs, urlsplit

import pytest

from agrobr import datasets
from agrobr.alt.sicar import api, client, parser
from agrobr.exceptions import ParseError

Page = tuple[int | str | None, list[int]]
PaginationServer = Callable[[int, list[Page]], AsyncMock]
FIXTURE = Path(__file__).parents[1] / "golden_data/sicar/selecao_20260906/df_tabular.json"


@pytest.fixture
def official_features() -> list[dict[str, Any]]:
    return json.loads(FIXTURE.read_bytes())["features"]


@pytest.fixture
def pagination_server(
    monkeypatch: pytest.MonkeyPatch, official_features: list[dict[str, Any]]
) -> PaginationServer:
    def install(hits: int, pages: list[Page]) -> AsyncMock:
        responses = [f'<FeatureCollection numberMatched="{hits}"/>'.encode()]
        responses.extend(
            json.dumps(
                {
                    "type": "FeatureCollection",
                    "numberMatched": matched,
                    "numberReturned": len(indices),
                    "features": [official_features[index] for index in indices],
                }
            ).encode()
            for matched, indices in pages
        )
        fetch = AsyncMock(side_effect=responses)
        monkeypatch.setattr(client, "PAGE_SIZE", 2)
        monkeypatch.setattr(client, "fetch_wfs", fetch)
        return fetch

    return install


async def test_paginacao_crescente_busca_ultima_pagina_e_propaga_aviso(
    pagination_server: PaginationServer, official_features: list[dict[str, Any]]
):
    fetch = pagination_server(4, [(4, [0, 1]), (5, [2, 3]), (5, [4])])

    frame, meta = await api.imoveis("DF", municipio=5300108, return_meta=True)

    assert len(frame) == meta.records_count == 5
    assert official_features[4]["properties"]["cod_imovel"] in set(frame["cod_imovel"])
    assert len(meta.validation_warnings) == 1
    assert "4 para 5" in meta.validation_warnings[0]
    queries = [parse_qs(urlsplit(call.args[0]).query) for call in fetch.await_args_list]
    assert all(query["sortBy"] == ["cod_imovel"] for query in queries)
    assert [query["startIndex"] for query in queries[1:]] == [["0"], ["2"], ["4"]]


async def test_paginacao_decrescente_valida_ultima_contagem(pagination_server: PaginationServer):
    fetch = pagination_server(4, [(4, [0, 1]), (3, [2])])
    notices: list[str] = []
    details: dict[str, Any] = {}

    pages, _url = await client.fetch_imoveis(
        "DF", validation_warnings=notices, source_details=details
    )

    assert len(parser.parse_imoveis_json(pages)) == 3
    assert fetch.await_count == 3
    assert len(notices) == 1
    assert "4 para 3" in notices[0]
    assert details == {"anunciados": 3, "features_unicas": 3}


async def test_paginacao_alta_e_queda_mantem_maior_numero_de_paginas(
    pagination_server: PaginationServer,
):
    fetch = pagination_server(4, [(5, [0, 1]), (3, [2]), (3, [])])
    notices: list[str] = []

    pages, _url = await client.fetch_imoveis("DF", validation_warnings=notices)

    assert len(pages) == 3
    assert len(parser.parse_imoveis_json(pages)) == 3
    assert fetch.await_count == 4
    assert len(notices) == 2
    assert "4 para 5" in notices[0]
    assert "5 para 3" in notices[1]


async def test_paginacao_total_final_inconsistente_orienta_repeticao(
    pagination_server: PaginationServer,
):
    pagination_server(4, [(5, [0, 1]), (5, [2, 3]), (5, [])])

    with pytest.raises(ParseError, match=r"inconsistente: 4.*5.*repita a consulta"):
        await client.fetch_imoveis("DF")


@pytest.mark.parametrize("pages", [[(4, [0, 0])], [(4, [0, 1]), (4, [1, 2])]])
async def test_paginacao_duplicada_falha(pagination_server: PaginationServer, pages: list[Page]):
    fetch = pagination_server(4, pages)

    with pytest.raises(ParseError, match="id de feature repetido"):
        await client.fetch_imoveis("DF")

    assert all(
        parse_qs(urlsplit(call.args[0]).query)["sortBy"] == ["cod_imovel"]
        for call in fetch.await_args_list
    )


@pytest.mark.parametrize("matched", [4, None, "unknown"])
async def test_paginacao_sem_drift_nao_avisa(
    pagination_server: PaginationServer, matched: int | str | None
):
    pagination_server(4, [(matched, [0, 1]), (matched, [2, 3])])
    notices: list[str] = []

    pages, _url = await client.fetch_imoveis("DF", validation_warnings=notices)

    assert len(parser.parse_imoveis_json(pages)) == 4
    assert notices == []


@pytest.mark.parametrize("max_features", [3, None])
async def test_paginacao_geo_envia_ordenacao(
    pagination_server: PaginationServer, max_features: int | None
):
    indices = [2] if max_features == 3 else [2, 3]
    fetch = pagination_server(4, [(4, [0, 1]), (4, indices)])

    pages, _url = await client.fetch_imoveis_geo("DF", max_features=max_features)

    assert len(pages) == 2
    assert all(
        parse_qs(urlsplit(call.args[0]).query).get("sortBy") == ["cod_imovel"]
        for call in fetch.await_args_list
    )


async def test_dataset_preserva_aviso_da_fonte(pagination_server: PaginationServer):
    pagination_server(4, [(4, [0, 1]), (5, [2, 3]), (5, [4])])

    frame, meta = await datasets.cadastro_rural("DF", municipio=5300108, return_meta=True)

    assert len(frame) == meta.records_count == 5
    assert len(meta.validation_warnings) == 1
    assert "4 para 5" in meta.validation_warnings[0]


async def test_resumo_preserva_aviso_da_varredura(pagination_server: PaginationServer):
    pagination_server(4, [(4, [0, 1]), (5, [2, 3]), (5, [4])])

    frame, meta = await api.resumo("DF", municipio=5300108, return_meta=True)

    assert frame["total"].iloc[0] == 5
    assert len(meta.validation_warnings) == 1
    assert "4 para 5" in meta.validation_warnings[0]


async def test_paginacao_drift_para_zero_retorna_vazio_com_aviso(
    pagination_server: PaginationServer,
):
    pagination_server(4, [(0, []), (0, [])])
    notices: list[str] = []

    pages, _url = await client.fetch_imoveis("DF", validation_warnings=notices)

    assert parser.parse_imoveis_json(pages).empty
    assert len(notices) == 1
    assert "4 para 0" in notices[0]


async def test_varias_paginas_carimbam_o_manifesto_no_meta(
    pagination_server: PaginationServer, monkeypatch: pytest.MonkeyPatch
):
    pagination_server(4, [(4, [0, 1]), (4, [2, 3])])
    lidas: list[list[bytes]] = []
    original = parser.parse_imoveis_json

    def espiar(pages: list[bytes], **kwargs: Any) -> Any:
        lidas.append(list(pages))
        return original(pages, **kwargs)

    monkeypatch.setattr(parser, "parse_imoveis_json", espiar)

    _, meta = await api.imoveis("DF", municipio=5300108, return_meta=True)

    detalhes = meta.source_details
    recursos = [
        {"pagina": indice, "sha256": hashlib.sha256(pagina).hexdigest(), "bytes": len(pagina)}
        for indice, pagina in enumerate(lidas[0], 1)
    ]
    manifesto = json.dumps(
        {"query": meta.source_url, "resources": recursos},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    esperado = hashlib.sha256(manifesto).hexdigest()
    assert detalhes.get("resources") == recursos
    assert detalhes.get("query") == meta.source_url
    parametros = parse_qs(urlsplit(meta.source_url).query)
    assert ("count" in parametros, "startIndex" in parametros) == (False, False)
    assert detalhes.get("hash_kind") == "resource_manifest_sha256"
    assert detalhes.get("resource_bytes") == sum(len(pagina) for pagina in lidas[0])
    assert (meta.raw_content_hash, meta.raw_content_size) == (esperado, len(manifesto))
