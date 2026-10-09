from __future__ import annotations

import copy
import gzip
import hashlib
import json
import math
import threading
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, Mock
from xml.etree import ElementTree

import pandas as pd
import pytest

from agrobr import contracts
from agrobr.alt import sicar
from agrobr.alt.sicar import api, client, parser
from agrobr.alt.sicar.models import COLUNAS_IMOVEIS_GEO
from agrobr.exceptions import ParseError
from agrobr.utils import geo as geo_utils
from tests import helpers
from tests.helpers import collect_failures

GOLDEN = Path(__file__).parents[1] / "golden_data"
GEO = GOLDEN / "sicar/geo_20260922"
GEO_MANIFEST = json.loads((GEO / "manifest.json").read_text(encoding="utf-8"))
R11 = GOLDEN / "reconciliacao_registros_precos_zoneamento_seguro_20260918/sicar"
URL = "https://geoserver.car.gov.br/geoserver/sicar/wfs"
INSTANTES = [
    ("2026-09-01T16:44:58.004Z", "2026-09-01T16:44:58.004Z"),
    ("2026-09-01T13:44:58.004-03:00", "2026-09-01T16:44:58.004Z"),
    ("2026-09-01T16:44:58.004", "2026-09-01T16:44:58.004Z"),
    ("2026-09-03T14:27:12.212000+00:00", "2026-09-03T14:27:12.212Z"),
    ("2026-09-03T14:27:12.212000000Z", "2026-09-03T14:27:12.212Z"),
    ("2026-09-03T14:27:12.212000000000Z", "2026-09-03T14:27:12.212Z"),
    ("2026-09-03T14:27:12.2Z", "2026-09-03T14:27:12.200Z"),
    ("2026-09-03T14:27:12.21Z", "2026-09-03T14:27:12.210Z"),
    ("2026-09-03T14:27:12.000000000Z", "2026-09-03T14:27:12Z"),
    ("2026-09-03T11:27:12.212000-03:00", "2026-09-03T14:27:12.212Z"),
    ("2026-09-03", "2026-09-03T00:00:00Z"),
]


def geo_capture(name: str) -> bytes:
    resource = next(item for item in GEO_MANIFEST["resources"] if item["file"] == name)
    content = (GEO / name).read_bytes()
    assert hashlib.sha256(content).hexdigest() == resource["sha256"]
    return content


def geo_features(name: str) -> list[dict[str, Any]]:
    return json.loads(geo_capture(name))["features"]


def geo_linha(feature: dict[str, Any]) -> tuple[Any, ...]:
    values = feature["properties"]
    updated = values.get("data_atualizacao")
    return (
        values["cod_imovel"],
        values["status_imovel"],
        datetime.fromisoformat(values["dat_criacao"]),
        None if updated is None else datetime.fromisoformat(updated),
        values["area"],
        values["condicao"],
        values["uf"],
        values["municipio"],
        values["cod_municipio_ibge"],
        values["m_fiscal"],
        values["tipo_imovel"],
        values["cod_municipio_ibge"],
        feature["geometry"],
    )


def instante(texto: str | None) -> str | None:
    if texto is None:
        return None
    return datetime.fromisoformat(texto).isoformat().replace("+00:00", "Z")


def descarte(descartada: dict[str, Any], mantida: dict[str, Any], criterio: str) -> dict[str, Any]:
    valores = descartada["properties"]
    return {
        "cod_imovel": valores["cod_imovel"],
        "feature_id": descartada["id"],
        "feature_id_mantida": mantida["id"],
        "criterio": criterio,
        "data_atualizacao": instante(valores.get("data_atualizacao")),
        "data_criacao": instante(valores["dat_criacao"]),
    }


def geo_rows(frame: Any) -> list[tuple[Any, ...]]:
    assert type(frame).__name__ == "GeoDataFrame"
    assert list(frame.columns) == COLUNAS_IMOVEIS_GEO
    assert frame.crs.to_epsg() == 4326
    return [
        (
            *(None if pd.isna(value) else value for value in row[:-1]),
            json.loads(json.dumps(row[-1].__geo_interface__)),
        )
        for row in frame.itertuples(index=False, name=None)
    ]


def resumo_esperado(features: list[dict[str, Any]]) -> dict[str, Any]:
    values = [feature["properties"] for feature in features]
    status = Counter(item["status_imovel"] for item in values)
    tipos = Counter(item["tipo_imovel"] for item in values)
    areas = [item["area"] for item in values]
    fiscais = [item["m_fiscal"] for item in values]
    return {
        "total": len(values),
        "ativos": status["AT"],
        "pendentes": status["PE"],
        "suspensos": status["SU"],
        "cancelados": status["CA"],
        "area_total_ha": math.fsum(areas),
        "area_media_ha": math.fsum(areas) / len(areas),
        "modulos_fiscais_medio": math.fsum(fiscais) / len(fiscais),
        "por_tipo_IRU": tipos["IRU"],
        "por_tipo_AST": tipos["AST"],
        "por_tipo_PCT": tipos["PCT"],
    }


def test_cql_exato_por_filtro():
    casos: list[tuple[dict[str, Any], str | None]] = [
        ({}, None),
        (
            {
                "cod_municipio": 5107925,
                "status": "at",
                "tipo": "iru",
                "area_min": 0,
                "area_max": 0,
                "criado_apos": "2024-02-29",
                "atualizado_apos": "2026-01-01T23:59:59.123000",
            },
            "cod_municipio_ibge=5107925 AND status_imovel='AT' AND tipo_imovel='IRU' "
            "AND area>=0 AND area<=0 AND dat_criacao>='2024-02-29' "
            "AND data_atualizacao>'2026-01-01T23:59:59.123Z'",
        ),
        *(
            ({"atualizado_apos": corte}, f"data_atualizacao>'{esperado}'")
            for corte, esperado in INSTANTES
        ),
    ]
    with collect_failures() as check:
        for filtros, esperado in casos:
            with check(filtros):
                assert api._build_cql_filter(**filtros) == esperado


async def test_imoveis_traz_o_hash_e_o_tamanho_do_corpo_wfs(monkeypatch: pytest.MonkeyPatch):
    corpo = gzip.decompress((R11 / "sicar_df_001.json.gz").read_bytes())
    monkeypatch.setattr(client, "fetch_imoveis", AsyncMock(return_value=([corpo], URL)))

    with helpers.sem_excecao():
        _, meta = await api.imoveis("df", municipio=5300108, return_meta=True)

    helpers.conferir_corpo(meta, corpo)
    assert meta.source_url == URL


async def test_imoveis_ordena_a_saida_por_cod_imovel(monkeypatch: pytest.MonkeyPatch):
    documento = json.loads(gzip.decompress((R11 / "sicar_df_001.json.gz").read_bytes()))
    codigos = sorted(feature["properties"]["cod_imovel"] for feature in documento["features"])
    documento["features"].reverse()
    fetch = AsyncMock(return_value=([json.dumps(documento).encode()], URL))
    monkeypatch.setattr(client, "fetch_imoveis", fetch)

    frame = await api.imoveis("df", municipio=5300108)

    assert frame["cod_imovel"].tolist() == codigos
    assert fetch.await_args.args == ("DF", "cod_municipio_ibge=5300108")


@pytest.mark.parametrize("municipio", ["Brasília", " BRASILIA ", 5300108, "5300108"])
async def test_municipio_por_nome_ou_codigo_filtra_pelo_codigo(
    monkeypatch: pytest.MonkeyPatch, municipio: int | str
):
    corpo = gzip.decompress((R11 / "sicar_df_001.json.gz").read_bytes())
    fetch = AsyncMock(return_value=([corpo], URL))
    monkeypatch.setattr(client, "fetch_imoveis", fetch)

    await api.imoveis("df", municipio=municipio)

    assert fetch.await_args.args == ("DF", "cod_municipio_ibge=5300108")


async def test_geo_publica_propriedades_e_geometria_do_corpo(monkeypatch: pytest.MonkeyPatch):
    body = geo_capture("df_geo_srs4326_count3.json")
    features = json.loads(body)["features"]
    fetch = AsyncMock(return_value=([body], URL))
    monkeypatch.setattr(client, "fetch_imoveis_geo", fetch)
    monkeypatch.setattr(client, "fetch_hits", AsyncMock(return_value=len(features)))

    frame, meta = await api.imoveis_geo("df", max_registros=None, return_meta=True)

    assert geo_rows(frame) == [geo_linha(feature) for feature in features]
    assert fetch.await_args.args == ("DF", None)
    assert fetch.await_args.kwargs == {"max_features": None, "validation_warnings": []}
    assert (meta.selected_source, meta.attempted_sources) == ("sicar_wfs_geo", ["sicar_wfs_geo"])
    assert meta.schema_version == contracts.get_contract("sicar_imoveis").version == "2.1"
    assert meta.records_count == len(features)
    helpers.conferir_corpo(meta, body)
    invertido = json.loads(body)
    invertido["features"].reverse()
    fetch.return_value = ([json.dumps(invertido).encode()], URL)
    ordenado = await api.imoveis_geo("df", max_registros=None)
    assert geo_rows(ordenado) == [geo_linha(feature) for feature in features]


@pytest.mark.parametrize(
    ("arquivo", "uf", "cod_municipio", "descartes"),
    [
        (
            "ms_5007901_geo.json",
            "MS",
            5007901,
            [
                ("sicar_imoveis_ms.16368904", "sicar_imoveis_ms.16369101", "data_atualizacao"),
                ("sicar_imoveis_ms.16369728", "sicar_imoveis_ms.16369495", "data_atualizacao"),
            ],
        ),
        (
            "mt_5103353_geo.json",
            "MT",
            5103353,
            [("sicar_imoveis_mt.16367470", "sicar_imoveis_mt.16367477", "data_criacao")],
        ),
        (
            "rs_4320552_geo.json",
            "RS",
            4320552,
            [("sicar_imoveis_rs.16368583", "sicar_imoveis_rs.16368593", "data_criacao")],
        ),
    ],
)
async def test_geo_real_seleciona_a_versao_mais_recente_como_o_tabular(
    monkeypatch: pytest.MonkeyPatch,
    arquivo: str,
    uf: str,
    cod_municipio: int,
    descartes: list[tuple[str, str, str]],
):
    body = geo_capture(arquivo)
    features = {feature["id"]: feature for feature in json.loads(body)["features"]}
    fetch = AsyncMock(return_value=([body], URL))
    monkeypatch.setattr(client, "fetch_imoveis_geo", fetch)

    frame, meta = await api.imoveis_geo(
        uf, municipio=cod_municipio, criado_apos="2026-09-22", return_meta=True
    )

    descartadas = {descartada for descartada, _mantida, _criterio in descartes}
    mantidas = sorted(
        (
            feature
            for identificador, feature in features.items()
            if identificador not in descartadas
        ),
        key=lambda feature: feature["properties"]["cod_imovel"],
    )
    assert geo_rows(frame) == [geo_linha(feature) for feature in mantidas]
    assert fetch.await_args.args == (
        uf,
        f"cod_municipio_ibge={cod_municipio} AND dat_criacao>='2026-09-22'",
    )
    assert meta.source_details.get("sicar") == {
        "features_unicas": len(features),
        "codigos_colapsados": len(descartes),
        "versoes_descartadas_total": len(descartes),
        "versoes_descartadas": [
            descarte(features[descartada], features[mantida], criterio)
            for descartada, mantida, criterio in descartes
        ],
        "versoes_descartadas_truncadas": False,
        "criterios": {"data_atualizacao": 0, "data_criacao": 0, "feature_id": 0}
        | Counter(criterio for *_ids, criterio in descartes),
    }
    assert len(meta.validation_warnings) == 1
    assert meta.validation_warnings[0].startswith(f"{len(descartes)} codigos de imovel")


async def test_stream_real_seleciona_a_versao_mesmo_dividida_entre_paginas(
    monkeypatch: pytest.MonkeyPatch,
):
    paginas = [
        geo_capture("df_vazio_geo.json"),
        geo_capture("ms_5007901_stream_p0.json"),
        geo_capture("ms_5007901_stream_p1.json"),
    ]
    features = {
        feature["id"]: feature for pagina in paginas for feature in json.loads(pagina)["features"]
    }
    captured = []

    async def stream(uf: str, cql_filter: str | None = None, *, max_features: int | None = 5000):
        captured.append((uf, cql_filter, max_features))
        for pagina in paginas:
            yield [pagina], URL

    monkeypatch.setattr(client, "stream_imoveis_geo", stream)
    lotes = [
        lote
        async for lote in api.imoveis_geo_stream("MS", municipio=5007901, criado_apos="2026-09-22")
    ]

    mantidas = [
        features[identificador]
        for identificador in (
            "sicar_imoveis_ms.16369101",
            "sicar_imoveis_ms.16367807",
            "sicar_imoveis_ms.16368897",
            "sicar_imoveis_ms.16369495",
        )
    ]
    assert [len(lote) for lote in lotes] == [3, 1]
    assert geo_rows(pd.concat(lotes)) == [geo_linha(feature) for feature in mantidas]
    assert captured == [("MS", "cod_municipio_ibge=5007901 AND dat_criacao>='2026-09-22'", None)]


async def test_stream_sem_feicoes_nao_entrega_lote(monkeypatch: pytest.MonkeyPatch):
    vazia = geo_capture("df_vazio_geo.json")

    async def stream(*_args: Any, **_kwargs: Any):
        for _pagina in range(2):
            yield [vazia], URL

    monkeypatch.setattr(client, "stream_imoveis_geo", stream)

    lotes = [lote async for lote in api.imoveis_geo_stream("DF", criado_apos="2099-01-01")]

    assert lotes == []


async def test_geo_recusa_feature_repetida_como_o_tabular(monkeypatch: pytest.MonkeyPatch):
    pagina = geo_capture("ms_5007901_stream_p1.json")

    async def stream(*_args: Any, **_kwargs: Any):
        for _repeticao in range(2):
            yield [pagina], URL

    monkeypatch.setattr(client, "stream_imoveis_geo", stream)
    with collect_failures() as check:
        with (
            check("lote"),
            pytest.raises(ParseError, match=r"id de feature repetido sicar_imoveis_ms\.16369495"),
        ):
            parser.parse_imoveis_geojson([pagina, pagina])
        with check("stream"), pytest.raises(ParseError, match="id de feature repetido"):
            _lotes = [lote async for lote in api.imoveis_geo_stream("MS", municipio=5007901)]


async def test_geo_rotula_o_srid_declarado_pela_fonte(monkeypatch: pytest.MonkeyPatch):
    caso = next(item for item in GEO_MANIFEST["cases"] if item["id"] == "df_srid")
    arquivos = {
        (
            pedido["match"]["path"],
            tuple(sorted(pedido["match"]["params"].items())),
            pedido["match"]["skip"],
        ): pedido["file"]
        for pedido in caso["requests"]
    }
    seen = helpers.install_replay_http(monkeypatch, caso, GEO)
    consulta = {
        ("max_registros" if chave == "max_features" else chave): valor
        for chave, valor in caso["query"].items()
    }
    try:
        resultado = await api.imoveis_geo(**consulta, return_meta=True)
    except ParseError as erro:
        resultado = erro
    finally:
        helpers.assert_replay_served(seen)

    servidos = [arquivos[helpers.replay_signature(url)] for url in seen["served"]]
    corpo = json.loads(geo_capture(servidos[-1]))
    declarado = int(corpo["crs"]["properties"]["name"].rsplit(":", 1)[1])
    assert not isinstance(resultado, ParseError), resultado
    frame, _meta = resultado
    assert frame.crs.to_epsg() == declarado
    assert geo_rows(frame) == [geo_linha(feature) for feature in corpo["features"]]


def test_geo_recusa_pagina_com_crs_diferente_do_pedido():
    sem_crs = json.loads(geo_capture("df_geo_srs4326_count3.json")) | {"crs": None}
    casos = [
        ("4674", geo_capture("df_geo_agrobr_count3.json"), "urn:ogc:def:crs:EPSG::4674"),
        ("sem crs", json.dumps(sem_crs).encode(), "None"),
    ]
    with collect_failures() as check:
        for nome, pagina, declarado in casos:
            with (
                check(nome),
                pytest.raises(ParseError, match=f"CRS declarado {declarado} diverge do EPSG:4326"),
            ):
                parser.parse_imoveis_geojson([pagina])
        with check("vazia sem crs"):
            assert parser.parse_imoveis_geojson([geo_capture("df_vazio_geo.json")]).empty


async def test_geo_recusa_data_sem_fuso_como_o_tabular(monkeypatch: pytest.MonkeyPatch):
    documento = json.loads(geo_capture("df_geo_srs4326_count3.json"))
    for feature in documento["features"]:
        for campo in ("dat_criacao", "data_atualizacao"):
            feature["properties"][campo] = feature["properties"][campo].removesuffix("Z")
    monkeypatch.setattr(
        client,
        "fetch_imoveis_geo",
        AsyncMock(return_value=([json.dumps(documento).encode()], URL)),
    )

    with pytest.raises(ParseError, match="Data JSON sem fuso em data_criacao"):
        await api.imoveis_geo("DF", municipio="Brasília")


async def test_geo_vazio_preserva_colunas_tipos_e_crs(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(
        client,
        "fetch_imoveis_geo",
        AsyncMock(return_value=([geo_capture("df_vazio_geo.json")], URL)),
    )

    frame, meta = await api.imoveis_geo(
        "DF", municipio=5300108, criado_apos="2099-01-01", return_meta=True
    )

    with collect_failures() as check:
        for nome, resultado in (
            ("página vazia", frame),
            ("sem páginas", parser.parse_imoveis_geojson([])),
        ):
            with check(nome):
                assert resultado.empty
                assert list(resultado.columns) == COLUNAS_IMOVEIS_GEO
                assert resultado.crs is not None and resultado.crs.to_epsg() == 4326
                assert str(resultado["data_criacao"].dtype) == "datetime64[ns, UTC]"
                assert str(resultado["data_atualizacao"].dtype) == "datetime64[ns, UTC]"
    assert meta.records_count == 0


async def test_condicao_nula_na_fonte_continua_nula(monkeypatch: pytest.MonkeyPatch):
    xsd = ElementTree.fromstring(geo_capture("describe_df.xsd"))
    campo = next(
        elemento
        for elemento in xsd.iter("{http://www.w3.org/2001/XMLSchema}element")
        if elemento.get("name") == "condicao"
    )
    assert (campo.get("nillable"), campo.get("minOccurs")) == ("true", "0")
    geo = json.loads(geo_capture("df_geo_srs4326_count3.json"))
    tabular = json.loads(gzip.decompress((R11 / "sicar_df_001.json.gz").read_bytes()))
    for documento in (geo, tabular):
        documento["features"][0]["properties"]["condicao"] = None
        del documento["features"][1]["properties"]["condicao"]
    monkeypatch.setattr(
        client, "fetch_imoveis_geo", AsyncMock(return_value=([json.dumps(geo).encode()], URL))
    )
    monkeypatch.setattr(
        client, "fetch_imoveis", AsyncMock(return_value=([json.dumps(tabular).encode()], URL))
    )

    resultados = {
        "geo": (await api.imoveis_geo("DF", municipio="Brasília"), geo),
        "tabular": (await api.imoveis("DF", municipio=5300108), tabular),
    }

    with collect_failures() as check:
        for nome, (frame, documento) in resultados.items():
            with check(nome):
                esperado = {
                    feature["properties"]["cod_imovel"]: feature["properties"].get("condicao")
                    for feature in documento["features"]
                }
                obtido = {
                    codigo: None if pd.isna(valor) else valor
                    for codigo, valor in zip(frame["cod_imovel"], frame["condicao"], strict=True)
                }
                assert obtido == esperado
                contracts.validate_dataset(
                    pd.DataFrame(frame.drop(columns="geometry", errors="ignore")), "sicar_imoveis"
                )


async def test_geo_incompativel_vira_parse_error(monkeypatch: pytest.MonkeyPatch):
    base = json.loads(geo_capture("df_geo_srs4326_count3.json"))
    misto = copy.deepcopy(base)
    misto["features"][1]["properties"]["dat_criacao"] = "2019-12-12T13:53:36.353"
    invalido = copy.deepcopy(base)
    invalido["features"][0]["properties"]["status_imovel"] = "INVALIDO"
    sem_area = copy.deepcopy(base)
    for feature in sem_area["features"]:
        del feature["properties"]["area"]
    casos = [
        ("fusos misturados", json.dumps(misto).encode(), "com e sem fuso"),
        ("registro inválido", json.dumps(invalido).encode(), "Status invalido"),
        ("propriedade ausente", json.dumps(sem_area).encode(), "Colunas obrigatorias"),
        ("JSON inválido", b"not json at all {{{", "GeoJSON"),
    ]
    with collect_failures() as check:
        for nome, body, mensagem in casos:
            with check(nome), pytest.raises(ParseError, match=mensagem):
                monkeypatch.setattr(
                    client, "fetch_imoveis_geo", AsyncMock(return_value=([body], URL))
                )
                await api.imoveis_geo("DF", municipio="Brasília")


def test_aviso_de_truncamento_so_quando_a_pagina_unica_atinge_o_limite(
    monkeypatch: pytest.MonkeyPatch,
):
    unica = geo_capture("df_geo_srs4326_count3.json")
    duas = [unica, geo_capture("ms_5007901_stream_p1.json")]
    casos = [
        ([unica], 3, 3, ["sicar_geo_truncated"]),
        ([unica], 4, 3, []),
        (duas, 1, 4, []),
    ]
    with collect_failures() as check:
        for pages, limite, linhas, esperado in casos:
            with check((len(pages), limite)):
                logger = Mock()
                monkeypatch.setattr(geo_utils, "logger", logger)
                frame = parser.parse_imoveis_geojson(pages, max_features=limite)
                assert len(frame) == linhas
                assert [call.args[0] for call in logger.warning.call_args_list] == esperado


async def test_resumo_municipal_agrega_as_linhas_publicadas(monkeypatch: pytest.MonkeyPatch):
    body = gzip.decompress((R11 / "sicar_df_001.json.gz").read_bytes())
    features = json.loads(body)["features"]
    fetch = AsyncMock(side_effect=[([body], URL), ([], URL)])
    monkeypatch.setattr(client, "fetch_imoveis", fetch)

    frame = await api.resumo("DF", municipio="Brasília")
    vazio = await api.resumo("DF", municipio="5300108")

    assert frame.to_dict("records") == [pytest.approx(resumo_esperado(features), rel=1e-12)]
    assert vazio.to_dict("records") == [
        dict.fromkeys(resumo_esperado(features), 0)
        | dict.fromkeys(("area_total_ha", "area_media_ha", "modulos_fiscais_medio"), 0.0)
    ]
    assert [call.args[:2] for call in fetch.await_args_list] == [
        ("DF", "cod_municipio_ibge=5300108"),
        ("DF", "cod_municipio_ibge=5300108"),
    ]


async def test_resumo_estadual_conta_cada_status_pela_sondagem(monkeypatch: pytest.MonkeyPatch):
    contagens = {
        None: 21006,
        "status_imovel='AT'": 19000,
        "status_imovel='PE'": 1200,
        "status_imovel='SU'": 500,
        "status_imovel='CA'": 306,
    }

    async def hits(uf: str, cql_filter: str | None = None, **_kwargs: Any) -> int:
        assert uf == "DF"
        return contagens[cql_filter]

    monkeypatch.setattr(client, "fetch_hits", hits)
    frame = await api.resumo("df")
    assert frame.to_dict("records") == [
        {"total": 21006, "ativos": 19000, "pendentes": 1200, "suspensos": 500, "cancelados": 306}
    ]


async def test_resumo_estadual_rotula_a_contagem_de_feicoes_publicadas(
    monkeypatch: pytest.MonkeyPatch,
):
    body = (GOLDEN / "sicar/versoes_20260916/go_duplicates.json").read_bytes()
    feicoes = json.loads(body)["features"]
    assert len({feicao["properties"]["cod_imovel"] for feicao in feicoes}) == 1
    monkeypatch.setattr(client, "fetch_hits", AsyncMock(return_value=len(feicoes)))
    monkeypatch.setattr(client, "fetch_imoveis", AsyncMock(return_value=([body], URL)))

    estadual, meta = await api.resumo("GO", return_meta=True)
    municipal = await api.resumo("GO", municipio=5205802)

    assert (estadual.loc[0, "total"], municipal.loc[0, "total"]) == (2, 1)
    assert meta.source_details.get("sicar") == {"unidade": "feicoes_publicadas"}
    assert meta.validation_warnings == [
        "sicar: o resumo sem município conta feições publicadas, e versões do mesmo cod_imovel "
        "contam separado; com municipio, conta imóveis (uma versão por cod_imovel)"
    ]


async def test_contagem_sai_uma_vez_e_o_aviso_de_volume_vem_do_client(
    monkeypatch: pytest.MonkeyPatch,
):
    tabular = gzip.decompress((R11 / "sicar_df_001.json.gz").read_bytes())
    geo = json.loads(geo_capture("df_geo_srs4326_count3.json"))
    geo.update(numberMatched=3, totalFeatures=3)
    corpos = {"imoveis": tabular, "imoveis_geo": json.dumps(geo).encode()}
    casos: list[tuple[str, dict[str, Any], int, int, int, str | None]] = [
        ("imoveis", {}, 62, 61, 1, "sicar_large_query"),
        ("imoveis", {}, 62, 62, 1, None),
        ("imoveis_geo", {"max_registros": None}, 3, 2, 1, "sicar_geo_large_query"),
        ("imoveis_geo", {"max_registros": None}, 3, 3, 1, None),
        ("imoveis_geo", {"max_registros": 5_000}, 3, 2, 0, None),
    ]
    with collect_failures() as check:
        for nome, filtros, total, limiar, chamadas, evento in casos:
            with check((nome, filtros, limiar)):
                logger = Mock()
                hits = AsyncMock(return_value=total)
                monkeypatch.setattr(client, "logger", logger)
                monkeypatch.setattr(client.models, "MAX_FEATURES_WARNING", limiar)
                monkeypatch.setattr(client, "fetch_hits", hits)
                monkeypatch.setattr(client, "fetch_wfs", AsyncMock(return_value=corpos[nome]))
                frame = await getattr(api, nome)("DF", **filtros)
                assert len(frame) == total
                avisos = [call.args[0] for call in logger.warning.call_args_list]
                assert avisos == ([] if evento is None else [evento])
                assert hits.await_count == chamadas


def test_docstring_do_pacote_nao_afirma_licenca_cc_by():
    assert "CC-BY" not in sicar.__doc__
    assert "não comprovada" in sicar.__doc__


async def test_parse_roda_fora_do_loop(monkeypatch: pytest.MonkeyPatch):
    tabular = gzip.decompress((R11 / "sicar_df_001.json.gz").read_bytes())
    monkeypatch.setattr(client, "fetch_imoveis", AsyncMock(return_value=([tabular], URL)))
    monkeypatch.setattr(
        client,
        "fetch_imoveis_geo",
        AsyncMock(return_value=([geo_capture("df_geo_srs4326_count3.json")], URL)),
    )
    threads: list[int] = []
    for nome in ("parse_imoveis_json", "parse_imoveis_geojson"):
        original = getattr(parser, nome)
        monkeypatch.setattr(
            parser,
            nome,
            lambda *a, _f=original, **k: threads.append(threading.get_ident()) or _f(*a, **k),
        )

    await api.imoveis("DF", municipio=5300108)
    await api.imoveis_geo("DF", municipio=5300108, max_registros=3)

    assert len(threads) == 2
    assert threading.get_ident() not in threads


@pytest.mark.parametrize("geo", [False, True])
async def test_vazio_sai_com_os_dtypes_do_cheio(monkeypatch: pytest.MonkeyPatch, geo: bool):
    if geo:
        pytest.importorskip("geopandas")
        nome, funcao = "fetch_imoveis_geo", api.imoveis_geo
        cheio, vazio = geo_capture("df_geo_srs4326_count3.json"), geo_capture("df_vazio_geo.json")
    else:
        nome, funcao = "fetch_imoveis", api.imoveis
        cheio = gzip.decompress((R11 / "sicar_df_001.json.gz").read_bytes())
        vazio = json.dumps(
            {"type": "FeatureCollection", "features": [], "numberMatched": 0, "numberReturned": 0}
        ).encode()
    frames = []
    for corpo in (cheio, vazio):
        monkeypatch.setattr(client, nome, AsyncMock(return_value=([corpo], URL)))
        with helpers.sem_excecao():
            frames.append(await funcao("DF", municipio=5300108))
    preenchido, sem_linhas = frames
    assert not preenchido.empty and sem_linhas.empty
    assert sem_linhas.dtypes.astype(str).to_dict() == preenchido.dtypes.astype(str).to_dict()
