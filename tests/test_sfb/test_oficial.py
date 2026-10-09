from __future__ import annotations

import json
import warnings
from types import SimpleNamespace

import httpx
import pytest

from agrobr import sfb
from agrobr.exceptions import InvalidParameterError, SourceUnavailableError
from agrobr.sfb import client
from agrobr.utils import geo
from tests import helpers
from tests.test_sfb import oficial

UFS = ("DF", "AP", "ES", "MA", "PA")
DF = (-48.3, -16.1, -47.3, -15.4)
INVALIDOS = {
    "uf": ("XX", InvalidParameterError),
    "bioma": ("Lunar", InvalidParameterError),
    "categoria": ("PARQUE ESTADUAL", InvalidParameterError),
    "bbox": ((-30.0, -1.0, -31.0, -2.0), ValueError),
}
IFN_DECLARADO = [
    {
        "co_pontos_lote": i,
        "co_lote": 7,
        "nu_ciclo_execucao": "1",
        "no_conglomerado": f"C{i:04d}",
        "no_uf": "DF",
        "no_municipio": "Brasília",
        "no_bioma": "CERRADO",
    }
    for i in range(2001)
]
FILTROS = (
    (sfb.cnfp, ("uf", "bioma", "categoria", "bbox")),
    (sfb.cnfp_geo, ("uf", "bioma", "categoria", "bbox")),
    (sfb.concessoes, ("uf", "bbox")),
    (sfb.concessoes_geo, ("uf", "bbox")),
    (sfb.ifn_conglomerados, ("uf", "bioma", "bbox")),
    (sfb.ifn_conglomerados_geo, ("uf", "bioma", "bbox")),
)


def instalar(
    monkeypatch: pytest.MonkeyPatch,
    corpos: dict[str, bytes] | None = None,
    resto: bytes | None = None,
) -> list[str]:
    servidos = corpos or {}
    pedidos: list[str] = []

    def responder(request: httpx.Request) -> httpx.Response:
        pedidos.append(str(request.url))
        corpo = servidos.get(str(request.url), resto)
        if corpo is None:
            return httpx.Response(404, request=request)
        return httpx.Response(
            200, content=corpo, headers={"content-type": "application/json"}, request=request
        )

    namespace = SimpleNamespace(**vars(httpx))
    namespace.AsyncClient = lambda **kwargs: httpx.AsyncClient(
        transport=httpx.MockTransport(responder), **kwargs
    )
    monkeypatch.setattr(geo, "httpx", namespace)
    monkeypatch.setattr(client, "httpx", namespace)
    return pedidos


async def test_cnfp_de_cada_uf_confere_com_a_fonte(monkeypatch):
    pedidos = instalar(monkeypatch, oficial.corpos(*(f"cnfp_{uf.lower()}" for uf in UFS)))

    with helpers.collect_failures() as check:
        for uf in UFS:
            cenario = f"cnfp_{uf.lower()}"
            with check(uf):
                pedidos.clear()
                with (
                    helpers.capturar_logs() as logs,
                    helpers.sem_excecao(),
                    pytest.warns(
                        UserWarning, match=f"{oficial.ambiguos(cenario)} registros"
                    ) as avisos,
                ):
                    df, meta = await sfb.cnfp(uf=uf, return_meta=True)
                assert meta.validation_warnings == [str(aviso.message) for aviso in avisos]
                exemplo = next(
                    linha["anocriacao"]
                    for linha in oficial.feicoes(cenario)
                    if len(set(oficial._ANO.findall(linha["anocriacao"] or ""))) > 1
                )
                assert exemplo in meta.validation_warnings[0]
                assert pedidos == oficial.urls(cenario)
                assert list(df.columns) == oficial.COLUNAS_CNFP
                assert str(df["ano_criacao"].dtype) == "Int64"
                assert oficial.publicado(df) == oficial.esperado_cnfp(cenario)
                ambiguo = df[df["ano_criacao_texto"] == exemplo]
                assert len(ambiguo) and ambiguo["ano_criacao"].isna().all()
                assert meta.schema_version == "1.1"
                assert meta.source_url == oficial.urls(cenario)[1]
                assert (meta.source_method, meta.selected_source, meta.attempted_sources) == (
                    "httpx+arcgis+json",
                    "sfb_cnfp",
                    ["sfb_cnfp"],
                )
                assert meta.records_count == len(df)
                assert [
                    log["linhas"] for log in logs if log["event"] == "sfb_ano_criacao_ambiguo"
                ] == [oficial.ambiguos(cenario)]


async def test_filtros_do_cnfp(monkeypatch):
    pedidos = instalar(monkeypatch, oficial.corpos("cnfp_df_cerrado_apa"))

    with helpers.sem_excecao():
        df, meta = await sfb.cnfp(
            uf="df", bioma="Cerrado", categoria="apa", bbox=DF, return_meta=True
        )

    assert pedidos == oficial.urls("cnfp_df_cerrado_apa")
    pagina = oficial.GOLDEN / oficial.respostas("cnfp_df_cerrado_apa")[1]["file"]
    helpers.conferir_corpo(meta, pagina.read_bytes())
    assert oficial.publicado(df) == oficial.esperado_cnfp("cnfp_df_cerrado_apa")
    assert {linha["categoria"] for linha in oficial.publicado(df)} == {"APA"}


async def test_concessoes_conferem_com_a_fonte(monkeypatch):
    pedidos = instalar(monkeypatch, oficial.corpos("concessoes", "concessoes_pa"))

    with helpers.collect_failures() as check:
        for cenario, filtros in (
            ("concessoes", {}),
            ("concessoes_pa", {"uf": "PA", "bbox": (-60.0, -10.0, -45.0, 0.0)}),
        ):
            with check(cenario):
                pedidos.clear()
                with helpers.sem_excecao():
                    df, meta = await sfb.concessoes(**filtros, return_meta=True)
                assert pedidos == oficial.urls(cenario)
                assert list(df.columns) == oficial.COLUNAS_CONCESSOES
                assert str(df["ano_criacao"].dtype) == "Int64"
                assert oficial.publicado(df) == oficial.esperado_concessoes(cenario)
                assert (meta.source_url, meta.selected_source) == (
                    oficial.urls(cenario)[1],
                    "sfb_concessoes",
                )


async def test_geometria_confere_com_os_aneis_oficiais(monkeypatch):
    pytest.importorskip("geopandas")
    pedidos = instalar(monkeypatch, oficial.corpos("cnfp_geo_df_arie", "concessoes_geo_ro"))

    with helpers.collect_failures() as check:
        for camada, chamada, cenario, esperado, aneis in (
            (
                "cnfp",
                lambda: sfb.cnfp_geo(
                    uf="DF", bioma="Cerrado", categoria="ARIE", bbox=DF, return_meta=True
                ),
                "cnfp_geo_df_arie",
                oficial.esperado_cnfp,
                "geometria_cnfp_df_arie.json",
            ),
            (
                "concessoes",
                lambda: sfb.concessoes_geo(
                    uf="RO", bbox=(-66.0, -13.0, -59.0, -7.0), return_meta=True
                ),
                "concessoes_geo_ro",
                oficial.esperado_concessoes,
                "geometria_concessoes_ro.json",
            ),
        ):
            with check(camada):
                pedidos.clear()
                with helpers.sem_excecao():
                    gdf, meta = await chamada()
                assert pedidos == oficial.urls(cenario)
                assert gdf.crs.to_epsg() == 4326
                assert oficial.publicado(gdf) == esperado(cenario)
                assert (meta.source_url, meta.source_method, meta.selected_source) == (
                    oficial.urls(cenario)[1],
                    "httpx+arcgis+geojson",
                    f"sfb_{camada}_geo",
                )
                pagina = oficial.GOLDEN / oficial.respostas(cenario)[1]["file"]
                helpers.conferir_corpo(meta, pagina.read_bytes())
                oficiais = oficial.aneis_oficiais(aneis)
                for fid, geometria in zip(gdf["fid"], gdf.geometry, strict=True):
                    poligonos = getattr(geometria, "geoms", [geometria])
                    contornos = [p.exterior for p in poligonos] + [
                        i for p in poligonos for i in p.interiors
                    ]
                    assert (
                        oficial.sem_orientacao(
                            [[list(ponto) for ponto in anel.coords] for anel in contornos]
                        )
                        == oficiais[int(fid)]
                    ), fid
    with helpers.sem_excecao():
        sem_meta = await sfb.concessoes_geo(uf="RO", bbox=(-66.0, -13.0, -59.0, -7.0))
    assert type(sem_meta).__name__ == "GeoDataFrame"
    assert oficial.publicado(sem_meta) == oficial.esperado_concessoes("concessoes_geo_ro")


async def test_bbox_sem_feicao_devolve_tabela_vazia(monkeypatch):
    pedidos = instalar(monkeypatch, oficial.corpos("concessoes_oceano"))

    with helpers.sem_excecao():
        df = await sfb.concessoes(bbox=(-31.0, -2.0, -30.0, -1.0))

    assert pedidos == oficial.urls("concessoes_oceano")
    assert df.empty
    assert list(df.columns) == oficial.COLUNAS_CONCESSOES


async def test_ifn_fora_do_ar_vira_erro(monkeypatch):
    erro = (oficial.GOLDEN / "ifn_df_00.json").read_bytes()
    pedidos = instalar(monkeypatch, resto=erro)

    with helpers.collect_failures() as check:
        for cenario, funcao in (
            ("ifn_df", sfb.ifn_conglomerados),
            ("ifn_geo_df", sfb.ifn_conglomerados_geo),
        ):
            with check(cenario):
                pedidos.clear()
                with helpers.levanta_exatamente(SourceUnavailableError, match="not started"):
                    await funcao(uf="DF", bioma="Cerrado", bbox=DF)
                assert len(pedidos) == 1
                assert "/dataset_ifn_tb_pontos_lote/FeatureServer/0/query" in pedidos[0]


def ifn_declarado(monkeypatch: pytest.MonkeyPatch, total: dict[str, int]) -> list[dict[str, str]]:
    pedidos: list[dict[str, str]] = []

    def responder(request: httpx.Request) -> httpx.Response:
        params = dict(request.url.params)
        pedidos.append({"path": request.url.path, **params})
        if "/dataset_ifn_tb_lote/" in request.url.path:
            corpo = (
                {"count": 1}
                if params.get("returnCountOnly") == "true"
                else {"features": [{"attributes": {"co_lote": 7, "no_lote": "Lote 7"}}]}
            )
            return httpx.Response(200, json=corpo, request=request)
        if params.get("returnCountOnly") == "true":
            return httpx.Response(200, json=total, request=request)
        inicio = int(params["where"].rsplit(" > ", 1)[1]) + 1 if " > " in params["where"] else 0
        linhas = IFN_DECLARADO[inicio : inicio + int(params["resultRecordCount"])]
        if params["f"] == "geojson":
            feicoes = [
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [-47.9, -15.8]},
                    "properties": linha,
                }
                for linha in linhas
            ]
            corpo = {"type": "FeatureCollection", "features": feicoes}
        else:
            corpo = {
                "objectIdFieldName": "co_pontos_lote",
                "spatialReference": {"wkid": 4326},
                "exceededTransferLimit": False,
                "features": [{"attributes": linha} for linha in linhas],
            }
        return httpx.Response(200, json=corpo, request=request)

    namespace = SimpleNamespace(**vars(httpx))
    namespace.AsyncClient = lambda **kwargs: httpx.AsyncClient(
        transport=httpx.MockTransport(responder), **kwargs
    )
    monkeypatch.setattr(geo, "httpx", namespace)
    monkeypatch.setattr(client, "httpx", namespace)
    return pedidos


async def test_ifn_pagina_por_chave_com_corpo_minimo_declarado(monkeypatch):
    total = {"count": 2001}
    pedidos = ifn_declarado(monkeypatch, total)

    with helpers.sem_excecao():
        df, meta = await sfb.ifn_conglomerados(uf="DF", return_meta=True)

    pontos = [p for p in pedidos if "/dataset_ifn_tb_pontos_lote/" in p["path"]]
    assert [(p.get("orderByFields"), p.get("resultRecordCount")) for p in pontos] == [
        (None, None),
        ("co_pontos_lote", "2000"),
        ("co_pontos_lote", "1"),
    ]
    assert [p["where"] for p in pontos] == [
        "no_uf='DF'",
        "no_uf='DF'",
        "(no_uf='DF') AND co_pontos_lote > 1999",
    ]
    assert [p.get("returnGeometry") for p in pontos[1:]] == ["false", "false"]
    assert list(df.columns) == [
        "id",
        "codigo_lote",
        "lote",
        "conglomerado",
        "uf",
        "municipio",
        "bioma",
        "ciclo",
    ]
    assert df["id"].tolist() == list(range(2001))
    assert "orderByFields=co_pontos_lote" in meta.source_url
    assert meta.selected_source == "sfb_ifn_conglomerados"

    total["count"] = 2002
    with helpers.levanta_exatamente(SourceUnavailableError, match="2001 de 2002"):
        await sfb.ifn_conglomerados(uf="DF")


async def test_ifn_geo_com_corpo_minimo_declarado(monkeypatch):
    pytest.importorskip("geopandas")
    ifn_declarado(monkeypatch, {"count": 3})

    with helpers.sem_excecao():
        gdf, meta = await sfb.ifn_conglomerados_geo(uf="DF", return_meta=True)

    assert gdf.crs.to_epsg() == 4326
    assert gdf["id"].tolist() == [0, 1, 2]
    assert meta.selected_source == "sfb_ifn_conglomerados_geo"


async def test_cnfp_em_2_paginas_nao_carimba_o_corpo(monkeypatch):
    pedidos = instalar(monkeypatch, oficial.corpos("cnfp_pa"))

    with helpers.sem_excecao():
        _, meta = await sfb.cnfp(uf="PA", return_meta=True)

    assert pedidos == oficial.urls("cnfp_pa")
    assert (meta.raw_content_hash, meta.raw_content_size) == (None, 0)


async def test_ifn_geo_em_2_paginas_carimba_o_manifesto(monkeypatch):
    pytest.importorskip("geopandas")
    pedidos = ifn_declarado(monkeypatch, {"count": 2001})

    with helpers.sem_excecao():
        gdf, meta = await sfb.ifn_conglomerados_geo(uf="DF", return_meta=True)

    assert [
        p.get("resultRecordCount") for p in pedidos if p.get("orderByFields") == "co_pontos_lote"
    ] == ["2000", "1"]
    assert len(gdf) == 2001
    assert meta.raw_content_hash is not None and meta.raw_content_size > 0
    assert [r["role"] for r in meta.source_details["resources"]] == ["pontos", "pontos", "lotes"]


async def test_resposta_invalida_nao_vira_dado():
    waf = (oficial.GOLDEN / "waf_request_rejected.html").read_bytes()
    contagem, pagina = oficial.urls("cnfp_df")
    vazia = json.dumps({**json.loads(oficial.corpos("cnfp_df")[pagina]), "features": []}).encode()
    with helpers.collect_failures() as check:
        for nome, trocas, resto, mensagem in (
            ("WAF na contagem", {contagem: waf}, None, "não é JSON"),
            ("WAF na página", {pagina: waf}, None, "HTML"),
            (
                "página faltando",
                {
                    contagem: b'{"count":30}',
                    pagina.replace("resultRecordCount=29", "resultRecordCount=30"): oficial.corpos(
                        "cnfp_df"
                    )[pagina],
                },
                vazia,
                "29 de 30",
            ),
        ):
            with check(nome), pytest.MonkeyPatch.context() as mp:
                instalar(mp, {**oficial.corpos("cnfp_df"), **trocas}, resto)
                with helpers.levanta_exatamente(SourceUnavailableError, match=mensagem):
                    await sfb.cnfp(uf="DF")


async def test_parametros_invalidos_recusados_antes_da_rede(monkeypatch):
    pedidos = instalar(monkeypatch)

    with helpers.collect_failures() as check:
        for funcao, parametros in FILTROS:
            for parametro in parametros:
                valor, erro = INVALIDOS[parametro]
                with check(f"{funcao.__name__} {parametro}"), helpers.levanta_exatamente(erro):
                    await funcao(**{parametro: valor})
    assert pedidos == []


async def test_argumento_desconhecido_recusado_antes_da_rede(monkeypatch):
    pedidos = instalar(monkeypatch)

    with helpers.collect_failures() as check:
        for funcao, _ in FILTROS:
            with check(funcao.__name__), helpers.levanta_exatamente(TypeError, match="municipio"):
                await funcao(uf="DF", municipio="Brasília")
    assert pedidos == []


async def test_as_polars_publica_os_mesmos_valores(monkeypatch):
    pl = pytest.importorskip("polars")
    instalar(monkeypatch, oficial.corpos("cnfp_df", "concessoes"))

    with helpers.sem_excecao():
        cnfp = await sfb.cnfp(uf="DF", as_polars=True)
        concessoes = await sfb.concessoes(as_polars=True)

    assert isinstance(cnfp, pl.DataFrame)
    assert isinstance(concessoes, pl.DataFrame)
    assert oficial.publicado(cnfp.to_pandas()) == oficial.esperado_cnfp("cnfp_df")
    assert oficial.publicado(concessoes.to_pandas()) == oficial.esperado_concessoes("concessoes")

    ifn_declarado(monkeypatch, {"count": 3})
    with helpers.sem_excecao():
        ifn = await sfb.ifn_conglomerados(uf="DF", as_polars=True)
    assert isinstance(ifn, pl.DataFrame)
    assert ifn["id"].to_list() == [0, 1, 2]


async def test_geometria_invalida_publicada_vira_aviso_sem_reparo(monkeypatch):
    pytest.importorskip("geopandas")
    corpos = oficial.corpos("cnfp_geo_df_arie", "concessoes_geo_ro")
    url = oficial.urls("cnfp_geo_df_arie")[1]
    pagina = json.loads(corpos[url])
    gravata = [[-47.9, -15.8], [-47.8, -15.7], [-47.8, -15.8], [-47.9, -15.7], [-47.9, -15.8]]
    pagina["features"][0]["geometry"] = {"type": "Polygon", "coordinates": [gravata]}
    corpos[url] = json.dumps(pagina).encode()
    instalar(monkeypatch, corpos)
    aviso = f"SFB cnfp: 1 de {len(pagina['features'])} geometrias inválidas como publicadas pela fonte; use make_valid antes de operações espaciais."
    cnfp = {"uf": "DF", "bioma": "Cerrado", "categoria": "ARIE", "bbox": DF}
    with warnings.catch_warnings(record=True) as avisos, helpers.sem_excecao():
        warnings.simplefilter("always")
        gdf, meta = await sfb.cnfp_geo(**cnfp, return_meta=True)
        await sfb.cnfp_geo(**cnfp)
        validas, meta_validas = await sfb.concessoes_geo(
            uf="RO", bbox=(-66.0, -13.0, -59.0, -7.0), return_meta=True
        )
    assert [str(a.message) for a in avisos if "geometrias" in str(a.message)] == [aviso, aviso]
    assert meta.validation_warnings.count(aviso) == 1
    assert (meta.source_details["geometrias_invalidas"], meta.source_details["geometrias"]) == (
        1,
        len(pagina["features"]),
    )
    assert int((~gdf.geometry.is_valid).sum()) == 1
    assert (
        validas.geometry.is_valid.all()
        and "geometrias_invalidas" not in meta_validas.source_details
    )
    assert not [a for a in meta_validas.validation_warnings if "geometrias" in a]


async def test_geometria_vazia_publicada_nao_gera_aviso(monkeypatch):
    pytest.importorskip("geopandas")
    corpos = oficial.corpos("cnfp_geo_df_arie")
    url = oficial.urls("cnfp_geo_df_arie")[1]
    pagina = json.loads(corpos[url])
    pagina["features"][0]["geometry"] = {"type": "Polygon", "coordinates": []}
    corpos[url] = json.dumps(pagina).encode()
    instalar(monkeypatch, corpos)
    with warnings.catch_warnings(record=True) as avisos, helpers.sem_excecao():
        warnings.simplefilter("always")
        gdf, meta = await sfb.cnfp_geo(
            uf="DF", bioma="Cerrado", categoria="ARIE", bbox=DF, return_meta=True
        )
    assert gdf.geometry.iloc[0].is_empty
    assert [str(aviso.message) for aviso in avisos] == []
    assert meta.validation_warnings == []
    assert "geometrias_invalidas" not in meta.source_details
