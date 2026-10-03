from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import httpx
import pandas as pd
import pytest

from agrobr import sfb
from agrobr.exceptions import ParseError, SourceUnavailableError
from agrobr.sfb import api, client, models, parser
from agrobr.utils import geo

GOLDEN = Path(__file__).parents[1] / "golden_data/sfb/ifn_migracao_20261002"


def instalar(monkeypatch, *, geometria=False, lotes=None, pontos=None, count=None):
    pedidos = []
    if pontos is None:
        pontos = (
            GOLDEN / ("ponto_5454_geo.json" if geometria else "pontos_df_tab.json")
        ).read_bytes()
    cadastro = (GOLDEN / "lotes_df.json").read_bytes() if lotes is None else lotes

    def responder(request):
        pedidos.append(request)
        params = request.url.params
        if "/Conglomerado/" in request.url.path:
            raw = (GOLDEN.parent / "oficial_20260923/ifn_df_00.json").read_bytes()
            return httpx.Response(200, content=raw)
        auxiliar = "/dataset_ifn_tb_lote/" in request.url.path
        if auxiliar and "error" in json.loads(cadastro):
            return httpx.Response(200, content=cadastro)
        if params.get("returnCountOnly") == "true":
            if auxiliar:
                total = len(json.loads(cadastro)["features"])
            elif "no_bioma='CERRADO'" in params["where"]:
                total = 0
            else:
                total = count if count is not None else len(json.loads(pontos)["features"])
            return httpx.Response(200, json={"count": total})
        return httpx.Response(200, content=cadastro if auxiliar else pontos)

    namespace = SimpleNamespace(**vars(httpx))
    namespace.AsyncClient = lambda **kwargs: httpx.AsyncClient(
        transport=httpx.MockTransport(responder), **kwargs
    )
    monkeypatch.setattr(geo, "httpx", namespace)
    monkeypatch.setattr(client, "httpx", namespace, raising=False)
    return pedidos


async def test_ifn_df_oficial_preserva_lote_e_ciclo(monkeypatch):
    pedidos = instalar(monkeypatch)
    esperado = json.loads((GOLDEN / "expected.json").read_bytes())

    df, meta = await sfb.ifn_conglomerados(uf="DF", bioma="Cerrado", return_meta=True)

    assert len(df) == esperado["records"] == 68
    assert df.iloc[0].to_dict() == esperado["first"]
    assert df["id"].tolist() == list(range(5454, 5522))
    assert df["lote"].unique().tolist() == ["DF-01"]
    assert df["ciclo"].unique().tolist() == ["1"]
    assert meta.schema_version == "1.1"
    assert meta.selected_source == "sfb_ifn_conglomerados"
    paginas = [r for r in pedidos if r.url.params.get("returnCountOnly") != "true"]
    assert all(r.url.params["returnGeometry"] == "false" for r in paginas)
    assert "no_lote" not in paginas[0].url.params["outFields"].split(",")


def test_filtro_ifn_usa_a_comparacao_com_resultado_publicado():
    assert api._build_where("ifn_conglomerados", uf="DF", bioma="Cerrado") == (
        "no_uf='DF' AND UPPER(no_bioma)='CERRADO'"
    )


def test_golden_ifn_preserva_os_hashes_da_fonte():
    registros = json.loads((GOLDEN / "metadata.json").read_bytes())["resources"]
    for registro in registros:
        corpo = (GOLDEN / registro["file"]).read_bytes()
        assert hashlib.sha256(corpo).hexdigest() == registro["sha256"]
        assert len(corpo) == registro["bytes"]


async def test_ifn_geo_confere_ponto_publicado_e_crs(monkeypatch):
    pytest.importorskip("geopandas")
    pedidos = instalar(monkeypatch, geometria=True)
    gdf, meta = await sfb.ifn_conglomerados_geo(uf="DF", return_meta=True)
    esperado = json.loads((GOLDEN / "expected.json").read_bytes())

    assert gdf.drop(columns="geometry").iloc[0].to_dict() == esperado["first"]
    assert list(gdf.geometry.iloc[0].coords)[0] == pytest.approx(esperado["geometry_5454"])
    assert gdf.crs.to_epsg() == 4326
    assert gdf.columns[-1] == "geometry"
    assert meta.schema_version == "1.1"
    assert meta.selected_source == "sfb_ifn_conglomerados_geo"
    auxiliares = [r for r in pedidos if "/dataset_ifn_tb_lote/" in r.url.path]
    assert auxiliares[-1].url.params["returnGeometry"] == "false"
    assert auxiliares[-1].url.params["f"] == "json"


@pytest.mark.parametrize("geometria", [False, True])
async def test_ifn_vazio_preserva_tipos_e_nao_busca_lotes(monkeypatch, geometria):
    if geometria:
        pytest.importorskip("geopandas")
    cheio = parser.parse_ifn(
        [(GOLDEN / ("ponto_5454_geo.json" if geometria else "pontos_df_tab.json")).read_bytes()],
        [(GOLDEN / "lotes_df.json").read_bytes()],
        geometria=geometria,
    )
    pedidos = instalar(monkeypatch, count=0)
    funcao = sfb.ifn_conglomerados_geo if geometria else sfb.ifn_conglomerados
    vazio, meta = await funcao(uf="DF", return_meta=True)

    assert vazio.empty
    assert vazio.dtypes.equals(cheio.dtypes)
    assert str(vazio["id"].dtype) == str(vazio["codigo_lote"].dtype) == "Int64"
    assert len(pedidos) == 1
    assert meta.source_details["resources"] == []
    assert meta.raw_content_hash == hashlib.sha256(b"[]").hexdigest()
    if geometria:
        assert vazio.crs.to_epsg() == 4326


async def test_ifn_manifesto_confere_cada_corpo_e_url(monkeypatch):
    pedidos = instalar(monkeypatch)
    _, meta = await sfb.ifn_conglomerados(uf="DF", return_meta=True)
    paginas = [r for r in pedidos if r.url.params.get("returnCountOnly") != "true"]
    esperado = []
    for request, nome, role in zip(
        paginas, ["pontos_df_tab.json", "lotes_df.json"], ["pontos", "lotes"], strict=True
    ):
        raw = (GOLDEN / nome).read_bytes()
        esperado.append(
            {
                "role": role,
                "url": str(request.url),
                "sha256": hashlib.sha256(raw).hexdigest(),
                "bytes": len(raw),
            }
        )
    assert meta.source_details["resources"] == esperado
    assert meta.source_details["hash_kind"] == "resource_manifest_sha256"
    assert meta.source_details["manifest_encoding"] == "canonical_json_utf8"
    assert meta.source_details["manifest_fields"] == ["resources"]
    assert meta.source_details["manifest_root"] == "resources"
    assert meta.source_details[meta.source_details["manifest_root"]] == esperado
    manifesto = json.dumps(
        esperado, sort_keys=True, ensure_ascii=False, separators=(",", ":")
    ).encode()
    assert meta.raw_content_hash == hashlib.sha256(manifesto).hexdigest()
    assert meta.raw_content_size == len(manifesto)
    assert meta.source_details["resource_bytes"] == sum(r["bytes"] for r in esperado)


async def test_ifn_hash_muda_se_so_o_cadastro_mudar(monkeypatch):
    instalar(monkeypatch)
    _, original = await sfb.ifn_conglomerados(uf="DF", return_meta=True)
    lote = json.loads((GOLDEN / "lotes_df.json").read_bytes())
    lote["features"][0]["attributes"]["no_lote"] = "nome alterado"
    instalar(monkeypatch, lotes=json.dumps(lote).encode())
    _, alterado = await sfb.ifn_conglomerados(uf="DF", return_meta=True)

    assert original.raw_content_hash != alterado.raw_content_hash
    assert original.source_details["resources"][0] == alterado.source_details["resources"][0]


@pytest.mark.parametrize("geometria", [False, True])
@pytest.mark.parametrize("campo", ["nu_ciclo_execucao", "co_lote", "no_conglomerado"])
def test_ifn_campo_ausente_em_uma_feicao_e_erro(campo, geometria):
    if geometria:
        pytest.importorskip("geopandas")
    pontos = json.loads(
        (GOLDEN / ("ponto_5454_geo.json" if geometria else "pontos_df_tab.json")).read_bytes()
    )
    atributos = "properties" if geometria else "attributes"
    del pontos["features"][-1][atributos][campo]
    with pytest.raises(ParseError, match=campo):
        parser.parse_ifn(
            [json.dumps(pontos).encode()],
            [(GOLDEN / "lotes_df.json").read_bytes()],
            geometria=geometria,
        )


@pytest.mark.parametrize(
    "cenario,erro",
    [
        ("orfao", "sem correspondente"),
        ("duplicado", "duplicado"),
        ("campo", "no_lote"),
        ("extra", "não solicitados"),
    ],
)
def test_ifn_cadastro_inconsistente_e_recusado(cenario, erro):
    lotes = json.loads((GOLDEN / "lotes_df.json").read_bytes())
    if cenario == "orfao":
        lotes["features"] = []
    elif cenario == "duplicado":
        lotes["features"] *= 2
    elif cenario == "campo":
        del lotes["features"][0]["attributes"]["no_lote"]
    else:
        lotes["features"].append({"attributes": {"co_lote": 9999, "no_lote": "extra"}})
    with pytest.raises(ParseError, match=erro):
        parser.parse_ifn(
            [(GOLDEN / "pontos_df_tab.json").read_bytes()], [json.dumps(lotes).encode()]
        )


def test_ifn_nulos_preservados_com_aviso_de_validacao():
    pontos = json.loads((GOLDEN / "pontos_df_tab.json").read_bytes())
    pontos["features"][0]["attributes"]["co_lote"] = None
    pontos["features"][0]["attributes"]["nu_ciclo_execucao"] = None
    lotes = json.loads((GOLDEN / "lotes_df.json").read_bytes())
    lotes["features"][0]["attributes"]["no_lote"] = None
    df = parser.parse_ifn([json.dumps(pontos).encode()], [json.dumps(lotes).encode()])

    assert len(df) == 68
    assert df["lote"].isna().all()
    assert pd.isna(df.loc[0, "codigo_lote"]) and pd.isna(df.loc[0, "ciclo"])
    assert df.attrs["agrobr_avisos"] == [
        "IFN: 1 pontos com código de lote nulo e 67 pontos com nome de lote nulo publicado no cadastro."
    ]


@pytest.mark.parametrize(
    "campo,valor",
    [("co_lote", "29"), ("co_lote", 29.5), ("co_pontos_lote", True), ("nu_ciclo_execucao", 1)],
)
def test_ifn_tipo_invalido_nao_vira_nulo(campo, valor):
    pontos = json.loads((GOLDEN / "pontos_df_tab.json").read_bytes())
    pontos["features"][0]["attributes"][campo] = valor
    with pytest.raises(ParseError, match=campo):
        parser.parse_ifn([json.dumps(pontos).encode()], [(GOLDEN / "lotes_df.json").read_bytes()])


async def test_ifn_falha_auxiliar_nao_devolve_tabela_parcial(monkeypatch):
    instalar(monkeypatch, lotes=b'{"error":{"code":500,"message":"cadastro indisponivel"}}')
    with pytest.raises(SourceUnavailableError, match="cadastro indisponivel"):
        await sfb.ifn_conglomerados(uf="DF")


@pytest.mark.parametrize("cenario", ["duplicado", "invertido"])
async def test_ifn_paginacao_recusa_oid_repetido_ou_fora_de_ordem(monkeypatch, cenario):
    pontos = json.loads((GOLDEN / "pontos_df_tab.json").read_bytes())
    if cenario == "duplicado":
        pontos["features"][1] = pontos["features"][0]
    else:
        pontos["features"].reverse()
    instalar(monkeypatch, pontos=json.dumps(pontos).encode())
    with pytest.raises(ParseError, match="duplicado ou fora de ordem"):
        await sfb.ifn_conglomerados(uf="DF")


async def test_ifn_lotes_sao_consultados_em_blocos_limitados(monkeypatch):
    pedidos = []
    original = json.loads((GOLDEN / "pontos_df_tab.json").read_bytes())["features"][0]["attributes"]
    pontos = [{**original, "co_pontos_lote": i, "co_lote": i} for i in range(1, 102)]

    async def sem_pausa(_):
        return None

    def responder(request):
        params = request.url.params
        auxiliar = "/dataset_ifn_tb_lote/" in request.url.path
        if auxiliar:
            codes = [
                int(c)
                for c in params["where"].removeprefix("co_lote IN (").removesuffix(")").split(",")
            ]
            rows = [{"co_lote": c, "no_lote": "DF-01"} for c in codes]
            if params.get("returnCountOnly") != "true":
                pedidos.append(codes)
                assert params["returnGeometry"] == "false"
        else:
            rows = pontos
        if params.get("returnCountOnly") == "true":
            return httpx.Response(200, json={"count": len(rows)})
        return httpx.Response(200, json={"features": [{"attributes": row} for row in rows]})

    namespace = SimpleNamespace(**vars(httpx))
    namespace.AsyncClient = lambda **kwargs: httpx.AsyncClient(
        transport=httpx.MockTransport(responder), **kwargs
    )
    monkeypatch.setattr(geo, "httpx", namespace)
    monkeypatch.setattr(client, "httpx", namespace)
    monkeypatch.setattr(client.asyncio, "sleep", sem_pausa)
    df = await sfb.ifn_conglomerados(uf="DF")

    assert len(df) == 101
    assert df["id"].tolist() == list(range(1, 102))
    assert [len(batch) for batch in pedidos] == [models.IFN_LOTES_POR_CONSULTA, 1] == [100, 1]
    assert [code for batch in pedidos for code in batch] == list(range(1, 102))


async def test_ifn_polars_preserva_texto_publicado(monkeypatch):
    pl = pytest.importorskip("polars")
    instalar(monkeypatch)
    df = await sfb.ifn_conglomerados(uf="DF", as_polars=True)
    assert df["ciclo"].dtype == pl.String
    assert df.row(0, named=True) == json.loads((GOLDEN / "expected.json").read_bytes())["first"]
