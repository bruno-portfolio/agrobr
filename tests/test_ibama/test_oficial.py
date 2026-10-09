from __future__ import annotations

import threading
import warnings
from types import SimpleNamespace

import httpx
import pytest

from agrobr import ibama
from agrobr.exceptions import ParseError, SourceUnavailableError
from agrobr.ibama import _cache, client, parser
from agrobr.utils.warnings import warn_once_reset
from tests import helpers
from tests.test_ibama import oficial

EDICAO = "2026-09-23 00:54:47"


def instalar(
    monkeypatch: pytest.MonkeyPatch,
    corpo: bytes | None = None,
    content_type: str = "text/csv",
) -> list[str]:
    url = oficial.manifest()["url"]
    body = oficial.RECORTE.read_bytes() if corpo is None else corpo
    pedidos: list[str] = []

    def responder(request: httpx.Request) -> httpx.Response:
        pedidos.append(str(request.url))
        if str(request.url) != url:
            return httpx.Response(404, request=request)
        return httpx.Response(
            200, content=body, headers={"content-type": content_type}, request=request
        )

    namespace = SimpleNamespace(**vars(httpx))
    namespace.AsyncClient = lambda **kwargs: httpx.AsyncClient(
        transport=httpx.MockTransport(responder), **kwargs
    )
    monkeypatch.setattr(client, "httpx", namespace)
    monkeypatch.setattr(client, "MIN_CSV_BYTES", 1_000)
    return pedidos


async def test_saida_publica_confere_com_o_recorte_oficial(monkeypatch):
    registros = oficial.fonte()
    pedidos = instalar(monkeypatch)

    with warnings.catch_warnings(record=True) as emitidos, helpers.sem_excecao():
        warnings.simplefilter("always")
        df, meta = await ibama.embargos(return_meta=True)

    url = oficial.manifest()["url"]
    assert pedidos == [url]
    assert oficial.RECORTE.read_bytes().startswith(b'\xef\xbb\xbf"SEQ_TAD";')
    assert list(df.columns) == [saida for saida, _, _ in oficial.COLUNAS]
    assert df["seq_tad"].fillna("").tolist() == [r["SEQ_TAD"] for r in registros]
    assert oficial.publicado(df) == [oficial.esperado(r) for r in registros]
    sujas = [r["SEQ_TAD"] for r in registros if oficial.esperado(r)["data_embargo"] is None]
    aviso = (
        "ibama: 5 valor(es) de data_embargo viraram NaT (data ilegível ou com ano fora de "
        "1900–2099 ou de dia posterior a 2026-09-23)."
    )
    assert sorted(sujas) == ["1454499", "1534325", "1598392", "1875850", "674856"]
    assert meta.validation_warnings == [aviso]
    assert [str(w.message) for w in emitidos if "viraram NaT" in str(w.message)] == [aviso]
    assert [str(df[c].dtype) for c in ("data_embargo", "data_desembargo")] == [
        "datetime64[ns]",
        "datetime64[ns]",
    ]
    assert len(registros) == meta.records_count == 62
    assert meta.source_url == url
    helpers.conferir_corpo(meta, oficial.RECORTE.read_bytes())
    assert meta.source_details == {"ultima_atualizacao_relatorio": EDICAO}
    assert (meta.source, meta.source_method) == ("ibama", "httpx+csv")
    assert (meta.attempted_sources, meta.selected_source) == (["ibama_sifisc"], "ibama_sifisc")


async def test_recorte_vazio_mantem_esquema_crs_e_edicao(monkeypatch):
    oceano = (-30.0, -20.0, -29.9, -19.9)
    instalar(monkeypatch)

    with helpers.sem_excecao():
        df, meta = await ibama.embargos(bbox=oceano, return_meta=True)

    assert df.empty and list(df.columns) == [saida for saida, _, _ in oficial.COLUNAS]
    assert meta.source_details == {"ultima_atualizacao_relatorio": EDICAO}
    pytest.importorskip("geopandas")

    with helpers.sem_excecao():
        gdf, meta_geo = await ibama.embargos_geo(bbox=oceano, return_meta=True)

    assert gdf.empty and list(gdf.columns) == [
        *(saida for saida, _, _ in oficial.COLUNAS),
        "geometry",
    ]
    assert gdf.crs.to_epsg() == 4326
    assert meta_geo.source_details == {"ultima_atualizacao_relatorio": EDICAO}
    assert (meta_geo.source, meta_geo.source_method) == ("ibama", "httpx+csv+wkt")


async def test_as_polars_publica_os_mesmos_valores(monkeypatch):
    pl = pytest.importorskip("polars")
    registros = oficial.fonte()
    instalar(monkeypatch)

    with helpers.sem_excecao():
        frame = await ibama.embargos(uf="DF", as_polars=True)

    assert isinstance(frame, pl.DataFrame)
    esperado = [oficial.esperado(r) for r in registros if r["UF"] == "DF"]
    assert oficial.publicado(frame.to_pandas()) == esperado


async def test_filtro_uf_devolve_exatamente_os_termos_da_uf(monkeypatch):
    registros = oficial.fonte()
    instalar(monkeypatch)

    with helpers.collect_failures() as check:
        for uf in ("DF", "mt", "RS"):
            with check(uf):
                with helpers.sem_excecao():
                    df = await ibama.embargos(uf=uf)
                esperado = [oficial.esperado(r) for r in registros if r["UF"] == uf.upper()]
                assert esperado
                assert oficial.publicado(df) == esperado


async def test_bbox_tabular_filtra_pelo_ponto_de_referencia(monkeypatch):
    registros = oficial.fonte()
    instalar(monkeypatch)

    with helpers.sem_excecao():
        df = await ibama.embargos(bbox=oficial.BBOX_DOC)

    esperado = [
        oficial.esperado(r) for r in registros if oficial.ponto_no_bbox(r, oficial.BBOX_DOC)
    ]
    assert len(esperado) == 12
    assert oficial.publicado(df) == esperado


async def test_bbox_tabular_deixa_fora_o_ponto_zerado(monkeypatch):
    instalar(monkeypatch)
    em_volta_de_zero = (-1.0, -10.0, 1.0, 1.0)

    with helpers.sem_excecao():
        todos = await ibama.embargos()
        df = await ibama.embargos(bbox=em_volta_de_zero)

    zerados = todos[todos["latitude"].eq(0) & todos["longitude"].eq(0)]
    assert sorted(zerados["seq_tad"]) == ["1598392", "5097"]
    assert df["seq_tad"].tolist() == ["164438"]
    assert (df["longitude"].iloc[0], df["latitude"].iloc[0]) == (0.0, -9.179167)


async def test_bbox_geo_filtra_pela_intersecao_do_poligono(monkeypatch):
    shapely = pytest.importorskip("shapely")
    pytest.importorskip("geopandas")
    registros = oficial.fonte()
    instalar(monkeypatch)
    caixa = shapely.box(*oficial.BBOX_DOC)

    with helpers.sem_excecao():
        gdf, meta = await ibama.embargos_geo(bbox=oficial.BBOX_DOC, return_meta=True)

    helpers.conferir_corpo(meta, oficial.RECORTE.read_bytes())
    esperado = [
        r
        for r in registros
        if r["GEOM_AREA_EMBARGADA"]
        and (g := shapely.from_wkt(r["GEOM_AREA_EMBARGADA"], on_invalid="ignore")) is not None
        and shapely.intersects(g, caixa)
    ]
    assert len(esperado) == 6
    assert sum(not oficial.ponto_no_bbox(r, oficial.BBOX_DOC) for r in esperado) == 4
    assert oficial.publicado(gdf) == [oficial.esperado(r) for r in esperado]
    assert meta.source_details == {"ultima_atualizacao_relatorio": EDICAO}


async def test_geometria_confere_com_o_wkt_publicado(monkeypatch):
    shapely = pytest.importorskip("shapely")
    pytest.importorskip("geopandas")
    registros = oficial.fonte()
    instalar(monkeypatch)

    with helpers.sem_excecao():
        gdf = await ibama.embargos_geo()

    esperado = [
        r
        for r in registros
        if r["GEOM_AREA_EMBARGADA"]
        and shapely.from_wkt(r["GEOM_AREA_EMBARGADA"], on_invalid="ignore") is not None
    ]
    fonte = [shapely.from_wkt(r["GEOM_AREA_EMBARGADA"]) for r in esperado]
    assert len(esperado) == len(gdf) >= 20
    assert {g.geom_type for g in fonte} == {"Polygon", "MultiPolygon"}
    assert any(g.geom_type == "Polygon" and len(g.interiors) for g in fonte)
    assert not all(shapely.is_valid(fonte))
    assert gdf.crs.to_epsg() == 4326
    assert oficial.publicado(gdf) == [oficial.esperado(r) for r in esperado]
    for registro, geometria in zip(esperado, gdf.geometry, strict=True):
        assert geometria.geom_type.upper() == registro["GEOM_AREA_EMBARGADA"].split(" ", 1)[0]
        coordenadas = shapely.get_coordinates(geometria).ravel().tolist()
        assert coordenadas == oficial.numeros_do_wkt(registro["GEOM_AREA_EMBARGADA"])


async def test_geo_descarta_so_o_wkt_ilegivel(monkeypatch):
    shapely = pytest.importorskip("shapely")
    pytest.importorskip("geopandas")
    registros = oficial.fonte()
    instalar(monkeypatch)

    for uf, ilegiveis in (("PR", 1), ("MT", 0)):
        with helpers.capturar_logs() as logs, helpers.sem_excecao():
            gdf = await ibama.embargos_geo(uf=uf)

        com_wkt = [r for r in registros if r["UF"] == uf and r["GEOM_AREA_EMBARGADA"]]
        legiveis = [
            r
            for r in com_wkt
            if shapely.from_wkt(r["GEOM_AREA_EMBARGADA"], on_invalid="ignore") is not None
        ]
        assert len(com_wkt) - len(legiveis) == ilegiveis
        assert oficial.publicado(gdf) == [oficial.esperado(r) for r in legiveis]
        avisos = [e["descartados"] for e in logs if e["event"] == "ibama_embargos_geo_wkt_invalido"]
        assert avisos == ([ilegiveis] if ilegiveis else [])


async def test_geo_descarta_wkt_aninhado_sem_chamar_o_shapely(monkeypatch):
    shapely = pytest.importorskip("shapely")
    pytest.importorskip("geopandas")
    registros = [r for r in oficial.fonte() if r["UF"] == "MT" and r["GEOM_AREA_EMBARGADA"]]
    alvo = registros[0]["GEOM_AREA_EMBARGADA"].encode()
    aninhado = b"GEOMETRYCOLLECTION (" * 40 + b"POINT (1 1)" + b")" * 40
    corpo = oficial.RECORTE.read_bytes()
    assert corpo.count(alvo) == 1
    instalar(monkeypatch, corpo=corpo.replace(alvo, aninhado))
    lidos: list[object] = []
    from_wkt = shapely.from_wkt

    def registrar(wkts, **kwargs):
        lidos.extend(wkts)
        return from_wkt(wkts, **kwargs)

    monkeypatch.setattr(shapely, "from_wkt", registrar)
    with helpers.capturar_logs() as logs, helpers.sem_excecao():
        gdf = await ibama.embargos_geo(uf="MT")

    assert oficial.publicado(gdf) == [oficial.esperado(r) for r in registros[1:]]
    assert aninhado.decode() not in lidos
    avisos = [e["descartados"] for e in logs if e["event"] == "ibama_embargos_geo_wkt_invalido"]
    assert avisos == [1]


async def test_recusas_antes_da_rede(monkeypatch):
    pedidos = instalar(monkeypatch)

    with helpers.collect_failures() as check:
        for nome, chamada in (
            ("uf", lambda: ibama.embargos(uf="XX")),
            ("uf_geo", lambda: ibama.embargos_geo(uf="XX")),
            ("bbox_invertida", lambda: ibama.embargos(bbox=(-54.0, -14.0, -56.0, -16.0))),
            ("bbox_geo_incompleta", lambda: ibama.embargos_geo(bbox=(-56.0, -16.0, -54.0))),
        ):
            with check(nome), helpers.levanta_exatamente(ValueError):
                await chamada()
    assert pedidos == []


async def test_download_invalido_nao_vira_dado():
    with helpers.collect_failures() as check:
        for nome, corpo, content_type, erro in (
            ("html", b"<html><body>manutencao</body></html>" * 100, "text/html", "Assinatura"),
            ("truncado", oficial.RECORTE.read_bytes()[:900], "text/csv", "truncamento"),
        ):
            with check(nome), pytest.MonkeyPatch.context() as mp:
                instalar(mp, corpo=corpo, content_type=content_type)
                with helpers.levanta_exatamente(SourceUnavailableError, match=erro):
                    await ibama.embargos()


async def test_layout_sem_coluna_publicada_vira_parse_error(monkeypatch):
    corpo = oficial.RECORTE.read_bytes().replace(b'"QTD_AREA_EMBARGADA"', b'"QTD_AREA"', 1)
    instalar(monkeypatch, corpo=corpo)

    with helpers.levanta_exatamente(ParseError, match="QTD_AREA_EMBARGADA"):
        await ibama.embargos()


async def test_area_com_ponto_vira_parse_error(monkeypatch):
    original = oficial.RECORTE.read_bytes()
    assert original.count(b'"71,9994"') == 1
    instalar(monkeypatch, corpo=original.replace(b'"71,9994"', b'"71.9994"'))

    with helpers.levanta_exatamente(ParseError, match="QTD_AREA_EMBARGADA com ponto"):
        await ibama.embargos()


async def test_as_polars_mantem_texto_como_string_em_coluna_toda_nula(monkeypatch):
    pl = pytest.importorskip("polars")
    instalar(monkeypatch)
    texto = (
        "seq_tad",
        "numero_tad",
        "num_processo",
        "descricao",
        "codigo_municipio",
        "municipio",
        "uf",
        "nome_imovel",
        "status",
    )

    with helpers.collect_failures() as check:
        for uf in ("PA", "RJ", "SP", "RO"):
            with check(uf):
                frame = await ibama.embargos(uf=uf, as_polars=True)
                assert frame["nome_imovel"].null_count() == len(frame) > 0
                assert {coluna: frame.schema[coluna] for coluna in texto} == dict.fromkeys(
                    texto, pl.String
                )


async def test_parse_e_cache_rodam_fora_do_loop(monkeypatch):
    instalar(monkeypatch)
    threads: list[int] = []
    for modulo, nome in ((parser, "parse_embargos_csv"), (_cache, "_ler"), (_cache, "_gravar")):
        original = getattr(modulo, nome)
        monkeypatch.setattr(
            modulo,
            nome,
            lambda *a, _f=original, **k: threads.append(threading.get_ident()) or _f(*a, **k),
        )

    await ibama.embargos(uf="DF")

    assert len(threads) == 3
    assert threading.get_ident() not in threads


async def test_primeira_gravacao_do_cache_avisa_do_dado_pessoal(monkeypatch):
    instalar(monkeypatch)
    warn_once_reset("ibama_cache_pii")

    with warnings.catch_warnings(record=True) as sem_cache:
        warnings.simplefilter("always")
        await ibama.embargos(use_cache=False)
    with warnings.catch_warnings(record=True) as com_cache:
        warnings.simplefilter("always")
        await ibama.embargos()
        await ibama.embargos(use_cache=False)
        await ibama.embargos()

    def pessoais(capturados):
        return [str(w.message) for w in capturados if "CPF/CNPJ" in str(w.message)]

    assert pessoais(sem_cache) == []
    [aviso] = pessoais(com_cache)
    assert "cache" in aviso
    assert "use_cache=False" in aviso


def _edicoes_trocadas(*novas: bytes) -> bytes:
    marca = f'"{EDICAO}"\n'.encode()
    partes = oficial.RECORTE.read_bytes().rsplit(marca, len(novas))
    assert len(partes) == len(novas) + 1
    trocadas = (b'"' + nova + b'"\n' + parte for nova, parte in zip(novas, partes[1:]))
    return partes[0] + b"".join(trocadas)


async def test_edicao_e_a_maior_data_valida_da_coluna(monkeypatch):
    instalar(monkeypatch, corpo=_edicoes_trocadas(b"ND", b"2205-01-01 00:00:00"))

    with warnings.catch_warnings(record=True) as emitidos, helpers.sem_excecao():
        warnings.simplefilter("always")
        df, meta = await ibama.embargos(return_meta=True)

    assert oficial.publicado(df) == [oficial.esperado(r) for r in oficial.fonte()]
    assert meta.source_details == {"ultima_atualizacao_relatorio": EDICAO}
    assert not any("edição do arquivo" in str(w.message) for w in emitidos)


async def test_edicao_sem_data_valida_avisa_que_a_regra_nao_foi_aplicada(monkeypatch):
    instalar(monkeypatch, corpo=_edicoes_trocadas(*[b"ND"] * 62))

    with warnings.catch_warnings(record=True) as emitidos, helpers.sem_excecao():
        warnings.simplefilter("always")
        df, meta = await ibama.embargos(return_meta=True)

    aviso = (
        "ibama: a edição do arquivo (ULTIMA_ATUALIZACAO_RELATORIO) não foi lida, nenhum valor é "
        "data válida; a data de ato posterior à edição não foi anulada."
    )
    assert meta.validation_warnings[0] == aviso
    assert [str(w.message) for w in emitidos if "edição do arquivo" in str(w.message)] == [aviso]
    assert meta.source_details == {"ultima_atualizacao_relatorio": None}
    assert df["data_embargo"].dt.year.max() > 2026
