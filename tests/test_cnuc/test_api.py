from __future__ import annotations

import sys

import pandas as pd
import pytest

from agrobr import cnuc
from agrobr.cnuc import api, parser
from agrobr.exceptions import InvalidParameterError, ParseError, ResourceLimitError
from tests.helpers import levanta_exatamente
from tests.test_cnuc.golden import GEO, TABULAR, hits, instalar, primeiras

CAPELA = b"<ms:municipio>CAPELA (SE)</ms:municipio>"
CORTADA = b"<ms:municipio>CAPELA (SE), DIVINA PASTORA (SE), ROS\xc3\x81RIO DO CA...</ms:municipio>"
GARARU = b"<ms:municipio>GARARU (SE)</ms:municipio>"
GRAFIA = b"<ms:municipio>GARARU-X (SE)</ms:municipio>"
DIVINA = b"<ms:municipio>DIVINA PASTORA (SE)</ms:municipio>"
DIVINA_ARACAJU = b"<ms:municipio>DIVINA PASTORA (SE), ARACAJU (SE)...</ms:municipio>"


@pytest.mark.parametrize(
    ("kwargs", "mensagem"),
    [
        ({"uf": "XX"}, "UF inválida"),
        ({"municipio": "Atlantida"}, "Município"),
        ({"municipio": "Aracaju", "uf": "MT"}, "Município"),
        ({"esfera": "distrital"}, "Esfera inválida"),
        ({"categoria": "Bosque"}, "Categoria inválida"),
        ({"grupo": "ZZ"}, "Grupo inválido"),
        ({"grupo": 1}, "Grupo inválido"),
        ({"bioma": "Marte"}, "Bioma inválido"),
        ({"bbox": (1, 2, 0, 3)}, "BBOX"),
        ({"max_registros": 0}, "max_registros"),
        ({"max_registros": True}, "max_registros"),
        ({"as_polars": 1}, "booleanos"),
        ({"return_meta": "sim"}, "booleanos"),
    ],
    ids=lambda valor: str(valor),
)
async def test_parametro_invalido_antes_da_rede(monkeypatch, kwargs, mensagem):
    fetch_count, fetch_ucs = instalar(monkeypatch)
    with levanta_exatamente(InvalidParameterError, mensagem):
        await cnuc.ucs(**kwargs)
    fetch_count.assert_not_awaited()
    fetch_ucs.assert_not_awaited()


async def test_ucs_geo_valida_antes_da_rede(monkeypatch):
    fetch_count, _ = instalar(monkeypatch)
    with levanta_exatamente(InvalidParameterError, "Esfera inválida"):
        await cnuc.ucs_geo(esfera="privada")
    with levanta_exatamente(InvalidParameterError, "return_meta"):
        await cnuc.ucs_geo(return_meta=1)
    fetch_count.assert_not_awaited()


async def test_argumento_desconhecido():
    with levanta_exatamente(TypeError, "unexpected keyword argument 'estado'"):
        await cnuc.ucs(estado="SE")


async def test_parametros_normalizados_chegam_ao_servidor(monkeypatch):
    fetch_count, fetch_ucs = instalar(monkeypatch)
    await cnuc.ucs(
        uf="se",
        esfera="MUNICIPAL",
        categoria="reserva particular do patrimonio natural",
        grupo="us",
        bbox=(-38.5, -11.6, -36.3, -9.3),
    )
    filtro = fetch_count.await_args.args[0]
    assert filtro == api.client.FiltroServidor(
        uf="SE",
        esfera="municipal",
        categoria="Reserva Particular do Patrimônio Natural",
        grupo="US",
        bbox=(-38.5, -11.6, -36.3, -9.3),
    )
    assert fetch_ucs.await_args.args[0] == filtro


async def test_ucs_de_se_com_meta(monkeypatch):
    instalar(monkeypatch)
    frame, meta = await cnuc.ucs(uf="SE", return_meta=True)
    assert len(frame) == 19
    assert meta.source == "cnuc"
    assert meta.selected_source == "cnuc_wfs"
    assert meta.records_count == 19
    assert meta.validation_warnings == []
    assert meta.source_details["coverage"] == {
        "expected": 19,
        "downloaded": 19,
        "returned": 19,
        "status": "count_reconciled",
        "limit": 10_000,
        "truncated": False,
        "transactional_snapshot": False,
    }
    assert meta.source_details["query"]["uf"] == "SE"


async def test_uf_casa_sigla_exata_na_lista(monkeypatch):
    instalar(monkeypatch)
    frame = await cnuc.ucs(uf="AL")
    assert frame["codigo"].tolist() == ["0000.00.1812"]
    assert frame["uf"].tolist() == ["AL/BA/SE"]


async def test_bioma_filtra_pela_lista_derivada(monkeypatch):
    instalar(monkeypatch)
    frame = await cnuc.ucs(bioma="caatinga")
    assert frame["codigo"].tolist() == [
        "0000.00.0147",
        "0000.00.1812",
        "0000.00.3091",
        "0000.00.3092",
        "0000.28.1603",
        "0000.28.5574",
    ]


async def test_municipio_casa_nome_inteiro_e_avisa_lista_cortada_e_grafia(monkeypatch):
    corpo = (
        TABULAR.replace(CAPELA, CORTADA, 1)
        .replace(GARARU, GRAFIA, 1)
        .replace(DIVINA, DIVINA_ARACAJU, 1)
    )
    _, fetch_ucs = instalar(monkeypatch, tabular=corpo)
    with pytest.warns(UserWarning) as avisos:
        frame, meta = await cnuc.ucs(municipio="aracaju", return_meta=True)
    assert frame["codigo"].tolist() == ["0030.28.3459", "2007.28.5583"]
    assert fetch_ucs.await_args.args[0].uf == "SE"
    assert meta.source_details["query"]["municipio"] == 2800308
    assert meta.validation_warnings == [
        "cnuc: 1 UC(s) de SE com a lista de municípios cortada pela fonte, que pode incluir "
        "Aracaju, ficaram fora: 0000.28.1604",
        "cnuc: grafia de município fora do cadastro do IBGE, não comparada com Aracaju: "
        "GARARU-X (SE) em 0000.28.5574",
    ]
    assert [str(aviso.message) for aviso in avisos] == meta.validation_warnings


async def test_max_registros_trunca_por_codigo(monkeypatch):
    _, fetch_ucs = instalar(monkeypatch, contagem=hits(19))
    frame, meta = await cnuc.ucs(esfera="federal", max_registros=19, return_meta=True)
    assert fetch_ucs.await_args.kwargs["count"] == 19
    assert len(frame) == 19
    assert meta.source_details["coverage"]["truncated"] is False
    _, fetch_ucs = instalar(monkeypatch)
    frame, meta = await cnuc.ucs(uf="SE", max_registros=4, return_meta=True)
    assert fetch_ucs.await_args.kwargs["count"] is None
    assert frame["codigo"].tolist() == [
        "0000.00.0147",
        "0000.00.0199",
        "0000.00.0269",
        "0000.00.1812",
    ]
    assert meta.source_details["coverage"]["truncated"] is True


async def test_limite_tabular_antes_do_download(monkeypatch):
    _, fetch_ucs = instalar(monkeypatch, contagem=hits(10_001))
    with levanta_exatamente(ResourceLimitError, "excede o limite de 10000"):
        await cnuc.ucs()
    fetch_ucs.assert_not_awaited()


async def test_contagem_divergente(monkeypatch):
    instalar(monkeypatch, contagem=hits(20))
    with levanta_exatamente(ParseError, "Contagem divergente"):
        await cnuc.ucs(uf="SE")


async def test_selecao_vazia_nao_baixa_feicoes(monkeypatch):
    _, fetch_ucs = instalar(monkeypatch, contagem=hits(0))
    frame, meta = await cnuc.ucs(esfera="municipal", return_meta=True)
    fetch_ucs.assert_not_awaited()
    assert frame.empty
    assert frame.dtypes.equals(parser.parse_ucs(TABULAR).dtypes)
    assert meta.source_url == "https://test/hits"


async def test_as_polars_sem_polars(monkeypatch):
    monkeypatch.setitem(sys.modules, "polars", None)
    with levanta_exatamente(ImportError, r"Instale agrobr\[polars\]"):
        await cnuc.ucs(as_polars=True)


async def test_as_polars(monkeypatch):
    pl = pytest.importorskip("polars")
    instalar(monkeypatch)
    frame = await cnuc.ucs(as_polars=True)
    assert frame.schema["area_ha"] == pl.Float64
    assert frame.schema["data_criacao"] == pl.Datetime("ns")
    assert frame.schema["codigo"] == pl.Utf8
    assert frame.height == 19
    vazio = api._to_polars(parser.empty_frame())
    assert vazio.schema == frame.schema


async def test_ucs_geo_de_se(monkeypatch):
    gpd = pytest.importorskip("geopandas")
    instalar(monkeypatch)
    gdf, meta = await cnuc.ucs_geo(uf="SE", return_meta=True)
    assert isinstance(gdf, gpd.GeoDataFrame)
    assert gdf.crs.to_epsg() == 4326
    assert len(gdf) == 19
    assert meta.selected_source == "cnuc_wfs_geo"
    assert meta.source_details["coverage"]["limit"] == 600


async def test_ucs_geo_limite_antes_do_download(monkeypatch):
    pytest.importorskip("geopandas")
    _, fetch_ucs = instalar(monkeypatch, contagem=hits(601))
    with levanta_exatamente(ResourceLimitError, "601 UCs excede o limite de 600 com geometria"):
        await cnuc.ucs_geo(esfera="estadual")
    fetch_ucs.assert_not_awaited()


@pytest.mark.parametrize(
    ("kwargs", "dica"),
    [
        ({"esfera": "estadual"}, "reduza com uf, esfera, categoria, grupo, bbox ou max_registros"),
        ({"uf": "SE", "max_registros": 1}, "reduza com uf, esfera, categoria, grupo ou bbox"),
        (
            {"bioma": "caatinga", "max_registros": 1},
            "reduza com uf, esfera, categoria, grupo ou bbox",
        ),
    ],
    ids=["sem_filtro_local", "uf", "bioma"],
)
async def test_dica_do_limite_so_sugere_o_que_reduz_o_download(monkeypatch, kwargs, dica):
    pytest.importorskip("geopandas")
    _, fetch_ucs = instalar(monkeypatch, contagem=hits(601))
    with levanta_exatamente(ResourceLimitError) as erro:
        await cnuc.ucs_geo(**kwargs)
    assert erro.value.reason == f"Seleção de 601 UCs excede o limite de 600 com geometria; {dica}"
    fetch_ucs.assert_not_awaited()


async def test_ucs_geo_vazio(monkeypatch):
    gpd = pytest.importorskip("geopandas")
    instalar(monkeypatch, contagem=hits(0))
    gdf = await cnuc.ucs_geo(uf="SE")
    assert isinstance(gdf, gpd.GeoDataFrame)
    assert gdf.crs.to_epsg() == 4326
    assert gdf.columns.tolist() == parser.COLUNAS_SAIDA_GEO
    assert pd.DataFrame(gdf.drop(columns="geometry")).dtypes.equals(parser.empty_frame().dtypes)


async def test_ucs_geo_max_registros_reduz_o_download_no_servidor(monkeypatch):
    pytest.importorskip("geopandas")
    _, fetch_ucs = instalar(monkeypatch, contagem=hits(3450), geo=primeiras(GEO, 5))
    gdf, meta = await cnuc.ucs_geo(max_registros=5, return_meta=True)
    assert fetch_ucs.await_args.kwargs == {"geo": True, "count": 5}
    assert len(gdf) == 5
    assert meta.source_details["coverage"]["expected"] == 3450
    assert meta.source_details["coverage"]["downloaded"] == 5
    assert meta.source_details["coverage"]["truncated"] is True
