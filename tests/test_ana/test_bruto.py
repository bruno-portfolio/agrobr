from __future__ import annotations

import copy
import dataclasses
import gzip
import hashlib
import json
from pathlib import Path
from unittest.mock import Mock

import httpx
import pytest

from agrobr import bruto
from agrobr.ana import parser
from agrobr.bruto import registry
from agrobr.exceptions import InvalidParameterError, ParseError, ResourceLimitError
from tests.test_bruto.conftest import resposta

GOLDEN = Path(__file__).parents[1] / "golden_data/ana/bruto_massas_4674_20261003"
CHAVE = ("ana", "massas_dagua")
BBOX = (-47.466, -15.993, -47.462, -15.988)


class Fonte:
    def __init__(self):
        self.corpos = {
            nome: (GOLDEN / f"{nome}.json").read_bytes() for nome in ("contagem", "ids", "pagina")
        }
        self.depois = None
        self.contagens = 0
        self.pedidos = []
        self.paginas = []

    def responder(self, pedido):
        self.pedidos.append(pedido)
        parametros = pedido.url.params
        if "returnCountOnly" in parametros:
            self.contagens += 1
            corpo = (
                self.depois
                if self.contagens > 1 and self.depois is not None
                else self.corpos["contagem"]
            )
        elif "returnIdsOnly" in parametros:
            corpo = self.corpos["ids"]
        elif self.paginas:
            corpo = self.paginas.pop(0)
        else:
            corpo = self.corpos["pagina"]
        return resposta(200, corpo, {"Content-Type": "application/json", "ETag": 'W/"pagina"'})


@pytest.fixture
def fonte(monkeypatch):
    origem = Fonte()
    original = httpx.AsyncClient

    def cliente(**kwargs):
        return original(transport=httpx.MockTransport(origem.responder), **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", cliente)
    monkeypatch.setitem(
        registry.RECURSOS, CHAVE, dataclasses.replace(registry.RECURSOS[CHAVE], habilitado=True)
    )
    return origem


async def _coletar(tmp_path, **opcoes):
    parametros = {"nome": "barragem_df", "uf": "DF", "bbox": BBOX}
    return await bruto.coletar(*CHAVE, destino=tmp_path, **(parametros | opcoes))


def _corpo(tmp_path, artefato):
    corpo = (tmp_path / artefato.arquivo).read_bytes()
    return gzip.decompress(corpo) if artefato.compressao == "gzip" else corpo


@pytest.mark.parametrize("compactar", [False, True])
async def test_bruto_preserva_respostas_e_valores_publicados(
    fonte, tmp_path, monkeypatch, compactar
):
    monkeypatch.setattr(
        parser, "parse_massas_dagua", Mock(side_effect=AssertionError("parser tabular"))
    )
    coleta = await _coletar(tmp_path, compactar=compactar)
    entrada = coleta.entrada
    esperado = json.loads((GOLDEN / "expected.json").read_text("utf-8"))
    assert entrada.status == "ok"
    assert entrada.cobertura.completa
    assert entrada.cobertura.total_antes == entrada.cobertura.total_depois == esperado["count"]
    assert entrada.feicoes == entrada.cobertura.ids_distintos == esperado["count"]
    assert entrada.cobertura.ids_repetidos == 0
    assert entrada.crs == "EPSG:4674"
    pagina = entrada.paginas[0]
    corpo = _corpo(tmp_path, pagina)
    assert corpo == fonte.corpos["pagina"]
    assert pagina.sha256 == hashlib.sha256(corpo).hexdigest()
    assert pagina.bytes == len(corpo)
    assert pagina.cabecalhos["etag"] == 'W/"pagina"'
    assert entrada.crs_evidencia.localizador == "/spatialReference/wkid"
    dado = json.loads(corpo)
    assert dado["spatialReference"]["wkid"] == esperado["wkid"]
    atributos = dado["features"][0]["attributes"]
    assert {campo: atributos[campo] for campo in esperado["attributes"]} == esperado["attributes"]
    anel = dado["features"][0]["geometry"]["rings"][0]
    assert anel[0] == esperado["primeiro_vertice"]
    assert len(anel) == esperado["vertices_primeiro_anel"]
    assert [c.papel for c in entrada.controles] == ["contagem_antes", "ids", "contagem_depois"]
    assert [_corpo(tmp_path, c) for c in entrada.controles] == [
        fonte.corpos["contagem"],
        fonte.corpos["ids"],
        fonte.corpos["contagem"],
    ]
    assert json.loads(_corpo(tmp_path, entrada.controles[1]))["objectIds"] == esperado["ids"]
    metadata = json.loads((GOLDEN / "metadata.json").read_text("utf-8"))
    assert [dict(p.url.params) for p in fonte.pedidos] == [
        p["parametros"] for p in metadata["requests"]
    ]
    assert pagina.paginacao.model_dump() == {
        "tipo": "fid",
        "min": 114424,
        "max": 114424,
        "quantidade": 1,
    }
    antes = coleta.manifesto.read_bytes()
    fonte.pedidos.clear()
    repetida = await _coletar(tmp_path, compactar=compactar, retomar=True)
    assert repetida.reutilizado and not fonte.pedidos
    assert repetida.manifesto.read_bytes() == antes


@pytest.mark.parametrize("bbox_crs", ["EPSG:4326", "EPSG:4674"])
@pytest.mark.parametrize("uf,bbox", [("DF", BBOX), (None, BBOX), ("DF", None)])
async def test_mesmo_filtro_e_crs_em_controles_e_pagina(fonte, tmp_path, uf, bbox, bbox_crs):
    if bbox is None and bbox_crs != "EPSG:4674":
        with pytest.raises(InvalidParameterError, match="bbox_crs sem bbox"):
            await _coletar(tmp_path, uf=uf, bbox=bbox, bbox_crs=bbox_crs)
        assert not fonte.pedidos
        return
    entrada = (await _coletar(tmp_path, uf=uf, bbox=bbox, bbox_crs=bbox_crs)).entrada
    consulta = entrada.parametros
    assert entrada.selecao.bbox_crs == (bbox_crs if bbox is not None else None)
    for pedido in fonte.pedidos:
        params = dict(pedido.url.params)
        assert params["outSR"] == "4674"
        assert params["outFields"] == "*"
        assert params["returnGeometry"] == "true"
        assert not {"resultOffset", "resultRecordCount", "orderByFields"} & params.keys()
        for chave in ("geometry", "geometryType", "inSR", "spatialRel"):
            assert params.get(chave) == consulta.get(chave)
        if bbox is not None:
            assert params["inSR"] == bbox_crs.split(":")[1]
        if "returnCountOnly" in params or "returnIdsOnly" in params:
            assert params["where"] == consulta["where"]
        else:
            assert params["where"].startswith(f"({consulta['where']}) AND FID >= ")


@pytest.mark.parametrize("ids", [[], None])
async def test_zero_confirma_contagens_e_lista_oficial_sem_paginas(fonte, tmp_path, ids):
    fonte.corpos["contagem"] = b'{"count":0}'
    fonte.corpos["ids"] = json.dumps({"objectIdFieldName": "FID", "objectIds": ids}).encode()
    entrada = (await _coletar(tmp_path)).entrada
    assert entrada.status == "ok" and entrada.cobertura.completa
    assert entrada.feicoes == entrada.cobertura.ids_distintos == 0
    assert entrada.paginas == [] and entrada.crs is None
    assert len(fonte.pedidos) == len(entrada.controles) == 3


@pytest.mark.parametrize(
    ("defeito", "estado"),
    [
        ("fid_ausente", "nao_comprovada"),
        ("fid_duplicado", "divergente"),
        ("fid_fora_faixa", "divergente"),
        ("fid_booleano", "nao_comprovada"),
        ("pagina_vazia", "divergente"),
        ("ids_duplicados", "divergente"),
        ("ids_nulos", "divergente"),
        ("ids_ausentes", "nao_comprovada"),
        ("nome_id", "nao_comprovada"),
        ("count_antes", "divergente"),
        ("count_depois", "divergente"),
        ("crs", "divergente"),
        ("latest_wkid", "divergente"),
        ("geometria_ausente", "nao_comprovada"),
        ("limite_transferencia", "nao_comprovada"),
        ("limite_transferencia_com_faixa_incompleta", "nao_comprovada"),
        ("total_pagina", "divergente"),
    ],
)
async def test_divergencia_impede_sucesso(fonte, tmp_path, defeito, estado):
    pagina = json.loads(fonte.corpos["pagina"])
    ids = json.loads(fonte.corpos["ids"])
    feicao = pagina["features"][0]
    if defeito == "fid_ausente":
        del feicao["attributes"]["FID"]
    elif defeito == "fid_duplicado":
        pagina["features"].append(copy.deepcopy(feicao))
    elif defeito == "fid_fora_faixa":
        feicao["attributes"]["FID"] += 1
    elif defeito == "fid_booleano":
        feicao["attributes"]["FID"] = True
    elif defeito == "pagina_vazia":
        pagina["features"] = []
    elif defeito == "ids_duplicados":
        ids["objectIds"] *= 2
    elif defeito == "ids_nulos":
        ids["objectIds"] = None
    elif defeito == "ids_ausentes":
        del ids["objectIds"]
    elif defeito == "nome_id":
        ids["objectIdFieldName"] = "gid"
    elif defeito == "count_antes":
        fonte.corpos["contagem"] = b'{"count":2}'
    elif defeito == "count_depois":
        fonte.depois = b'{"count":2}'
    elif defeito == "crs":
        pagina["spatialReference"]["wkid"] = 4326
    elif defeito == "latest_wkid":
        pagina["spatialReference"]["latestWkid"] = 4326
    elif defeito == "geometria_ausente":
        del feicao["geometry"]
    elif defeito in ("limite_transferencia", "limite_transferencia_com_faixa_incompleta"):
        pagina["exceededTransferLimit"] = True
        if defeito == "limite_transferencia_com_faixa_incompleta":
            pagina["features"] = []
    elif defeito == "total_pagina":
        pagina["count"] = 2
    fonte.corpos["pagina"] = json.dumps(pagina).encode()
    fonte.corpos["ids"] = json.dumps(ids).encode()
    with pytest.raises(ParseError):
        await _coletar(tmp_path)
    entrada = json.loads((tmp_path / "manifesto.jsonl").read_text("utf-8"))
    assert entrada["status"] == "erro"
    assert entrada["cobertura"]["estado"] == estado
    assert not entrada["cobertura"]["completa"]
    assert entrada["erro"]["tipo"] == "ParseError"


@pytest.mark.parametrize("papel", ["contagem", "ids", "pagina"])
async def test_erro_arcgis_em_http_200_vira_parse_error(fonte, tmp_path, papel):
    fonte.corpos[papel] = b'{"error":{"code":400,"message":"Consulta invalida"}}'
    with pytest.raises(ParseError, match="ArcGIS de erro"):
        await _coletar(tmp_path)
    entrada = json.loads((tmp_path / "manifesto.jsonl").read_text("utf-8"))
    assert entrada["status"] == "erro"
    assert entrada["cobertura"]["estado"] == "nao_comprovada"
    assert not entrada["cobertura"]["completa"]


async def test_limite_nao_trunca_ids_nem_inicia_paginas(fonte, tmp_path):
    fonte.corpos["contagem"] = b'{"count":2}'
    with pytest.raises(ResourceLimitError):
        await _coletar(tmp_path, limites=bruto.LimitesBrutos(max_ids=1))
    assert len(fonte.pedidos) == 1


async def test_selecao_invalida_recusada_antes_da_rede(fonte, tmp_path):
    with pytest.raises(InvalidParameterError):
        await _coletar(tmp_path, uf=None, bbox=None)
    assert not fonte.pedidos
    assert not (tmp_path / "manifesto.jsonl").exists()


async def test_faixas_esparsas_preservam_ordem_original_dos_bytes(fonte, tmp_path):
    pagina = json.loads(fonte.corpos["pagina"])
    feicao = pagina["features"][0]
    fonte.corpos["contagem"] = b'{"count":3}'
    fonte.corpos["ids"] = b'{"objectIdFieldName":"FID","objectIds":[114424,10,27]}'
    partes = []
    for faixa in ([27, 10], [114424]):
        corpo = copy.deepcopy(pagina)
        corpo["features"] = []
        for fid in faixa:
            registro = copy.deepcopy(feicao)
            registro["attributes"]["FID"] = fid
            corpo["features"].append(registro)
        partes.append(json.dumps(corpo, indent=1).encode())
    fonte.paginas = list(partes)
    entrada = (await _coletar(tmp_path, tamanho_pagina=2)).entrada
    assert entrada.status == "ok" and entrada.feicoes == 3
    assert [p.paginacao.model_dump() for p in entrada.paginas] == [
        {"tipo": "fid", "min": 10, "max": 27, "quantidade": 2},
        {"tipo": "fid", "min": 114424, "max": 114424, "quantidade": 1},
    ]
    assert [_corpo(tmp_path, p) for p in entrada.paginas] == partes
    assert [f["attributes"]["FID"] for f in json.loads(partes[0])["features"]] == [27, 10]
