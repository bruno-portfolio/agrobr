from __future__ import annotations

import gzip
import hashlib
import json
import re

import httpx
import pydantic
import pytest

from agrobr import bruto
from agrobr.bruto import models, registry, validation
from agrobr.exceptions import (
    InvalidParameterError,
    ParseError,
    ResourceLimitError,
    SourceUnavailableError,
)
from agrobr.sfb import bruto as sfb_bruto
from tests.test_bruto.conftest import manifesto, resposta

CHAVE = ("sfb", "cnfp")
CAMADA = "Hosted/CNFP_v19_03_retificado_17072025/FeatureServer/9"
URL = f"https://mapas.florestal.gov.br/server/rest/services/{CAMADA}/query"
BASE = {"where": "1=1", "outFields": "*", "returnGeometry": "true", "f": "json"}
FAIXA = re.compile(r"^\(1=1\) AND fid >= (\d+) AND fid <= (\d+)$")
REFERENCIA = {"wkid": 102100, "latestWkid": 3857}


def _feicao(fid):
    anel = [[-6.0e6, -3.0e5], [-6.0e6, -3.1e5], [-6.1e6, -3.1e5], [-6.0e6, -3.0e5]]
    return {
        "attributes": {"fid": fid, "nome": "FLORESTA SINTÉTICA", "uf": "AM"},
        "geometry": {"rings": [anel]},
    }


class Arcgis:
    def __init__(self):
        self.fids = [1, 2, 3, 4, 5]
        self.contagens = []
        self.ids = None
        self.paginas = []
        self.status = {}
        self.pedidos = []

    def corpo_pagina(self, inicio, fim):
        envelope = {
            "objectIdFieldName": "fid",
            "geometryType": "esriGeometryPolygon",
            "spatialReference": REFERENCIA,
            "fields": [{"name": "fid", "type": "esriFieldTypeOID"}],
            "features": [_feicao(fid) for fid in self.fids if inicio <= fid <= fim],
        }
        return json.dumps(envelope, ensure_ascii=False, indent=1).encode("utf-8")

    def responder(self, pedido):
        self.pedidos.append(dict(pedido.url.params))
        parametros = pedido.url.params
        papel = (
            "contagem"
            if "returnCountOnly" in parametros
            else "ids"
            if "returnIdsOnly" in parametros
            else "pagina"
        )
        if papel in self.status:
            return resposta(self.status[papel], b"<html>fora</html>")
        if papel == "contagem":
            corpo = self.contagens.pop(0) if self.contagens else None
            corpo = corpo or json.dumps({"count": len(self.fids)}).encode()
        elif papel == "ids":
            corpo = (
                self.ids
                or json.dumps({"objectIdFieldName": "fid", "objectIds": self.fids[::-1]}).encode()
            )
        elif self.paginas:
            corpo = self.paginas.pop(0)
        else:
            inicio, fim = map(int, FAIXA.match(parametros["where"]).groups())
            corpo = self.corpo_pagina(inicio, fim)
        return resposta(200, corpo, {"Content-Type": "application/json", "ETag": '"1776450889740"'})


@pytest.fixture
def arcgis(monkeypatch):
    servidor = Arcgis()
    original = httpx.AsyncClient

    def cliente(**kwargs):
        return original(transport=httpx.MockTransport(servidor.responder), **kwargs)

    monkeypatch.setattr(sfb_bruto.httpx, "AsyncClient", cliente)
    monkeypatch.setattr(sfb_bruto, "_PAUSA_SEGUNDOS", 0)
    return servidor


def _corpo(tmp_path, artefato):
    corpo = (tmp_path / artefato.arquivo).read_bytes()
    return gzip.decompress(corpo) if artefato.compressao == "gzip" else corpo


def _erro(tmp_path):
    (registro,) = manifesto(tmp_path)
    return registro["status"], registro["cobertura"]["estado"], registro["erro"]["tipo"]


@pytest.mark.parametrize("compactar", [True, False])
async def test_cnfp_guarda_as_paginas_como_vieram_no_crs_nativo(arcgis, tmp_path, compactar):
    entrada = (
        await bruto.coletar(*CHAVE, destino=tmp_path, tamanho_pagina=2, compactar=compactar)
    ).entrada

    assert (entrada.status, entrada.nome, entrada.url_solicitada, entrada.url) == (
        "ok",
        "brasil",
        URL,
        URL,
    )
    assert entrada.parametros == BASE
    assert entrada.selecao == models.Selecao(
        uf=None, bbox=None, bbox_crs=None, camada=CAMADA, edicao=20250717, natureza=None
    )
    assert (entrada.crs, entrada.crs_evidencia.localizador, entrada.crs_evidencia.valor) == (
        "EPSG:3857",
        "/spatialReference/latestWkid",
        "3857",
    )
    cobertura = entrada.cobertura
    assert (cobertura.estado, cobertura.total_antes, cobertura.total_depois) == ("conferida", 5, 5)
    assert (entrada.feicoes, cobertura.ids_distintos, cobertura.campo_id) == (5, 5, "fid")
    assert [c.papel for c in entrada.controles] == ["contagem_antes", "ids", "contagem_depois"]
    assert [(p.paginacao.min, p.paginacao.max) for p in entrada.paginas] == [(1, 2), (3, 4), (5, 5)]
    for pagina, (inicio, fim) in zip(entrada.paginas, [(1, 2), (3, 4), (5, 5)], strict=True):
        corpo = _corpo(tmp_path, pagina)
        assert corpo == arcgis.corpo_pagina(inicio, fim)
        assert (pagina.sha256, pagina.bytes) == (hashlib.sha256(corpo).hexdigest(), len(corpo))
        assert (pagina.crs, pagina.cabecalhos["etag"]) == ("EPSG:3857", '"1776450889740"')
    assert [p.get("orderByFields") for p in arcgis.pedidos] == [
        None,
        None,
        "fid",
        "fid",
        "fid",
        None,
    ]
    assert all("outSR" not in p for p in arcgis.pedidos)
    assert [p["where"] for p in arcgis.pedidos[2:5]] == [
        "(1=1) AND fid >= 1 AND fid <= 2",
        "(1=1) AND fid >= 3 AND fid <= 4",
        "(1=1) AND fid >= 5 AND fid <= 5",
    ]
    (linha,) = (tmp_path / "manifesto.jsonl").read_text("utf-8").splitlines()
    assert models.RecursoBruto.model_validate_json(linha).linha() == linha


async def test_zero_feicoes_fecha_sem_paginas(arcgis, tmp_path):
    arcgis.fids = []

    entrada = (await bruto.coletar(*CHAVE, destino=tmp_path)).entrada

    assert (entrada.status, entrada.feicoes, entrada.paginas, entrada.crs) == ("ok", 0, [], None)
    assert len(arcgis.pedidos) == 3


@pytest.mark.parametrize(
    ("defeito", "estado"),
    [
        ("ordem_quebrada", "divergente"),
        ("fid_fora_da_faixa", "divergente"),
        ("feicao_faltando", "divergente"),
        ("contagem_depois", "divergente"),
        ("ids_menos_que_contagem", "divergente"),
        ("ids_repetidos", "divergente"),
        ("ids_truncados", "nao_comprovada"),
        ("crs_4674", "divergente"),
        ("crs_sem_latest", "divergente"),
        ("crs_wkid", "divergente"),
        ("limite_transferencia", "nao_comprovada"),
        ("html", "nao_comprovada"),
        ("erro_arcgis", "nao_comprovada"),
    ],
)
async def test_divergencia_ou_corpo_fora_do_formato_nao_fecha_ok(arcgis, tmp_path, defeito, estado):
    primeira = json.loads(arcgis.corpo_pagina(1, 2))
    if defeito == "ordem_quebrada":
        primeira["features"].reverse()
    elif defeito == "fid_fora_da_faixa":
        primeira["features"][1]["attributes"]["fid"] = 3
    elif defeito == "feicao_faltando":
        del primeira["features"][1]
    elif defeito == "contagem_depois":
        arcgis.contagens = [None, b'{"count":6}']
    elif defeito == "ids_menos_que_contagem":
        arcgis.ids = b'{"objectIdFieldName":"fid","objectIds":[1,2,3,4]}'
    elif defeito == "ids_repetidos":
        arcgis.ids = b'{"objectIdFieldName":"fid","objectIds":[1,2,3,4,4]}'
    elif defeito == "ids_truncados":
        arcgis.ids = (
            b'{"objectIdFieldName":"fid","objectIds":[1,2,3,4,5],"exceededTransferLimit":true}'
        )
    elif defeito == "crs_4674":
        primeira["spatialReference"] = {"wkid": 4674, "latestWkid": 4674}
    elif defeito == "crs_sem_latest":
        primeira["spatialReference"] = {"wkid": 102100}
    elif defeito == "crs_wkid":
        primeira["spatialReference"] = {"wkid": 4326, "latestWkid": 3857}
    elif defeito == "limite_transferencia":
        primeira["exceededTransferLimit"] = True
    arcgis.paginas = [json.dumps(primeira).encode()]
    if defeito == "html":
        arcgis.paginas = [b"<html>manutencao</html>"]
    elif defeito == "erro_arcgis":
        arcgis.paginas = [b'{"error":{"code":400,"message":"Consulta invalida"}}']

    with pytest.raises(ParseError):
        await bruto.coletar(*CHAVE, destino=tmp_path, tamanho_pagina=2)

    assert _erro(tmp_path) == ("erro", estado, "ParseError")
    paginas_guardadas = len(list(tmp_path.rglob("p*.esri.json.gz")))
    assert paginas_guardadas == (3 if defeito == "contagem_depois" else 0)


async def test_pagina_acima_do_teto_para_a_coleta(arcgis, tmp_path):
    teto = len(arcgis.corpo_pagina(1, 2)) - 1
    assert len(arcgis.corpo_pagina(1, 1)) < teto

    mensagem = rf"página 1 \(fid 1–2, max_bytes_pagina={teto}\): .*teto de {teto} bytes"
    with pytest.raises(ResourceLimitError, match=mensagem):
        await bruto.coletar(
            *CHAVE,
            destino=tmp_path,
            tamanho_pagina=2,
            limites=bruto.LimitesBrutos(max_bytes_pagina=teto),
        )

    assert _erro(tmp_path) == ("erro", "nao_comprovada", "ResourceLimitError")
    (registro,) = manifesto(tmp_path)
    assert re.search(mensagem, registro["erro"]["mensagem"])
    assert "fid+%3E%3D+1+AND+fid+%3C%3D+2" in registro["erro"]["url"]
    entrada = (
        await bruto.coletar(
            *CHAVE,
            destino=tmp_path,
            nome="pagina_1",
            tamanho_pagina=1,
            limites=bruto.LimitesBrutos(max_bytes_pagina=teto),
        )
    ).entrada
    assert (entrada.status, len(entrada.paginas)) == ("ok", 5)


@pytest.mark.parametrize("papel", ["contagem", "ids", "pagina"])
async def test_404_no_paginado_e_fonte_indisponivel(arcgis, tmp_path, papel):
    arcgis.status[papel] = 404

    with pytest.raises(SourceUnavailableError):
        await bruto.coletar(*CHAVE, destino=tmp_path)

    (registro,) = manifesto(tmp_path)
    assert (registro["status"], registro["erro"]["http_status"]) == ("erro", 404)
    assert registro["erro"]["tipo"] == "SourceUnavailableError"


@pytest.mark.parametrize(
    ("argumentos", "trecho"),
    [
        ({"uf": "AM"}, "não aceita uf"),
        ({"bbox": (-60.0, -4.0, -59.0, -3.0), "nome": "x"}, "não aceita bbox"),
    ],
)
async def test_recorte_recusado_antes_da_rede(arcgis, tmp_path, argumentos, trecho):
    with pytest.raises(InvalidParameterError, match=trecho):
        await bruto.coletar(*CHAVE, destino=tmp_path, **argumentos)

    assert arcgis.pedidos == []


def test_planejar_nao_toca_a_rede_e_repete_o_pedido(arcgis):
    registrado = registry.recurso(*CHAVE)
    pedido = validation.pedido(
        registrado,
        nome=None,
        uf=None,
        bbox=None,
        bbox_crs="EPSG:4674",
        tamanho_pagina=None,
        compactar=True,
        retomar=False,
        limites=None,
    )

    plano = registry.adaptador(registrado).planejar(pedido)
    validation.conferir_plano(plano, pedido, registrado)

    assert (plano.url_solicitada, plano.parametros, plano.crs_esperado) == (URL, BASE, "EPSG:3857")
    assert arcgis.pedidos == []


def test_edicao_e_a_data_de_retificacao_do_nome_do_servico():
    dia, mes, ano = re.search(r"_retificado_(\d{2})(\d{2})(\d{4})/", sfb_bruto.CAMADA).groups()

    assert int(f"{ano}{mes}{dia}") == sfb_bruto.EDICAO


def _como(registro, fonte, recurso):
    antigo = f"{registro['fonte']}/{registro['recurso']}/"
    texto = json.dumps(registro).replace(antigo, f"{fonte}/{recurso}/")
    novo = {**json.loads(texto), "fonte": fonte, "recurso": recurso}
    novo["consulta_id"] = models.consulta_id(
        fonte=fonte,
        recurso=recurso,
        nome=novo["nome"],
        url_solicitada=novo["url_solicitada"],
        parametros=novo["parametros"],
        selecao=models.Selecao(**novo["selecao"]),
        modo=novo["modo"],
        formato=novo["formato"],
        tamanho_pagina=novo["opcoes"]["tamanho_pagina"],
        compactar=novo["opcoes"]["compactar"],
    )
    return novo


def _sem_ids(registro):
    controles = [c for c in registro["controles"] if c["papel"] != "ids"]
    for numero, controle in enumerate(controles, 1):
        controle["numero"] = numero
    registro["cobertura"]["controles"] = [c["arquivo"] for c in controles]
    return {**registro, "controles": controles}


@pytest.mark.parametrize(
    ("fonte", "recurso", "exige"),
    [("sfb", "cnfp", True), ("ana", "massas_dagua", True), ("ibge", "malha_municipal", False)],
)
@pytest.mark.usefixtures("arcgis")
async def test_manifesto_ok_exige_a_lista_oficial_no_cnfp_e_na_ana(tmp_path, fonte, recurso, exige):
    await bruto.coletar(*CHAVE, destino=tmp_path, tamanho_pagina=2)
    (registro,) = manifesto(tmp_path)
    com_ids = _como(registro, fonte, recurso)

    assert models.RecursoBruto.model_validate(com_ids).status == "ok"
    if exige:
        with pytest.raises(pydantic.ValidationError, match="lista oficial de IDs referenciada"):
            models.RecursoBruto.model_validate(_sem_ids(com_ids))
    else:
        assert models.RecursoBruto.model_validate(_sem_ids(com_ids)).status == "ok"
