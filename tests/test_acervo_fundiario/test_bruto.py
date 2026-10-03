from __future__ import annotations

import dataclasses
import json
from pathlib import Path
from typing import Any

import httpx
import pytest

from agrobr import bruto
from agrobr.acervo_fundiario import bruto as acervo_bruto
from agrobr.acervo_fundiario import client, parser
from agrobr.bruto import registry, validation
from agrobr.exceptions import InvalidParameterError, ParseError
from tests.test_bruto.conftest import ZIP_SINTETICO, manifesto, resposta

GOLDEN = Path(__file__).parents[1] / "golden_data" / "acervo_fundiario"
BASE = "https://certificacao.incra.gov.br/csv_shp/zip/"
URLS_AL = {
    "sigef_publico": (BASE + "Sigef%20P%C3%BAblico_AL.zip", "publico"),
    "sigef_privado": (BASE + "Sigef%20Privado_AL.zip", "privado"),
    "snci_publico": (BASE + "Im%C3%B3vel%20certificado%20SNCI%20P%C3%BAblico_AL.zip", "publico"),
    "snci_privado": (BASE + "Im%C3%B3vel%20certificado%20SNCI%20Privado_AL.zip", "privado"),
    "snci_brasil": (BASE + "Im%C3%B3vel%20certificado%20SNCI%20Brasil_AL.zip", None),
}
CABECALHOS_GET = {"ETag": '"get"', "Last-Modified": "Fri, 02 Oct 2026 07:07:47 GMT"}


class Incra:
    def __init__(self) -> None:
        self.arquivos: dict[str, list[httpx.Response]] = {}
        self.pedidos: list[tuple[str, str]] = []

    def responder(self, request: httpx.Request) -> httpx.Response:
        self.pedidos.append((request.method, str(request.url)))
        if request.method == "HEAD":
            return httpx.Response(200, headers={"ETag": '"head"', "Content-Length": "1"})
        fila = self.arquivos.get(str(request.url))
        return fila.pop(0) if fila else resposta(404, b"")


@pytest.fixture
def incra(monkeypatch: pytest.MonkeyPatch) -> Incra:
    servidor = Incra()
    original = httpx.AsyncClient

    def fabrica(**kwargs: Any) -> httpx.AsyncClient:
        return original(transport=httpx.MockTransport(servidor.responder), **kwargs)

    monkeypatch.setattr(client.httpx, "AsyncClient", fabrica)
    for chave in registry.RECURSOS:
        if chave[0] == "acervo_fundiario":
            entrada = dataclasses.replace(registry.RECURSOS[chave], habilitado=True)
            monkeypatch.setitem(registry.RECURSOS, chave, entrada)
    return servidor


def _zip_200(corpo: bytes = ZIP_SINTETICO, **cabecalhos: str) -> httpx.Response:
    return resposta(200, corpo, {**CABECALHOS_GET, **cabecalhos})


@pytest.mark.parametrize("recurso", sorted(URLS_AL))
async def test_cada_recurso_baixa_o_arquivo_da_uf_e_registra_a_natureza(incra, tmp_path, recurso):
    url, natureza = URLS_AL[recurso]
    incra.arquivos[url] = [_zip_200()]

    coleta = await bruto.coletar("acervo_fundiario", recurso, uf="al", destino=tmp_path)

    entrada = coleta.entrada
    assert incra.pedidos == [("GET", url)]
    assert (entrada.status, entrada.url_solicitada, entrada.url) == ("ok", url, url)
    assert (entrada.modo, entrada.formato, entrada.parametros) == ("arquivo", "zip", {})
    assert (entrada.selecao.uf, entrada.selecao.natureza, entrada.nome) == ("AL", natureza, "AL")
    assert entrada.arquivo == f"acervo_fundiario/{recurso}/AL/{entrada.coleta_id}/original.zip"
    assert (tmp_path / entrada.arquivo).read_bytes() == ZIP_SINTETICO
    assert (entrada.crs, entrada.crs_evidencia, entrada.paginas) == (None, None, [])


@pytest.mark.parametrize("natureza", ["publico", "privado"])
async def test_snci_oficial_guardado_byte_a_byte_com_o_hash_publicado(incra, tmp_path, natureza):
    pasta = GOLDEN / f"bruto_snci_{natureza}_al_20261003"
    metadata = json.loads((pasta / "metadata.json").read_text("utf-8"))
    original = (pasta / "response.zip").read_bytes()
    incra.arquivos[metadata["url"]] = [
        resposta(
            200,
            original,
            {
                "ETag": metadata["etag"],
                "Last-Modified": metadata["last_modified"],
                "Content-Length": str(metadata["content_length"]),
                "Content-Type": metadata["content_type"],
            },
        )
    ]

    entrada = (
        await bruto.coletar("acervo_fundiario", f"snci_{natureza}", uf="AL", destino=tmp_path)
    ).entrada

    assert (entrada.sha256, entrada.bytes) == (metadata["sha256"], metadata["bytes"])
    assert entrada.bytes_armazenados == len(original)
    assert (tmp_path / entrada.arquivo).read_bytes() == original
    assert entrada.cabecalhos["etag"] == metadata["etag"]
    assert entrada.cabecalhos["last-modified"] == metadata["last_modified"]
    assert entrada.url == metadata["url"]


async def test_404_fica_ausente_e_a_retomada_baixa_quando_o_arquivo_aparece(incra, tmp_path):
    url, _ = URLS_AL["snci_privado"]

    ausente = (
        await bruto.coletar("acervo_fundiario", "snci_privado", uf="AL", destino=tmp_path)
    ).entrada
    incra.arquivos[url] = [_zip_200()]
    retomada = await bruto.coletar(
        "acervo_fundiario", "snci_privado", uf="AL", destino=tmp_path, retomar=True
    )

    assert (ausente.status, ausente.http_status, ausente.arquivo) == ("ausente_na_fonte", 404, None)
    assert ausente.erro is not None and ausente.erro.tipo == "HTTP404"
    assert (retomada.entrada.status, retomada.reutilizado) == ("ok", False)
    assert incra.pedidos == [("GET", url), ("GET", url)]
    assert [e["status"] for e in manifesto(tmp_path)] == ["ok"]


async def test_so_get_sem_head_e_os_cabecalhos_sao_os_do_get(incra, tmp_path):
    url, _ = URLS_AL["sigef_publico"]
    incra.arquivos[url] = [_zip_200()]

    entrada = (
        await bruto.coletar("acervo_fundiario", "sigef_publico", uf="AL", destino=tmp_path)
    ).entrada

    assert [metodo for metodo, _ in incra.pedidos] == ["GET"]
    assert entrada.cabecalhos["etag"] == '"get"'
    assert entrada.bytes == len(ZIP_SINTETICO)


async def test_bruto_nao_le_tabela_nem_geo_nem_usa_o_cache(
    incra, tmp_path, isolated_cache, monkeypatch
):
    def proibido(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("o caminho bruto não lê nem guarda no cache")

    nomes_parser = ("parse_snci", "parse_snci_geo", "parse_sigef", "parse_sigef_geo")
    for nome in nomes_parser:
        monkeypatch.setattr(parser, nome, proibido)
    for nome in ("download_and_cache", "adquirir", "_head", "_save_meta", "_load_meta"):
        monkeypatch.setattr(client, nome, proibido)
    url, _ = URLS_AL["snci_brasil"]
    incra.arquivos[url] = [_zip_200()]

    entrada = (
        await bruto.coletar("acervo_fundiario", "snci_brasil", uf="AL", destino=tmp_path / "b")
    ).entrada

    assert entrada.status == "ok"
    assert list(isolated_cache.iterdir()) == []


@pytest.mark.parametrize("corpo", [b"<html>manutencao</html>", b""])
async def test_resposta_200_sem_assinatura_de_zip_e_erro_de_parse(incra, tmp_path, corpo):
    url, _ = URLS_AL["snci_publico"]
    incra.arquivos[url] = [resposta(200, corpo, {"Content-Type": "text/html"})]

    with pytest.raises(ParseError, match="assinatura de ZIP"):
        await bruto.coletar("acervo_fundiario", "snci_publico", uf="AL", destino=tmp_path)

    (registro,) = manifesto(tmp_path)
    assert (registro["status"], registro["arquivo"]) == ("erro", None)
    assert not list(tmp_path.rglob("original.zip"))


@pytest.mark.parametrize(
    ("argumentos", "trecho"),
    [({}, "exige uf"), ({"uf": "AL", "bbox": (-37.0, -10.0, -36.0, -9.0), "nome": "x"}, "bbox")],
)
async def test_selecao_invalida_recusada_antes_da_rede(
    incra, tmp_path, argumentos: dict[str, Any], trecho: str
):
    with pytest.raises(InvalidParameterError, match=trecho):
        await bruto.coletar("acervo_fundiario", "snci_publico", destino=tmp_path, **argumentos)

    assert incra.pedidos == []


def test_planejar_nao_toca_a_rede(incra):
    pedido = validation.pedido(
        registry.recurso("acervo_fundiario", "snci_publico"),
        nome=None,
        uf="AL",
        bbox=None,
        bbox_crs="EPSG:4674",
        tamanho_pagina=None,
        compactar=True,
        retomar=False,
        limites=None,
    )

    plano = acervo_bruto.adaptador.planejar(pedido)

    assert plano.url_solicitada == URLS_AL["snci_publico"][0]
    assert incra.pedidos == []
