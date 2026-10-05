from __future__ import annotations

import hashlib
import io
import json
import zipfile
from pathlib import Path

import httpx
import pytest

from agrobr import bruto
from agrobr.bruto import arquivo as bruto_arquivo
from agrobr.bruto import models, registry, validation
from agrobr.constants import URLS, Fonte
from agrobr.exceptions import (
    ContractViolationError,
    InvalidParameterError,
    ParseError,
    ResourceLimitError,
    SourceUnavailableError,
)
from tests.test_bruto.conftest import ZIP_SINTETICO, manifesto, resposta

GOLDEN_IBAMA = (
    Path(__file__).parents[1]
    / "golden_data"
    / "ibama"
    / "oficial_20260923"
    / "termo_de_embargo_recorte.csv"
)
CSV_CNUC = "ID_UC;Código UC;Nome da UC;UF\r\n1.045;0000.00.1045;RESERVA SINTÉTICA;SP\r\n".encode(
    "cp1252"
)
CATALOGO = URLS[Fonte.CNUC]["ckan_package"]
URL_CADASTRO = URLS[Fonte.CNUC]["cadastro_csv"]
CADASTRO = ("cnuc", "cadastro")
RECURSOS = {
    ("acervo_fundiario", "assentamentos"): (
        "https://certificacao.incra.gov.br/csv_shp/zip/Assentamento%20Brasil.zip",
        "zip",
        None,
        ZIP_SINTETICO,
    ),
    CADASTRO: (URL_CADASTRO, "csv", 202607, CSV_CNUC),
    ("ibama", "termos_embargo"): (
        URLS[Fonte.IBAMA]["termo_embargo_csv"],
        "csv",
        None,
        GOLDEN_IBAMA.read_bytes(),
    ),
    ("ibge", "malha_municipal_zip"): (
        URLS[Fonte.IBGE]["zip_malha_municipal"],
        "zip",
        2025,
        ZIP_SINTETICO,
    ),
    ("ibge", "areas_urbanizadas_zip"): (
        URLS[Fonte.IBGE]["zip_areas_urbanizadas"],
        "zip",
        2022,
        ZIP_SINTETICO,
    ),
}
CSVS = [chave for chave, (_, formato, _, _) in RECURSOS.items() if formato == "csv"]
CABECALHOS = {"Last-Modified": "Fri, 02 Oct 2026 20:00:00 GMT", "ETag": '"get"'}


def _xlsx() -> bytes:
    pacote = io.BytesIO()
    with zipfile.ZipFile(pacote, "w") as planilha:
        planilha.writestr("[Content_Types].xml", "<Types/>")
        planilha.writestr("xl/workbook.xml", "<workbook/>")
    return pacote.getvalue()


def _catalogo(*recursos: dict[str, str]) -> bytes:
    return json.dumps({"success": True, "result": {"resources": list(recursos)}}).encode()


CSV_2026_07 = {"name": "CNUC_2026_07", "format": "CSV", "url": URL_CADASTRO}
SHP_2026_07 = {"name": "CNUC_2026_07", "format": "zip + shp", "url": "https://nuvem.test/cnuc.zip"}


class Servidor:
    def __init__(self) -> None:
        self.respostas: dict[str, list[httpx.Response]] = {}
        self.pedidos: list[str] = []
        self.catalogo = _catalogo(CSV_2026_07, SHP_2026_07)

    def responder(self, request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        self.pedidos.append(url)
        fila = self.respostas.get(url)
        if fila:
            return fila.pop(0)
        if url == CATALOGO:
            return resposta(200, self.catalogo, {"Content-Type": "application/json"})
        return resposta(404, b"nao encontrado")


@pytest.fixture
def servidor(monkeypatch: pytest.MonkeyPatch) -> Servidor:
    falso = Servidor()

    def sessao() -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=httpx.MockTransport(falso.responder))

    monkeypatch.setattr(bruto_arquivo, "sessao", sessao)
    return falso


def _antes(chave: tuple[str, str]) -> list[str]:
    return [CATALOGO] if chave == CADASTRO else []


@pytest.mark.parametrize("chave", list(RECURSOS), ids="/".join)
async def test_cada_recurso_guarda_o_arquivo_nacional_como_veio(servidor, tmp_path, chave):
    url, formato, edicao, corpo = RECURSOS[chave]
    servidor.respostas[url] = [resposta(200, corpo, CABECALHOS)]

    entrada = (await bruto.coletar(*chave, destino=tmp_path)).entrada

    assert servidor.pedidos == [*_antes(chave), url]
    assert (entrada.status, entrada.modo, entrada.formato, entrada.nome) == (
        "ok",
        "arquivo",
        formato,
        "brasil",
    )
    assert (entrada.url_solicitada, entrada.url, entrada.parametros) == (url, url, {})
    assert entrada.selecao == models.Selecao(
        uf=None, bbox=None, bbox_crs=None, camada=None, edicao=edicao, natureza=None
    )
    assert entrada.arquivo == f"{chave[0]}/{chave[1]}/brasil/{entrada.coleta_id}/original.{formato}"
    assert (tmp_path / entrada.arquivo).read_bytes() == corpo
    assert (entrada.bytes, entrada.bytes_armazenados) == (len(corpo), len(corpo))
    assert entrada.sha256 == hashlib.sha256(corpo).hexdigest()
    assert entrada.cabecalhos["last-modified"] == CABECALHOS["Last-Modified"]
    assert (entrada.crs, entrada.cobertura.estado, entrada.avisos) == (None, "nao_aplicavel", [])
    (linha,) = (tmp_path / "manifesto.jsonl").read_text("utf-8").splitlines()
    assert models.RecursoBruto.model_validate_json(linha).linha() == linha


RECUSAS = [
    *((chave, b"<!DOCTYPE html><html>manutencao</html>\n", "CSV esperado") for chave in CSVS),
    *((chave, _xlsx(), "CSV esperado") for chave in CSVS),
    *((chave, b"", "CSV esperado") for chave in CSVS),
    *((chave, b"ID;NOME\r\n1;x\r\n", "CSV esperado") for chave in CSVS),
    (("ibge", "malha_municipal_zip"), b"<html>manutencao</html>", "assinatura de ZIP"),
    (("ibge", "areas_urbanizadas_zip"), b"", "assinatura de ZIP"),
    (("acervo_fundiario", "assentamentos"), b"<html>manutencao</html>", "assinatura de ZIP"),
]


@pytest.mark.parametrize(
    ("chave", "corpo", "trecho"),
    RECUSAS,
    ids=[f"{'/'.join(chave)}-{corpo[:6]!r}" for chave, corpo, _ in RECUSAS],
)
async def test_resposta_200_fora_do_formato_e_erro_de_parse(
    servidor, tmp_path, chave, corpo, trecho
):
    url = RECURSOS[chave][0]
    servidor.respostas[url] = [resposta(200, corpo, {"Content-Type": "text/csv"})]

    with pytest.raises(ParseError, match=trecho):
        await bruto.coletar(*chave, destino=tmp_path)

    (registro,) = manifesto(tmp_path)
    assert (registro["status"], registro["arquivo"], registro["erro"]["tipo"]) == (
        "erro",
        None,
        "ParseError",
    )
    assert not list(tmp_path.rglob("original.*"))


@pytest.mark.parametrize("chave", list(RECURSOS), ids="/".join)
async def test_404_fica_ausente_e_a_retomada_baixa_quando_o_arquivo_aparece(
    servidor, tmp_path, chave
):
    url, _, _, corpo = RECURSOS[chave]

    ausente = (await bruto.coletar(*chave, destino=tmp_path)).entrada
    servidor.respostas[url] = [resposta(200, corpo, CABECALHOS)]
    retomada = await bruto.coletar(*chave, destino=tmp_path, retomar=True)

    assert (ausente.status, ausente.http_status, ausente.arquivo) == ("ausente_na_fonte", 404, None)
    assert ausente.erro is not None and ausente.erro.tipo == "HTTP404"
    assert (retomada.entrada.status, retomada.reutilizado) == ("ok", False)
    assert servidor.pedidos == [*_antes(chave), url] * 2
    assert [e["status"] for e in manifesto(tmp_path)] == ["ok"]


@pytest.mark.parametrize("chave", CSVS, ids="/".join)
async def test_retomada_reusa_o_csv_conferido_pelo_hash_e_recusa_o_alterado(
    servidor, tmp_path, chave
):
    url, _, _, corpo = RECURSOS[chave]
    servidor.respostas[url] = [resposta(200, corpo, CABECALHOS)]
    primeira = (await bruto.coletar(*chave, destino=tmp_path)).entrada
    pedidos = list(servidor.pedidos)

    reuso = await bruto.coletar(*chave, destino=tmp_path, retomar=True)
    (tmp_path / primeira.arquivo).write_bytes(corpo[:-1] + b"X")

    assert (reuso.reutilizado, reuso.entrada) == (True, primeira)
    assert servidor.pedidos == pedidos
    with pytest.raises(ContractViolationError):
        await bruto.coletar(*chave, destino=tmp_path, retomar=True)


@pytest.mark.parametrize("chave", list(RECURSOS), ids="/".join)
@pytest.mark.parametrize(
    ("argumentos", "trecho"),
    [
        ({"uf": "AL"}, "não aceita uf"),
        ({"bbox": (-48.0, -16.0, -47.0, -15.0), "nome": "x"}, "não aceita bbox"),
        ({"tamanho_pagina": 10}, "não se aplica"),
    ],
)
async def test_recorte_e_paginacao_recusados_antes_da_rede(
    servidor, tmp_path, chave, argumentos, trecho
):
    with pytest.raises(InvalidParameterError, match=trecho):
        await bruto.coletar(*chave, destino=tmp_path, **argumentos)

    assert servidor.pedidos == []


@pytest.mark.parametrize("chave", list(RECURSOS), ids="/".join)
def test_planejar_nao_toca_a_rede(servidor, chave):
    registrado = registry.recurso(*chave)
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

    assert (plano.url_solicitada, plano.formato) == RECURSOS[chave][:2]
    assert (plano.opcoes.compactar, plano.opcoes.tamanho_pagina) == (False, None)
    assert servidor.pedidos == []


@pytest.mark.parametrize(
    ("catalogo", "trecho"),
    [
        (
            _catalogo({**CSV_2026_07, "url": URL_CADASTRO.replace(".csv", "_atualizado.csv")}),
            "_atualizado.csv",
        ),
        (_catalogo(SHP_2026_07), r"em \[\]"),
        (_catalogo(CSV_2026_07, {**CSV_2026_07, "format": "csv"}), "republicada ou retirada"),
        (b"<html>catalogo fora</html>", "ilegível"),
        (json.dumps({"result": {"resources": [{"name": "CNUC_2026_07"}]}}).encode(), "ilegível"),
    ],
    ids=["url_nova", "sem_csv", "duplicado", "html", "sem_formato"],
)
async def test_cadastro_cnuc_so_baixa_a_edicao_que_o_catalogo_confirma(
    servidor, tmp_path, catalogo, trecho
):
    servidor.catalogo = catalogo
    servidor.respostas[URL_CADASTRO] = [resposta(200, CSV_CNUC)]

    with pytest.raises(ParseError, match=trecho):
        await bruto.coletar(*CADASTRO, destino=tmp_path)

    assert servidor.pedidos == [CATALOGO]
    (registro,) = manifesto(tmp_path)
    assert (registro["status"], registro["arquivo"], registro["erro"]["tipo"]) == (
        "erro",
        None,
        "ParseError",
    )


@pytest.mark.parametrize(("status", "tentativas"), [(404, 1), (503, 3)])
async def test_catalogo_cnuc_fora_do_ar_nao_segue_sem_a_conferencia(
    servidor, tmp_path, status, tentativas
):
    servidor.respostas[CATALOGO] = [resposta(status, b"fora") for _ in range(5)]
    servidor.respostas[URL_CADASTRO] = [resposta(200, CSV_CNUC)]

    with pytest.raises(SourceUnavailableError):
        await bruto.coletar(*CADASTRO, destino=tmp_path)

    assert servidor.pedidos == [CATALOGO] * tentativas
    erro = manifesto(tmp_path)[0]["erro"]
    assert (erro["tipo"], erro["http_status"], erro["url"]) == (
        "SourceUnavailableError",
        status,
        CATALOGO,
    )


async def test_catalogo_cnuc_conta_no_orcamento_da_chamada(servidor, tmp_path):
    servidor.respostas[URL_CADASTRO] = [resposta(200, CSV_CNUC)]
    limite = len(servidor.catalogo) + len(CSV_CNUC) - 1
    assert max(len(servidor.catalogo), len(CSV_CNUC)) < limite

    with pytest.raises(ResourceLimitError):
        await bruto.coletar(
            *CADASTRO, destino=tmp_path, limites=bruto.LimitesBrutos(max_bytes_recurso=limite)
        )

    assert servidor.pedidos == [CATALOGO, URL_CADASTRO]
    assert manifesto(tmp_path)[0]["erro"]["tipo"] == "ResourceLimitError"


@pytest.mark.parametrize(
    ("inicio", "esperado"),
    [
        ('﻿"SEQ_TAD";"NUM_TAD"\n1;2\n'.encode(), ["SEQ_TAD", "NUM_TAD"]),
        ("ID_UC;Código UC\r\n".encode("cp1252"), ["ID_UC", "Código UC"]),
        ("ID_UC;Código UC\r\n".encode(), ["ID_UC", "Código UC"]),
        (b"SEQ_TAD;NUM_TAD", None),
    ],
    ids=["utf8_bom_aspas", "cp1252_crlf", "utf8", "sem_fim_de_linha"],
)
def test_cabecalho_csv(inicio, esperado):
    assert bruto_arquivo.cabecalho_csv(inicio, ";") == esperado
