from __future__ import annotations

import asyncio
import gzip
import hashlib
import json

import pytest

from agrobr import bruto
from agrobr.bruto import models
from agrobr.exceptions import ParseError, SourceUnavailableError
from tests.test_bruto.conftest import (
    ZIP_SINTETICO,
    AdaptadorPaginas,
    FonteFalsa,
    manifesto,
    resposta,
)


@pytest.mark.usefixtures("arquivo")
async def test_arquivo_ok_guarda_o_zip_sem_recompressao_e_os_cabecalhos_do_get(tmp_path):
    resultado = await bruto.coletar("acervo_fundiario", "snci_publico", uf="al", destino=tmp_path)

    entrada = resultado.entrada
    assert (resultado.reutilizado, entrada.status, entrada.nome, entrada.selecao.uf) == (
        False,
        "ok",
        "AL",
        "AL",
    )
    assert resultado.manifesto == tmp_path.absolute() / "manifesto.jsonl"
    guardado = (tmp_path / entrada.arquivo).read_bytes()
    assert guardado == ZIP_SINTETICO
    assert entrada.sha256 == hashlib.sha256(ZIP_SINTETICO).hexdigest()
    assert entrada.bytes == entrada.bytes_armazenados == len(ZIP_SINTETICO)
    assert entrada.http_status == 200 and entrada.http_inicio is not None
    assert entrada.cabecalhos["etag"] == 'W/"abc"' and "set-cookie" not in entrada.cabecalhos
    assert entrada.cobertura.estado == "nao_aplicavel" and entrada.cobertura.completa
    assert manifesto(tmp_path) == [entrada.model_dump(mode="json")]
    linha = (tmp_path / "manifesto.jsonl").read_bytes()
    assert linha.endswith(b"\n") and b"\r" not in linha and linha.count(b"\n") == 1
    assert not list(tmp_path.rglob("*.part"))


async def test_arquivo_404_vira_ausente_na_fonte_datado_e_retorna(arquivo, tmp_path):
    arquivo.status_arquivo = 404

    resultado = await bruto.coletar("acervo_fundiario", "snci_publico", uf="AL", destino=tmp_path)

    entrada = resultado.entrada
    assert entrada.status == "ausente_na_fonte"
    assert (entrada.erro.tipo, entrada.erro.http_status) == ("HTTP404", 404)
    assert entrada.http_status == 404 and entrada.cabecalhos["etag"] == 'W/"abc"'
    assert (entrada.arquivo, entrada.sha256, entrada.bytes) == (None, None, None)
    assert not entrada.cobertura.completa
    diagnostico = list((tmp_path / ".bruto").rglob("diagnostico/http404_*"))
    assert [d.read_bytes() for d in diagnostico] == [ZIP_SINTETICO]


@pytest.mark.parametrize("compactar", [True, False])
async def test_paginas_preservam_os_bytes_e_fecham_a_cobertura(paginado, tmp_path, compactar):
    paginado.ids = [f"53{i:05d}" for i in range(7)]

    resultado = await bruto.coletar(
        "ibge", "malha_municipal", uf="DF", destino=tmp_path, tamanho_pagina=3, compactar=compactar
    )

    entrada = resultado.entrada
    assert entrada.status == "ok" and len(entrada.paginas) == 3
    assert [p.paginacao.inicio for p in entrada.paginas] == [0, 3, 6]
    assert entrada.cobertura.model_dump() == {
        "total_antes": 7,
        "total_depois": 7,
        "recebidas": 7,
        "ids_distintos": 7,
        "ids_repetidos": 0,
        "campo_id": "cd_mun",
        "estado": "conferida",
        "completa": True,
        "snapshot_transacional": False,
        "controles": [c.arquivo for c in entrada.controles],
    }
    assert (
        entrada.crs == "EPSG:4674" and entrada.crs_evidencia.arquivo == entrada.paginas[0].arquivo
    )
    corpos = [r for r in paginado.pedidos if r.url.params.get("startIndex")]
    for pagina, pedido in zip(entrada.paginas, corpos, strict=True):
        guardado = (tmp_path / pagina.arquivo).read_bytes()
        corpo = gzip.decompress(guardado) if compactar else guardado
        assert corpo == paginado.responder(pedido).read()
        assert b"S\xc3\xa3o Jos\xc3\xa9\\r\\n" in corpo
        assert hashlib.sha256(corpo).hexdigest() == pagina.sha256 and pagina.bytes == len(corpo)
        assert pagina.compressao == ("gzip" if compactar else "nenhuma")
        assert pagina.url_solicitada == str(pedido.url)
    if compactar:
        assert gzip.decompress((tmp_path / entrada.paginas[0].arquivo).read_bytes())
        assert (tmp_path / entrada.paginas[0].arquivo).read_bytes()[4:8] == b"\x00\x00\x00\x00"


async def test_gzip_http_e_decodificado_antes_do_hash(paginado, tmp_path):
    paginado.gzip_http = True

    entrada = (await bruto.coletar("ibge", "malha_municipal", destino=tmp_path)).entrada

    pagina = entrada.paginas[0]
    corpo = gzip.decompress((tmp_path / pagina.arquivo).read_bytes())
    assert json.loads(corpo)["features"][0]["properties"]["cd_mun"] == 1
    assert pagina.cabecalhos["content-encoding"] == "gzip"
    assert hashlib.sha256(corpo).hexdigest() == pagina.sha256


async def test_zero_feicoes_confirmado_e_ok_sem_paginas(paginado, tmp_path):
    paginado.ids = []

    entrada = (await bruto.coletar("ibge", "malha_municipal", destino=tmp_path)).entrada

    assert (entrada.status, entrada.paginas, entrada.feicoes) == ("ok", [], 0)
    assert (entrada.crs, entrada.crs_evidencia) == (None, None)
    assert entrada.cobertura.recebidas == entrada.cobertura.ids_distintos == 0


@pytest.mark.parametrize(
    ("estragar", "mensagem"),
    [
        (lambda f: setattr(f, "total_depois", 6), "contagem antes 5, depois 6"),
        (lambda f: setattr(f, "ids", [1, 2, 2, 4, 5]), "repetido"),
        (lambda f: setattr(f, "ids", [1, 2, None, 4, 5]), "nulo ou inválido"),
        (lambda f: setattr(f, "crs", "urn:ogc:def:crs:EPSG::4326"), "CRS"),
        (lambda f: setattr(f, "total_pagina", 9), "declara 9"),
    ],
    ids=["total_mudou", "id_repetido", "id_nulo", "crs", "total_da_pagina"],
)
async def test_divergencia_levanta_parseerror_e_registra_erro_divergente(
    paginado, tmp_path, estragar, mensagem
):
    estragar(paginado)

    with pytest.raises(ParseError, match=mensagem):
        await bruto.coletar("ibge", "malha_municipal", destino=tmp_path, tamanho_pagina=2)

    [linha] = manifesto(tmp_path)
    assert (linha["status"], linha["erro"]["tipo"]) == ("erro", "ParseError")
    assert linha["cobertura"]["estado"] == "divergente" and not linha["cobertura"]["completa"]
    models.RecursoBruto.model_validate(linha)


async def test_403_levanta_sourceunavailable_e_registra_o_status(paginado, tmp_path):
    paginado.respostas["/wfs"] = [resposta(403, b"<html>bloqueado</html>")]

    with pytest.raises(SourceUnavailableError, match="HTTP 403"):
        await bruto.coletar("ibge", "malha_municipal", destino=tmp_path)

    [linha] = manifesto(tmp_path)
    assert (linha["erro"]["tipo"], linha["erro"]["http_status"]) == ("SourceUnavailableError", 403)
    assert linha["erro"]["url"].startswith("https://fonte.test/wfs?")
    assert linha["cobertura"]["estado"] == "nao_comprovada"


async def test_cancelamento_propaga_e_registra_erro(habilitar, fonte, tmp_path):
    class Lento(AdaptadorPaginas):
        async def adquirir(self, plano, *, contexto):
            contexto.conferir_prazo()
            await asyncio.sleep(10 if plano.modo == "paginado" else 0)

    habilitar(("ibge", "malha_municipal"), Lento(fonte))
    tarefa = asyncio.ensure_future(bruto.coletar("ibge", "malha_municipal", destino=tmp_path))
    await asyncio.sleep(0.05)
    tarefa.cancel()

    with pytest.raises(asyncio.CancelledError):
        await tarefa

    [linha] = manifesto(tmp_path)
    assert (linha["status"], linha["erro"]["tipo"], linha["erro"]["url"]) == (
        "erro",
        "CancelledError",
        None,
    )


@pytest.mark.usefixtures("arquivo", "paginado")
async def test_nova_entrada_preserva_as_outras(tmp_path):
    await bruto.coletar("acervo_fundiario", "snci_publico", uf="AL", destino=tmp_path)
    await bruto.coletar("ibge", "malha_municipal", destino=tmp_path)
    await bruto.coletar("ibge", "malha_municipal", uf="DF", destino=tmp_path)

    assert [(e["fonte"], e["nome"]) for e in manifesto(tmp_path)] == [
        ("acervo_fundiario", "AL"),
        ("ibge", "brasil"),
        ("ibge", "DF"),
    ]


async def test_adaptador_com_ordem_errada_vira_contractviolation(habilitar, fonte, tmp_path):
    from agrobr.exceptions import ContractViolationError

    class Pula(AdaptadorPaginas):
        async def adquirir(self, plano, *, contexto):
            assert plano is not None
            async with self.fonte.cliente() as http:
                pedido = models.PedidoHTTP(
                    url="https://fonte.test/wfs",
                    parametros={"resultType": "hits"},
                    papel="contagem_antes",
                    numero=2,
                    formato="xml",
                )
                resposta = await contexto.obter(http, pedido)
                contexto.registrar_controle(resposta, models.LeituraControle("contagem_antes", 5))

    habilitar(("ibge", "malha_municipal"), Pula(fonte))

    with pytest.raises(ContractViolationError, match="fora de ordem"):
        await bruto.coletar("ibge", "malha_municipal", destino=tmp_path)

    [linha] = manifesto(tmp_path)
    assert linha["erro"]["tipo"] == "ContractViolationError"
    assert list((tmp_path / ".bruto").rglob("diagnostico/contagem_antes_000002.bin"))


async def test_ana_ok_exige_a_lista_oficial_de_ids_mesmo_vazia(habilitar, tmp_path):
    class AnaSemIds(AdaptadorPaginas):
        def planejar(self, pedido):
            plano = super().planejar(pedido)
            return plano.model_copy(update={"formato": "esri_json", "campo_id": "FID"})

    fonte = FonteFalsa(ids=[])
    habilitar(("ana", "massas_dagua"), AnaSemIds(fonte))

    with pytest.raises(ParseError, match="lista oficial de IDs"):
        await bruto.coletar("ana", "massas_dagua", uf="AL", destino=tmp_path)
