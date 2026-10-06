from __future__ import annotations

import gzip
import json
from pathlib import Path

import httpx
import pydantic
import pytest

from agrobr import bruto, constants, exceptions
from tests import helpers


@pytest.fixture
def arcgis(monkeypatch):
    golden = Path(__file__).parents[1] / "golden_data/ana/bruto_massas_4674_20261003"
    corpos = {
        nome: (golden / f"{nome}.json").read_bytes() for nome in ("contagem", "ids", "pagina")
    }
    pedidos = []
    original = httpx.AsyncClient

    def responder(pedido):
        pedidos.append(pedido)
        parametros = pedido.url.params
        papel = (
            "contagem"
            if "returnCountOnly" in parametros
            else "ids"
            if "returnIdsOnly" in parametros
            else "pagina"
        )
        return httpx.Response(
            200,
            headers={
                "Content-Type": "application/json",
                "Content-Encoding": corpos.get("encoding", b"identity").decode(),
            },
            stream=httpx.ByteStream(corpos[papel]),
        )

    def cliente(**kwargs):
        return original(transport=httpx.MockTransport(responder), **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", cliente)
    return corpos, pedidos


async def _coletar(destino, **kwargs):
    return await bruto.coletar(
        "ana",
        "massas_dagua",
        destino=destino,
        nome="barragem_df",
        uf="DF",
        bbox=(-47.466, -15.993, -47.462, -15.988),
        **kwargs,
    )


async def test_coleta_limita_lista_oficial_maior_que_contagem(arcgis, tmp_path):
    corpos, pedidos = arcgis
    corpos["contagem"] = b'{"count":1}'
    corpos["ids"] = b'{"objectIdFieldName":"FID","objectIds":[114424,114425]}'

    with helpers.levanta_exatamente(exceptions.ResourceLimitError, match="2 IDs retidos"):
        await _coletar(tmp_path, limites=bruto.LimitesBrutos(max_ids=1))

    assert len(pedidos) == 2
    manifesto = json.loads((tmp_path / "manifesto.jsonl").read_text(encoding="utf-8"))
    assert manifesto["status"] == "erro"
    assert manifesto["erro"]["tipo"] == "ResourceLimitError"
    assert manifesto["paginas"] == []


async def test_retomada_confere_teto_individual_da_pagina(arcgis, tmp_path):
    _, pedidos = arcgis
    coleta = await _coletar(tmp_path)
    antes = coleta.manifesto.read_bytes()
    pedidos.clear()

    with helpers.levanta_exatamente(exceptions.ResourceLimitError, match="passa do limite de 1"):
        await _coletar(
            tmp_path,
            retomar=True,
            limites=bruto.LimitesBrutos(max_bytes_pagina=1),
        )

    assert pedidos == []
    assert coleta.manifesto.read_bytes() == antes


@pytest.mark.usefixtures("arcgis")
async def test_recurso_publico_recusa_faixa_fid_invertida(tmp_path):
    coleta = await _coletar(tmp_path)
    dados = coleta.entrada.model_dump(mode="json")
    dados["paginas"][0]["paginacao"]["min"] = 114425

    with helpers.levanta_exatamente(pydantic.ValidationError, match="FID com min maior que max"):
        bruto.RecursoBruto.model_validate(dados)


async def test_retomada_recusa_manifesto_acima_do_teto(arcgis, tmp_path, monkeypatch):
    _, pedidos = arcgis
    coleta = await _coletar(tmp_path)
    antes = coleta.manifesto.read_bytes()
    pedidos.clear()
    monkeypatch.setattr(constants, "BRUTO_MAX_BYTES_MANIFESTO", len(antes) - 1)

    with helpers.levanta_exatamente(exceptions.ResourceLimitError, match="manifesto acima de"):
        await _coletar(tmp_path, retomar=True)

    assert pedidos == []
    assert coleta.manifesto.read_bytes() == antes


async def test_coleta_resposta_com_codificacao_gzip_incorreta(arcgis, tmp_path):
    corpos, pedidos = arcgis
    corpos["encoding"] = b"gzip"

    with helpers.levanta_exatamente(exceptions.SourceUnavailableError, match="corpo gzip inválido"):
        await _coletar(tmp_path)

    assert pedidos
    registro = json.loads((tmp_path / "manifesto.jsonl").read_text(encoding="utf-8"))
    assert registro["status"] == "erro"
    assert registro["erro"]["tipo"] == "SourceUnavailableError"
    assert registro["paginas"] == []


async def test_arquivo_recusa_tamanho_alterado_apos_download(monkeypatch, tmp_path):
    golden = (
        Path(__file__).parents[1]
        / "golden_data/ibama/oficial_20260923/termo_de_embargo_recorte.csv"
    )
    corpo = golden.read_bytes()
    original = httpx.AsyncClient
    alterados = []

    def responder(_pedido):
        return httpx.Response(200, stream=httpx.ByteStream(corpo))

    class EscritorConcorrente(httpx.MockTransport):
        async def aclose(self):
            await super().aclose()
            (temporario,) = tmp_path.rglob("*.part")
            assert temporario.read_bytes() == corpo
            with temporario.open("ab") as arquivo:
                arquivo.write(b"\n")
            alterados.append(temporario)

    def cliente(**kwargs):
        return original(transport=EscritorConcorrente(responder), **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", cliente)
    with helpers.levanta_exatamente(
        exceptions.ContractViolationError,
        match=rf"{len(corpo) + 1} bytes no disco e {len(corpo)} lidos",
    ):
        await bruto.coletar("ibama", "termos_embargo", destino=tmp_path)

    assert len(alterados) == 1
    registro = json.loads((tmp_path / "manifesto.jsonl").read_text(encoding="utf-8"))
    assert registro["status"] == "erro"
    assert registro["erro"]["tipo"] == "ContractViolationError"


@pytest.mark.usefixtures("arcgis")
@pytest.mark.parametrize("defeito", ["tamanho_guardado", "tamanho_original"])
async def test_retomada_detecta_tamanho_divergente_do_artefato(tmp_path, defeito):
    coleta = await _coletar(tmp_path)
    pagina = coleta.entrada.paginas[0]
    alvo = tmp_path / pagina.arquivo
    if defeito == "tamanho_guardado":
        alvo.write_bytes(alvo.read_bytes() + b"\n")
        mensagem = "tamanho guardado difere de bytes_armazenados"
    else:
        alterado = gzip.compress(gzip.decompress(alvo.read_bytes()) + b"\n", mtime=0)
        alvo.write_bytes(alterado)
        dados = coleta.entrada.model_dump(mode="json")
        dados["paginas"][0]["bytes_armazenados"] = len(alterado)
        entrada = bruto.RecursoBruto.model_validate(dados)
        coleta.manifesto.write_bytes((entrada.linha() + "\n").encode())
        mensagem = "corpo maior que bytes declarado"
    antes = coleta.manifesto.read_bytes()

    with helpers.levanta_exatamente(exceptions.ContractViolationError, match=mensagem):
        await _coletar(tmp_path, retomar=True)

    assert coleta.manifesto.read_bytes() == antes
