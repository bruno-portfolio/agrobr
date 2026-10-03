from __future__ import annotations

import os
import subprocess
import sys
import threading
import time

import pytest

from agrobr import bruto
from agrobr.bruto import storage
from agrobr.exceptions import ContractViolationError, InvalidParameterError, ResourceLimitError
from tests.test_bruto.conftest import ARQUIVO, PAGINADO

SEGURAR = """
import sys, time
from pathlib import Path
from agrobr.bruto import storage
with storage.trava(Path(sys.argv[1])):
    print("travado", flush=True)
    time.sleep(120)
"""


def test_trava_entre_threads_falha_na_hora(tmp_path):
    entrou, liberar = threading.Event(), threading.Event()

    def segurar():
        with storage.trava(tmp_path):
            entrou.set()
            liberar.wait(10)

    thread = threading.Thread(target=segurar)
    thread.start()
    entrou.wait(10)
    try:
        with pytest.raises(ResourceLimitError, match="outra coleta"), storage.trava(tmp_path):
            pass
    finally:
        liberar.set()
        thread.join()
    with storage.trava(tmp_path):
        pass


def test_trava_entre_processos_e_solta_quando_o_processo_morre(tmp_path):
    processo = subprocess.Popen(
        [sys.executable, "-c", SEGURAR, str(tmp_path)], stdout=subprocess.PIPE, text=True
    )
    try:
        assert processo.stdout is not None and processo.stdout.readline().strip() == "travado"
        with pytest.raises(ResourceLimitError, match="outra coleta"), storage.trava(tmp_path):
            pass
    finally:
        processo.kill()
        processo.wait(30)
    assert _trava_livre_em(tmp_path, segundos=10)


def _trava_livre_em(raiz, *, segundos):
    prazo = time.monotonic() + segundos
    while True:
        try:
            with storage.trava(raiz):
                return True
        except ResourceLimitError:
            if time.monotonic() > prazo:
                return False
            time.sleep(0.05)


async def test_coleta_concorrente_no_mesmo_destino_falha_sem_esperar(paginado, tmp_path):
    processo = subprocess.Popen(
        [sys.executable, "-c", SEGURAR, str(tmp_path)], stdout=subprocess.PIPE, text=True
    )
    try:
        assert processo.stdout is not None and processo.stdout.readline().strip() == "travado"
        with pytest.raises(ResourceLimitError, match="outra coleta"):
            await bruto.coletar(*PAGINADO, destino=tmp_path)
    finally:
        processo.kill()
        processo.wait(30)
    assert paginado.pedidos == []


@pytest.mark.parametrize(
    ("conteudo", "classe", "mensagem"),
    [
        (b"\xef\xbb\xbf{}\n", ContractViolationError, "sem BOM"),
        (b"{}\r\n", ContractViolationError, "com LF"),
        (b"{}", ContractViolationError, "LF no fim"),
        (b"{\n", ContractViolationError, "não é JSON"),
        (b'{"schema_version":"1.1.0"}\n', InvalidParameterError, "só grava manifestos 1.0.0"),
        (b'{"schema_version":"2.0.0"}\n', InvalidParameterError, "só grava manifestos 1.0.0"),
        (b'{"schema_version":"1.0.0","tipo":"recurso"}\n', ContractViolationError, "inválida"),
        (b"\xff\n", ContractViolationError, "UTF-8"),
    ],
)
async def test_manifesto_invalido_e_recusado_antes_da_rede(
    arquivo, tmp_path, conteudo, classe, mensagem
):
    (tmp_path / "manifesto.jsonl").write_bytes(conteudo)

    with pytest.raises(classe, match=mensagem):
        await bruto.coletar(*ARQUIVO, uf="AL", destino=tmp_path)

    assert arquivo.pedidos == []
    assert (tmp_path / "manifesto.jsonl").read_bytes() == conteudo


@pytest.mark.usefixtures("arquivo")
async def test_chave_repetida_no_manifesto_e_violacao(tmp_path):
    await bruto.coletar(*ARQUIVO, uf="AL", destino=tmp_path)
    linha = (tmp_path / "manifesto.jsonl").read_bytes()
    (tmp_path / "manifesto.jsonl").write_bytes(linha * 2)

    with pytest.raises(ContractViolationError, match="repetida"):
        await bruto.coletar(*ARQUIVO, uf="SE", destino=tmp_path)


@pytest.mark.usefixtures("arquivo", "paginado")
async def test_falha_na_troca_do_manifesto_preserva_o_anterior(tmp_path, monkeypatch):
    await bruto.coletar(*ARQUIVO, uf="AL", destino=tmp_path)
    antes = (tmp_path / "manifesto.jsonl").read_bytes()
    original = os.replace

    def falhar(origem, destino):
        if str(destino).endswith("manifesto.jsonl"):
            raise PermissionError("disco cheio")
        return original(origem, destino)

    monkeypatch.setattr(os, "replace", falhar)

    with pytest.raises(PermissionError, match="disco cheio"):
        await bruto.coletar(*PAGINADO, destino=tmp_path)

    assert (tmp_path / "manifesto.jsonl").read_bytes() == antes
    assert not list(tmp_path.glob(".manifesto.jsonl.*"))


@pytest.mark.usefixtures("arquivo")
async def test_manifesto_acima_do_teto_nao_publica(tmp_path, monkeypatch):
    from agrobr import constants

    monkeypatch.setattr(constants, "BRUTO_MAX_BYTES_MANIFESTO", 1500)

    with pytest.raises(ResourceLimitError, match="manifesto passaria"):
        await bruto.coletar(*ARQUIVO, uf="AL", destino=tmp_path)

    assert not (tmp_path / "manifesto.jsonl").exists()


def test_link_dentro_do_destino_e_recusado(tmp_path):
    fora = tmp_path / "fora"
    fora.mkdir()
    raiz = tmp_path / "raiz"
    raiz.mkdir()
    try:
        os.symlink(fora, raiz / "ibge", target_is_directory=True)
    except OSError:
        pytest.skip("sem permissão para criar link simbólico")

    with pytest.raises(ContractViolationError, match="link ou junção"):
        storage.caminho(raiz, "ibge/malha_municipal/DF/x/p000001.geojson")
