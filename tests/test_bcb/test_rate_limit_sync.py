from __future__ import annotations

import asyncio
import threading
import time
import warnings

import pytest

from agrobr import sync
from agrobr.http.rate_limiter import RateLimiter
from tests.helpers import sem_excecao

IPCA_2024 = {"data_inicial": "01/01/2024", "data_final": "31/12/2024"}


@pytest.fixture
def intervalo_do_bcb(monkeypatch):
    monkeypatch.setenv("AGROBR_HTTP_RATE_LIMIT_BCB", "1.0")


def espiao(sgs_http, *, demora: float = 0.0) -> dict:
    registro: dict = {"inicios": [], "ativos": 0, "pico": 0}
    trava = threading.Lock()

    def observar(_request, _numero):
        with trava:
            registro["inicios"].append(time.perf_counter())
            registro["ativos"] += 1
            registro["pico"] = max(registro["pico"], registro["ativos"])
        time.sleep(demora)
        with trava:
            registro["ativos"] -= 1

    sgs_http(observar)
    return registro


def ipca() -> int:
    return len(sync.bcb.sgs("ipca", **IPCA_2024))


def em_threads(alvo, quantas: int = 2) -> list:
    barreira = threading.Barrier(quantas)
    saidas: list = []

    def rodar():
        barreira.wait()
        try:
            saidas.append(alvo())
        except Exception as erro:
            saidas.append(erro)

    threads = [threading.Thread(target=rodar, daemon=True) for _ in range(quantas)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)
    assert not any(thread.is_alive() for thread in threads), "thread do sync não terminou"
    return saidas


@pytest.mark.usefixtures("intervalo_do_bcb")
def test_sync_em_sequencia_respeita_1s_no_bcb(sgs_http):
    registro = espiao(sgs_http)
    with sem_excecao():
        assert [ipca(), ipca()] == [12, 12]
    primeiro, segundo = registro["inicios"]
    assert segundo - primeiro >= 0.95


@pytest.mark.usefixtures("intervalo_do_bcb")
def test_sync_em_2_threads_respeita_1s_no_bcb(sgs_http):
    registro = espiao(sgs_http)
    assert em_threads(ipca) == [12, 12]
    primeiro, segundo = sorted(registro["inicios"])
    assert segundo - primeiro >= 0.95


def test_sync_em_2_threads_respeita_a_concorrencia_do_bcb(sgs_http, monkeypatch):
    monkeypatch.setenv("AGROBR_HTTP_RATE_LIMIT_BCB", "0")
    registro = espiao(sgs_http, demora=0.3)
    assert em_threads(ipca) == [12, 12]
    assert registro["pico"] == 1


@pytest.mark.usefixtures("intervalo_do_bcb")
def test_sync_com_loop_rodando_herda_o_intervalo_do_bcb(sgs_http):
    registro = espiao(sgs_http)

    async def notebook():
        return [ipca(), ipca()]

    with warnings.catch_warnings(), sem_excecao():
        warnings.simplefilter("ignore")
        assert asyncio.run(notebook()) == [12, 12]
    primeiro, segundo = registro["inicios"]
    assert segundo - primeiro >= 0.95


@pytest.mark.usefixtures("intervalo_do_bcb")
@pytest.mark.timeout(60)
def test_vaga_presa_no_loop_que_chama_o_sync_nao_trava(sgs_http, monkeypatch):
    monkeypatch.setenv("AGROBR_HTTP_TIMEOUT_READ", "0.3")
    registro = espiao(sgs_http)
    saida: dict = {}

    async def dono(segurando: asyncio.Event, liberar: asyncio.Event) -> None:
        async with RateLimiter.acquire("bcb"):
            saida["dono"] = time.perf_counter()
            segurando.set()
            await liberar.wait()

    async def notebook() -> int:
        segurando, liberar = asyncio.Event(), asyncio.Event()
        tarefa = asyncio.create_task(dono(segurando, liberar))
        await segurando.wait()
        linhas = ipca()
        liberar.set()
        await tarefa
        return linhas

    def rodar() -> None:
        with warnings.catch_warnings(record=True) as avisos:
            warnings.simplefilter("always")
            try:
                saida["linhas"] = asyncio.run(notebook())
            except Exception as erro:
                saida["erro"] = erro
        saida["avisos"] = [str(aviso.message) for aviso in avisos]

    thread = threading.Thread(target=rodar, daemon=True)
    thread.start()
    thread.join(timeout=20)
    assert not thread.is_alive(), "o sync do BCB travou com a vaga presa no loop chamador"
    assert "erro" not in saida, saida.get("erro")
    assert saida["linhas"] == 12
    assert registro["inicios"][0] - saida["dono"] >= 0.95
    assert [aviso for aviso in saida["avisos"] if "vaga de bcb" in aviso]
