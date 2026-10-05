from __future__ import annotations

import asyncio
import contextvars
import gc
import threading

import pytest

from agrobr.utils import tasks


async def test_cancelamento_espera_a_thread_terminar():
    loop = asyncio.get_running_loop()
    comecou = asyncio.Event()
    liberar = threading.Event()
    terminou = threading.Event()

    def trabalho() -> str:
        loop.call_soon_threadsafe(comecou.set)
        liberar.wait(5)
        terminou.set()
        return "ok"

    tarefa = asyncio.create_task(tasks.to_thread_ate_o_fim(trabalho))
    await asyncio.wait_for(comecou.wait(), 2)
    tarefa.cancel()
    await asyncio.sleep(0.05)
    tarefa.cancel()
    await asyncio.sleep(0.05)

    assert not tarefa.done()
    liberar.set()
    with pytest.raises(asyncio.CancelledError):
        await tarefa
    assert terminou.is_set()


async def test_sem_cancelamento_devolve_o_resultado_e_a_excecao_da_thread():
    assert await tasks.to_thread_ate_o_fim(lambda a, b=0: a + b, 1, b=2) == 3
    with pytest.raises(ValueError, match="falhou"):
        await tasks.to_thread_ate_o_fim(lambda: (_ for _ in ()).throw(ValueError("falhou")))


@pytest.mark.parametrize("modo", ["simples", "repetido", "timeout"])
@pytest.mark.parametrize("thread_falha", [False, True])
async def test_cancela_so_depois_da_thread_e_sem_erro_no_handler_do_loop(modo, thread_falha):
    loop = asyncio.get_running_loop()
    comecou = asyncio.Event()
    liberar = threading.Event()
    terminou = threading.Event()
    prazos: list[asyncio.Timeout] = []
    sem_tratamento: list[dict[str, object]] = []
    contextos: list[str] = []
    marca = contextvars.ContextVar("marca", default="ausente")
    token = marca.set("mesmo-contexto")
    handler_original = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, contexto: sem_tratamento.append(contexto))

    def trabalho() -> int:
        contextos.append(marca.get())
        loop.call_soon_threadsafe(comecou.set)
        try:
            assert liberar.wait(5)
            if thread_falha:
                raise ValueError("a exceção da thread tem de ser recolhida")
            return 42
        finally:
            terminou.set()

    async def rodar() -> int:
        if modo == "timeout":
            async with asyncio.timeout(None) as prazo:
                prazos.append(prazo)
                return await tasks.to_thread_ate_o_fim(trabalho)
        return await tasks.to_thread_ate_o_fim(trabalho)

    tarefa = asyncio.create_task(rodar())
    try:
        await asyncio.wait_for(comecou.wait(), 2)
        if modo == "timeout":
            prazos[0].reschedule(loop.time())
        else:
            tarefa.cancel("cancelamento original")
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        if modo == "repetido":
            for indice in range(3):
                tarefa.cancel(f"repetido {indice}")
                await asyncio.sleep(0)
        assert not tarefa.done()
        assert not terminou.is_set()
        liberar.set()
        esperado = TimeoutError if modo == "timeout" else asyncio.CancelledError
        with pytest.raises(esperado) as erro:
            await tarefa
        if modo != "timeout":
            assert str(erro.value) == "cancelamento original"
            assert tarefa.cancelled()
        assert terminou.is_set()
        assert contextos == ["mesmo-contexto"]
        gc.collect()
        await asyncio.sleep(0)
        assert not sem_tratamento
    finally:
        liberar.set()
        await asyncio.gather(tarefa, return_exceptions=True)
        marca.reset(token)
        loop.set_exception_handler(handler_original)
