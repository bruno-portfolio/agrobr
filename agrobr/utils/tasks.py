from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Callable, Coroutine
from typing import Any, TypeVar

T = TypeVar("T")


async def gather_or_cancel(*coros: Coroutine[Any, Any, T]) -> list[T]:
    """Como ``asyncio.gather``, mas a 1ª falha cancela as outras tarefas.

    Roda num ``asyncio.TaskGroup`` e sobe a 1ª exceção sozinha, sem o ``ExceptionGroup``, com a causa dela: quem
    captura ``SourceUnavailableError`` segue capturando.
    """
    try:
        async with asyncio.TaskGroup() as group:
            tasks = [group.create_task(coro) for coro in coros]
    except BaseExceptionGroup as errors:
        first = errors.exceptions[0]
        raise first from first.__cause__
    return [task.result() for task in tasks]


async def to_thread_ate_o_fim(func: Callable[..., T], /, *args: Any, **kwargs: Any) -> T:
    """``asyncio.to_thread`` que, se cancelado, espera a thread terminar antes de propagar o cancelamento.

    Para quem segura um lock ou um arquivo que a thread usa: soltá-lo no meio deixaria a thread rodando sem ele.
    """
    tarefa = asyncio.ensure_future(asyncio.to_thread(func, *args, **kwargs))
    try:
        await asyncio.wait([tarefa])
    except asyncio.CancelledError:
        while not tarefa.done():
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.wait([tarefa])
        if not tarefa.cancelled():
            tarefa.exception()
        raise
    return tarefa.result()
