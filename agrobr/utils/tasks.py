from __future__ import annotations

import asyncio
from collections.abc import Coroutine
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
