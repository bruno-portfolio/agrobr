from __future__ import annotations

import asyncio
import os
import tempfile
import time
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager, contextmanager
from pathlib import Path

import structlog

from agrobr import constants

logger = structlog.get_logger()


def _replace_attempts(source: Path, target: Path) -> Iterator[float]:
    for attempt in range(constants.ATOMIC_REPLACE_ATTEMPTS):
        try:
            os.replace(source, target)
            return
        except PermissionError:
            if attempt + 1 == constants.ATOMIC_REPLACE_ATTEMPTS:
                raise
            yield constants.ATOMIC_REPLACE_RETRY_DELAY


@contextmanager
def temporary_output(path: Path) -> Iterator[Path]:
    with tempfile.NamedTemporaryFile(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent, delete=False
    ) as file:
        temporary = Path(file.name)
    try:
        yield temporary
    finally:
        try:
            temporary.unlink(missing_ok=True)
        except OSError as exc:
            logger.debug("atomic_output_cleanup_failed", path=str(temporary), error=str(exc))


@contextmanager
def atomic_output(path: Path) -> Iterator[Path]:
    with temporary_output(path) as temporary:
        yield temporary
        for delay in _replace_attempts(temporary, path):
            time.sleep(delay)


@asynccontextmanager
async def atomic_output_async(path: Path) -> AsyncIterator[Path]:
    with temporary_output(path) as temporary:
        yield temporary
        for delay in _replace_attempts(temporary, path):
            await asyncio.sleep(delay)
