from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest


@pytest.fixture
def isolated_cache(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    cache_dir = tmp_path / "agrobr_cache"
    cache_dir.mkdir()
    monkeypatch.setenv("AGROBR_CACHE_CACHE_DIR", str(cache_dir))
    yield cache_dir


@pytest.fixture(autouse=True)
def _reset_acervo_state() -> Iterator[None]:
    from agrobr.acervo_fundiario import client
    from agrobr.utils.warnings import warn_once_reset

    client._FETCH_LOCKS.clear()
    warn_once_reset()
    yield
    client._FETCH_LOCKS.clear()
    warn_once_reset()
