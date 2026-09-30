from __future__ import annotations

from collections.abc import Iterator

import pytest

from agrobr.conab._custo_producao import _acquisition


@pytest.fixture(autouse=True)
def clear_catalog_cache() -> Iterator[None]:
    _acquisition.clear()
    yield
    _acquisition.clear()
