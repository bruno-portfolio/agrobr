from __future__ import annotations

import time

import pytest

from agrobr.antaq import client
from agrobr.exceptions import SourceUnavailableError


async def test_intervalo_da_antaq_vale_entre_downloads(monkeypatch):
    monkeypatch.setenv("AGROBR_HTTP_RATE_LIMIT_ANTAQ", "0.3")
    instantes: list[float] = []

    def baixar(url: str) -> client._Download:
        instantes.append(time.monotonic())
        return client._Download(b"nao e zip", "text/html", url)

    monkeypatch.setattr(client, "_get_sync", baixar)
    for _ in range(2):
        with pytest.raises(SourceUnavailableError, match="not a ZIP"):
            await client._download_zip("https://estatistica.antaq.gov.br/ea/txt/2024.zip")
    assert instantes[1] - instantes[0] >= 0.29
