from __future__ import annotations

from datetime import date

import httpx
import pytest

from agrobr import cftc
from agrobr.cftc import client
from agrobr.exceptions import InvalidParameterError
from tests.helpers import levanta_exatamente


@pytest.mark.parametrize(
    ("start", "end"),
    [("2026-06-01", "2026-01-01"), (date(2026, 6, 1), "2026-05-31")],
    ids=["texto", "date_e_texto"],
)
async def test_start_depois_de_end_e_recusado_antes_da_rede(monkeypatch, start, end):
    def sem_rede(**_kwargs: object) -> httpx.AsyncClient:
        raise AssertionError("não deveria abrir conexão")

    monkeypatch.setattr(client.httpx, "AsyncClient", sem_rede)
    with levanta_exatamente(InvalidParameterError, match="posterior a end"):
        await cftc.cot("soja", start=start, end=end)
