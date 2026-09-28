from __future__ import annotations

from typing import Any

import httpx
import pytest

from agrobr import datasets
from agrobr.exceptions import AgrobrError, SourceUnavailableError
from tests.helpers import levanta_exatamente


@pytest.fixture
def tudo_404(monkeypatch: pytest.MonkeyPatch) -> None:
    async def responder(_transporte: Any, request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            404, headers={"content-type": "text/html"}, content=b"<html>x</html>", request=request
        )

    monkeypatch.setattr(httpx.AsyncHTTPTransport, "handle_async_request", responder)


@pytest.mark.usefixtures("tudo_404")
async def test_erro_final_traz_as_fontes_tentadas_e_a_causa():
    with levanta_exatamente(SourceUnavailableError) as capturada:
        await datasets.producao_anual("soja", ano=2023)

    erro = capturada.value
    assert erro.attempted_sources == ["ibge_pam", "conab"]
    assert isinstance(erro.__cause__, AgrobrError)
    assert [fonte for fonte, _tipo, _texto in erro.errors] == ["ibge_pam", "conab"]
