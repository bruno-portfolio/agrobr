from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from typing import Any

import httpx
import pytest

from agrobr import conab, ibge, mapbiomas_alerta
from agrobr.alt.anp_diesel import api as anp_api
from agrobr.conab.ceasa import client as ceasa_client
from agrobr.exceptions import SourceUnavailableError
from agrobr.ibge import client as ibge_client
from agrobr.mapbiomas_alerta import client as alerta_client
from agrobr.utils import tasks
from tests.helpers import levanta_exatamente


class _Espiao:
    """A 1ª chamada falha depois de ceder a vez; as outras esperam 5 s e anotam se foram canceladas."""

    def __init__(self) -> None:
        self.chamadas = 0
        self.registro: list[str] = []

    async def __call__(self, *_args: Any, **_kwargs: Any) -> Any:
        self.chamadas += 1
        if self.chamadas == 1:
            await asyncio.sleep(0.05)
            raise SourceUnavailableError(source="espiao", last_error="a 1ª tarefa falhou")
        try:
            await asyncio.sleep(5)
        except asyncio.CancelledError:
            self.registro.append("cancelada")
            raise
        self.registro.append("terminou")
        raise AssertionError("a tarefa irmã não foi cancelada")


async def _anp(monkeypatch: pytest.MonkeyPatch, espiao: _Espiao) -> Any:
    async def duas_semanas(_query: dict[str, Any]) -> list[str]:
        return ["https://exemplo/semana1.xlsx", "https://exemplo/semana2.xlsx"]

    monkeypatch.setattr(anp_api, "_resolve_price_urls", duas_semanas)
    monkeypatch.setattr(anp_api.client, "fetch_precos_resource", espiao)
    return await anp_api.acquire_prices(uf="DF")


async def _sidra(
    chamada: Callable[[], Any], monkeypatch: pytest.MonkeyPatch, espiao: _Espiao
) -> Any:
    monkeypatch.setattr(ibge_client, "fetch_sidra", espiao)
    return await chamada()


async def _alerta(monkeypatch: pytest.MonkeyPatch, espiao: _Espiao) -> Any:
    monkeypatch.setattr(alerta_client, "fetch_alert_date_range", espiao)
    monkeypatch.setattr(alerta_client, "fetch_last_publication", espiao)
    return await mapbiomas_alerta.alerta_info()


PONTOS: dict[str, Callable[[pytest.MonkeyPatch, _Espiao], Any]] = {
    "anp_semanas": _anp,
    "ibge_lspa_subprodutos": lambda mp, esp: _sidra(lambda: ibge.lspa("milho", ano=2024), mp, esp),
    "ibge_censo_tabelas": lambda mp, esp: _sidra(
        lambda: ibge.censo_agro("uso_terra", ano=1995), mp, esp
    ),
    "ibge_censo_anos": lambda mp, esp: _sidra(lambda: ibge.censo_agro("preparo_solo"), mp, esp),
    "mapbiomas_alerta_info": _alerta,
}


@pytest.fixture(autouse=True)
def _sem_rede(monkeypatch: pytest.MonkeyPatch) -> None:
    async def recusar(_transporte: Any, request: httpx.Request) -> httpx.Response:
        raise AssertionError(f"pedido real no teste: {request.url}")

    monkeypatch.setattr(httpx.AsyncHTTPTransport, "handle_async_request", recusar)


@pytest.mark.parametrize("ponto", list(PONTOS))
async def test_falha_de_uma_tarefa_cancela_as_irmas(monkeypatch, ponto):
    espiao = _Espiao()
    inicio = time.monotonic()

    with levanta_exatamente(SourceUnavailableError, "a 1ª tarefa falhou"):
        await PONTOS[ponto](monkeypatch, espiao)

    assert espiao.chamadas >= 2
    assert espiao.registro == ["cancelada"] * (espiao.chamadas - 1)
    assert time.monotonic() - inicio < 2


async def test_ceasa_propaga_falha_da_unica_consulta(monkeypatch):
    espiao = _Espiao()
    monkeypatch.setattr(ceasa_client, "fetch_precos", espiao)

    with levanta_exatamente(SourceUnavailableError, "a 1ª tarefa falhou"):
        await conab.ceasa.precos()

    assert espiao.chamadas == 1
    assert espiao.registro == []


async def test_gather_or_cancel_devolve_na_ordem():
    async def valor(atraso: float, resultado: str) -> str:
        await asyncio.sleep(atraso)
        return resultado

    assert await tasks.gather_or_cancel(valor(0.02, "a"), valor(0, "b"), valor(0.01, "c")) == [
        "a",
        "b",
        "c",
    ]


async def test_gather_or_cancel_sobe_o_erro_sem_grupo_e_com_a_causa():
    async def falha() -> None:
        try:
            raise httpx.ConnectError("sem rota")
        except httpx.ConnectError as exc:
            raise SourceUnavailableError(source="x", last_error="caiu") from exc

    with levanta_exatamente(SourceUnavailableError, "caiu") as capturada:
        await tasks.gather_or_cancel(falha(), asyncio.sleep(0))

    assert isinstance(capturada.value.__cause__, httpx.ConnectError)
