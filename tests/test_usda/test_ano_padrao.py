from __future__ import annotations

from datetime import UTC, datetime

import pytest

from agrobr import datasets, usda
from agrobr.utils import time as time_utils
from tests.helpers import sem_excecao

from .conftest import manifesto

FEVEREIRO = datetime(2026, 2, 10, 12, tzinfo=UTC)
JUNHO = datetime(2025, 6, 10, 12, tzinfo=UTC)


def _ano_ainda_sem_publicacao(gateway, ano: int) -> None:
    url = manifesto()["soja_BR_2024.json"]["url"].replace("/year/2024", f"/year/{ano}")
    gateway.rotas[url] = (200, b"[]")


def _anos_pedidos(gateway) -> list[int]:
    return [int(pedido.url.path.rsplit("/", 1)[1]) for pedido in gateway.pedidos]


async def test_sem_market_year_antes_do_wasde_de_maio_usa_o_ano_anterior(gateway, monkeypatch):
    monkeypatch.setattr(time_utils, "utcnow_aware", lambda: FEVEREIRO)
    _ano_ainda_sem_publicacao(gateway, 2026)
    gateway.servir("soja_BR_2025.json")

    with sem_excecao():
        df, meta = await usda.psd("soja", return_meta=True)

    assert not df.empty
    assert set(df["market_year"]) == {2025}
    assert _anos_pedidos(gateway) == [2026, 2025]
    assert meta.source_details["market_year"] == 2025
    assert meta.source_details["market_year_tentados"] == [2026, 2025]
    assert meta.source_details["market_year_padrao"] is True


async def test_sem_market_year_depois_do_wasde_usa_o_ano_corrente(gateway, monkeypatch):
    monkeypatch.setattr(time_utils, "utcnow_aware", lambda: JUNHO)
    gateway.servir("soja_BR_2025.json")

    with sem_excecao():
        df, meta = await usda.psd("soja", return_meta=True)

    assert set(df["market_year"]) == {2025}
    assert _anos_pedidos(gateway) == [2025]
    assert meta.source_details["market_year_tentados"] == [2025]


@pytest.mark.parametrize("market_year", [2026, 2025])
async def test_market_year_explicito_nao_recua(gateway, monkeypatch, market_year):
    monkeypatch.setattr(time_utils, "utcnow_aware", lambda: FEVEREIRO)
    _ano_ainda_sem_publicacao(gateway, 2026)
    gateway.servir("soja_BR_2025.json")

    with sem_excecao():
        _, meta = await usda.psd("soja", market_year=market_year, return_meta=True)

    assert _anos_pedidos(gateway) == [market_year]
    assert meta.source_details["market_year_padrao"] is False


async def test_oferta_demanda_global_herda_o_ano_padrao(gateway, monkeypatch):
    monkeypatch.setattr(time_utils, "utcnow_aware", lambda: FEVEREIRO)
    _ano_ainda_sem_publicacao(gateway, 2026)
    gateway.servir("soja_BR_2025.json")

    with sem_excecao():
        df, meta = await datasets.oferta_demanda_global("soja", return_meta=True)

    assert not df.empty
    assert meta.source_details["market_year"] == 2025
