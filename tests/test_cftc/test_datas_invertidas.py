from __future__ import annotations

from datetime import date, datetime
from unittest.mock import AsyncMock

import httpx
import pytest

from agrobr import cftc, datasets
from agrobr.cftc import client
from agrobr.cftc.models import resolve_contract_codes
from agrobr.exceptions import InvalidParameterError
from tests.helpers import levanta_exatamente

CHAMADAS = {
    "fonte": lambda produto, **kw: cftc.cot(produto, **kw),
    "dataset": lambda produto, **kw: datasets.posicionamento_fundos(produto, **kw),
}


@pytest.fixture
def sem_rede(monkeypatch):
    def recusar(**_kwargs: object) -> httpx.AsyncClient:
        raise AssertionError("não deveria abrir conexão")

    monkeypatch.setattr(client.httpx, "AsyncClient", recusar)


@pytest.mark.usefixtures("sem_rede")
@pytest.mark.parametrize("chamada", CHAMADAS)
@pytest.mark.parametrize(
    ("inicio", "fim"),
    [("2026-06-01", "2026-01-01"), (date(2026, 6, 1), "31/05/2026")],
    ids=["texto", "date_e_dd_mm"],
)
async def test_inicio_depois_de_fim_e_recusado_antes_da_rede(chamada, inicio, fim):
    with levanta_exatamente(InvalidParameterError, match="posterior a fim"):
        await CHAMADAS[chamada]("soja", inicio=inicio, fim=fim)


@pytest.mark.usefixtures("sem_rede")
@pytest.mark.parametrize("chamada", CHAMADAS)
async def test_data_fora_do_formato_e_recusada_antes_da_rede(chamada):
    with levanta_exatamente(InvalidParameterError, match="AAAA-MM-DD ou DD/MM/AAAA"):
        await CHAMADAS[chamada]("soja", inicio="01-06-2026")


@pytest.mark.usefixtures("sem_rede")
@pytest.mark.parametrize("produto", ["xx", 5])
async def test_produto_sem_contrato_e_recusado_com_a_lista(produto):
    with levanta_exatamente(InvalidParameterError, match=r"Valores válidos: acucar, algodao"):
        await cftc.cot(produto)
    with levanta_exatamente(InvalidParameterError, match=r"Valores válidos: acucar, algodao"):
        resolve_contract_codes(produto)


async def test_date_datetime_e_dd_mm_viram_o_filtro_iso(monkeypatch):
    buscar = AsyncMock(side_effect=RuntimeError("parar depois do filtro"))
    monkeypatch.setattr(client, "fetch_cot", buscar)
    with pytest.raises(RuntimeError):
        await cftc.cot("soja", inicio="01/06/2026", fim=datetime(2026, 6, 30, 15, 0))
    assert buscar.await_args.kwargs["start"] == date(2026, 6, 1)
    assert buscar.await_args.kwargs["end"] == date(2026, 6, 30)
