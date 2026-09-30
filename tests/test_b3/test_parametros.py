from __future__ import annotations

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from agrobr import contracts
from agrobr.b3 import api, client
from agrobr.exceptions import InvalidParameterError
from tests.test_b3.test_api import _golden_oi_csv, _make_empty_zip_fixture, _make_zip_fixture

URL_ZIP = "https://www.b3.com.br/pesquisapregao/download?filelist=PR250213.zip"
PERIODO = {"inicio": "2025-02-10", "fim": "2025-02-14"}


@pytest.fixture
def rede():
    with (
        patch.object(
            client,
            "fetch_ajustes_zip",
            new_callable=AsyncMock,
            return_value=(_make_zip_fixture(), URL_ZIP),
        ) as zip_,
        patch.object(
            client,
            "fetch_posicoes_abertas",
            new_callable=AsyncMock,
            return_value=(_golden_oi_csv(), "https://arquivos.b3.com.br/test"),
        ) as oi,
    ):
        yield zip_, oi


CHAMADAS = {
    "ajustes": lambda **kw: api.ajustes(data="2025-02-13", **kw),
    "posicoes_abertas": lambda **kw: api.posicoes_abertas(data="2025-12-19", **kw),
    "historico": lambda **kw: api.historico(**PERIODO, **kw),
    "oi_historico": lambda **kw: api.oi_historico(**PERIODO, **kw),
}


@pytest.mark.parametrize("funcao", CHAMADAS)
@pytest.mark.parametrize("contrato", ["XYZ", "boi_gordo", 5])
async def test_contrato_fora_da_lista_e_recusado_antes_da_rede(rede, funcao, contrato):
    with pytest.raises(
        InvalidParameterError, match=r"contrato .* inválido\. Valores válidos: boi, "
    ):
        await CHAMADAS[funcao](contrato=contrato)
    assert all(mock.await_count == 0 for mock in rede)


@pytest.mark.usefixtures("rede")
@pytest.mark.parametrize("contrato", ["milho", "Milho", "CCM", "ccm"])
async def test_contrato_aceita_nome_ou_ticker_sem_caixa(contrato):
    df = await api.ajustes(data="13/02/2025", contrato=contrato)
    assert set(df["ticker"]) == {"CCM"}


@pytest.mark.parametrize(
    "chamada",
    [
        lambda: api.ajustes(data="21-09-2026"),
        lambda: api.posicoes_abertas(data="2026/09/21"),
        lambda: api.historico(contrato="boi", inicio="21-09-2026", fim="2026-09-22"),
        lambda: api.oi_historico(contrato="boi", inicio="2026-09-21", fim="22.09.2026"),
    ],
    ids=["ajustes", "posicoes_abertas", "historico", "oi_historico"],
)
async def test_data_fora_do_formato_e_recusada_antes_da_rede(rede, chamada):
    with pytest.raises(InvalidParameterError, match="AAAA-MM-DD ou DD/MM/AAAA"):
        await chamada()
    assert all(mock.await_count == 0 for mock in rede)


async def test_data_em_dd_mm_aaaa_vale_em_toda_a_familia(rede):
    zip_, oi = rede
    await api.posicoes_abertas(data="19/12/2025")
    assert oi.await_args.args == ("2025-12-19",)
    await api.historico(contrato="milho", inicio="13/02/2025", fim="13/02/2025")
    assert zip_.await_args.args == ("13/02/2025",)


@pytest.mark.parametrize("funcao", ["historico", "oi_historico"])
async def test_inicio_depois_de_fim_e_recusado_antes_da_rede(rede, funcao):
    chamada = api.historico if funcao == "historico" else api.oi_historico
    with pytest.raises(InvalidParameterError, match=r"inicio \(2025-02-14\) posterior a fim"):
        await chamada(contrato="boi", inicio="2025-02-14", fim="2025-02-10")
    assert all(mock.await_count == 0 for mock in rede)


@pytest.mark.parametrize(
    "chamada",
    [
        lambda: api.historico(contrato="boi", **PERIODO, vencimento="xx"),
        lambda: api.posicoes_abertas(data="2025-12-19", tipo="futuros"),
        lambda: api.oi_historico(contrato="boi", **PERIODO, tipo="call"),
    ],
    ids=["vencimento_historico", "tipo_posicoes", "tipo_oi_historico"],
)
async def test_vencimento_e_tipo_invalidos_sao_recusados_antes_da_rede(rede, chamada):
    with pytest.raises(InvalidParameterError, match="inválido"):
        await chamada()
    assert all(mock.await_count == 0 for mock in rede)


async def test_historico_limita_os_dias_abertos_ao_mesmo_tempo(monkeypatch):
    monkeypatch.setenv("AGROBR_HTTP_MAX_CONCURRENT_B3", "2")
    abertos = pico = 0

    async def ajustes_do_dia(**_kwargs):
        nonlocal abertos, pico
        abertos += 1
        pico = max(pico, abertos)
        await asyncio.sleep(0.01)
        abertos -= 1
        return contracts.get_contract("ajuste_diario").empty_frame(), api.build_source_meta(
            "b3", URL_ZIP, "httpx+zip+xml", 0, 0, pd.DataFrame(), 1
        )

    monkeypatch.setattr(api, "ajustes", ajustes_do_dia)
    await api.historico(contrato="boi", inicio="2025-02-03", fim="2025-02-14")
    assert pico == 2


PREGOES = Path(__file__).resolve().parents[1] / "golden_data" / "b3" / "pregoes_20260921_20260922"


def _zip_oficial(data: str) -> tuple[bytes, str]:
    return (
        PREGOES / f"b3_ajustes_PR{data[8:10]}{data[3:5]}{data[:2]}_recorte.zip"
    ).read_bytes(), URL_ZIP


def _csv_oficial(data: str) -> tuple[bytes, str]:
    arquivo = PREGOES / f"b3_oi_{data.replace('-', '')}_agribusiness.csv"
    return arquivo.read_bytes(), "https://arquivos.b3.com.br/test"


@pytest.fixture
def capturas_oficiais():
    with (
        patch.object(client, "fetch_ajustes_zip", new_callable=AsyncMock, side_effect=_zip_oficial),
        patch.object(
            client, "fetch_posicoes_abertas", new_callable=AsyncMock, side_effect=_csv_oficial
        ),
    ):
        yield


DIAS = {"inicio": "2026-09-21", "fim": "2026-09-22"}


@pytest.mark.usefixtures("capturas_oficiais")
@pytest.mark.parametrize(
    ("contrato", "cheio", "vazio"),
    [
        (
            "ajuste_diario",
            lambda: api.historico(contrato="boi", **DIAS),
            lambda: api.historico(contrato="boi", **DIAS, vencimento="Z30"),
        ),
        (
            "posicoes_abertas",
            lambda: api.posicoes_abertas(data="2026-09-21"),
            lambda: api.posicoes_abertas(data="2026-09-21", contrato="cafe_conillon"),
        ),
        (
            "posicoes_abertas",
            lambda: api.oi_historico(contrato="boi", **DIAS),
            lambda: api.oi_historico(contrato="cafe_conillon", **DIAS),
        ),
    ],
    ids=["historico", "posicoes_abertas", "oi_historico"],
)
async def test_cheio_e_vazio_saem_com_os_dtypes_do_contrato(contrato, cheio, vazio):
    esperado = contracts.get_contract(contrato).empty_frame().dtypes.to_dict()
    com_linhas, sem_linhas = await cheio(), await vazio()
    assert len(com_linhas) > 0
    assert sem_linhas.empty
    assert com_linhas.dtypes.to_dict() == esperado
    assert sem_linhas.dtypes.to_dict() == esperado


async def test_ajustes_com_pregao_e_sem_pregao_saem_com_os_dtypes_do_contrato():
    esperado = contracts.get_contract("ajuste_diario").empty_frame().dtypes.to_dict()
    with patch.object(
        client, "fetch_ajustes_zip", new_callable=AsyncMock, side_effect=_zip_oficial
    ):
        com_pregao = await api.ajustes(data="2026-09-22")
    vazio = (_make_empty_zip_fixture(), URL_ZIP)
    with patch.object(client, "fetch_ajustes_zip", new_callable=AsyncMock, return_value=vazio):
        sem_pregao = await api.ajustes(data="2026-09-20")
    assert len(com_pregao) > 0
    assert sem_pregao.empty
    assert com_pregao.dtypes.to_dict() == esperado
    assert sem_pregao.dtypes.to_dict() == esperado
