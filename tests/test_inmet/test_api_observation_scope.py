from __future__ import annotations

import warnings
from datetime import date
from unittest.mock import AsyncMock

import pytest

from agrobr import inmet
from agrobr.exceptions import ParseError
from agrobr.inmet import client
from tests.helpers import levanta_exatamente


def observation(**changes):
    return {
        "CD_ESTACAO": "A001",
        "DT_MEDICAO": "2024-01-01",
        "HR_MEDICAO": "1200 UTC",
        "UF": "DF",
        "TEM_INS": "25",
        "TEM_MAX": "27",
        "TEM_MIN": "20",
        "CHUVA": "4",
        **changes,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes",
    [
        {"CD_ESTACAO": "A002"},
        {"DT_MEDICAO": "2023-12-31"},
        {"DT_MEDICAO": "2024-01-03"},
        {"DT_MEDICAO": "2024-02-30"},
        {"DT_MEDICAO": "2024/01/01"},
        {"DT_MEDICAO": 1704067200},
        {"CD_ESTACAO": None},
    ],
)
async def test_estacao_rejeita_payload_divergente(monkeypatch, changes):
    monkeypatch.setattr(
        client, "fetch_dados_estacao", AsyncMock(return_value=[observation(**changes)])
    )
    with pytest.raises(ParseError):
        await inmet.estacao("A001", "2024-01-01", "2024-01-02")


@pytest.mark.asyncio
@pytest.mark.parametrize("changes", [{"UF": "SP"}, {"DT_MEDICAO": "2025-01-01"}, {"UF": None}])
async def test_uf_rejeita_payload_divergente(monkeypatch, changes):
    monkeypatch.setattr(
        client, "fetch_dados_estacoes_uf", AsyncMock(return_value=[observation(**changes)])
    )
    with pytest.raises(ParseError):
        await inmet.clima_uf("DF", 2024)


@pytest.mark.asyncio
async def test_metadata_api_estacao_independe_dataset(monkeypatch):
    monkeypatch.setattr(client, "fetch_dados_estacao", AsyncMock(return_value=[observation()]))
    data, meta = await inmet.estacao("a001", "2024-01-01", "2024-01-02", return_meta=True)
    assert data.estacao.eq("A001").all()
    assert meta.source_details == {
        "access": "inmet_api",
        "time_basis": "UTC",
        "requested_period": {"inicio": "2024-01-01", "fim": "2024-01-02"},
        "temporal_aggregation": "horario",
        "spatial_aggregation": {},
        "station_selection": {"mode": "codigo", "codigo": "A001"},
    }


@pytest.mark.asyncio
async def test_metadata_api_uf_documenta_selecao_e_formulas(monkeypatch):
    monkeypatch.setattr(client, "fetch_dados_estacoes_uf", AsyncMock(return_value=[observation()]))
    with warnings.catch_warnings(record=True) as avisos:
        warnings.simplefilter("always")
        _, meta = await inmet.clima_uf("df", 2024, return_meta=True)
    chuva = [str(aviso.message) for aviso in avisos if "Chuva mensal" in str(aviso.message)]
    assert [mensagem.split(":")[0] for mensagem in chuva] == ["Chuva mensal nula em DF 2024-01"]
    assert meta.validation_warnings == chuva
    assert meta.source_details["time_basis"] == "UTC"
    assert meta.source_details["station_selection"]["situacao"] == "Operante"
    assert meta.source_details["station_selection"]["returned_stations"] == ["A001"]
    assert (
        meta.source_details["spatial_aggregation"]["precip_acum_mm"]
        == "mean_of_complete_station_monthly_totals"
    )


@pytest.mark.asyncio
async def test_cliente_valida_intervalo_do_bloco(monkeypatch):
    monkeypatch.setattr(client, "_get_json", AsyncMock(side_effect=[[], [observation()]]))
    with pytest.raises(ParseError, match="fora do intervalo"):
        await client.fetch_dados_estacao("A001", date(2024, 1, 1), date(2025, 1, 1))


@pytest.mark.asyncio
async def test_uf_desembrulha_erro_de_layout_da_estacao(monkeypatch):
    monkeypatch.setattr(client, "_get_token", lambda: "synthetic-token")
    monkeypatch.setattr(
        client,
        "fetch_estacoes",
        AsyncMock(
            return_value=[{"CD_ESTACAO": "A001", "SG_ESTADO": "DF", "CD_SITUACAO": "Operante"}]
        ),
    )
    monkeypatch.setattr(
        client,
        "fetch_dados_estacao",
        AsyncMock(
            side_effect=ParseError(source="inmet", parser_version=1, reason="falha de identidade")
        ),
    )
    with levanta_exatamente(ParseError, "falha de identidade"):
        await client.fetch_dados_estacoes_uf("DF", date(2024, 1, 1), date(2024, 1, 2))


@pytest.mark.asyncio
async def test_uf_sem_chuva_na_api_nao_conta_estacoes_nem_avisa(monkeypatch):
    sem_chuva = {chave: valor for chave, valor in observation().items() if chave != "CHUVA"}
    monkeypatch.setattr(client, "fetch_dados_estacoes_uf", AsyncMock(return_value=[sem_chuva]))
    with warnings.catch_warnings(record=True) as avisos:
        warnings.simplefilter("always")
        mensal, meta = await inmet.clima_uf("df", 2024, return_meta=True)
    assert "estacoes_chuva" not in mensal.columns
    assert [str(aviso.message) for aviso in avisos if "Chuva mensal" in str(aviso.message)] == []
    assert meta.validation_warnings == []
