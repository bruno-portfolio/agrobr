import asyncio
import importlib
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pandas as pd

from agrobr import datasets
from agrobr.b3 import client
from agrobr.datasets.futuros_agricolas import (
    FuturosAgricolasDataset,
)
from agrobr.exceptions import InvalidParameterError, ResourceLimitError
from tests.helpers import collect_failures, isolated_dataset_case, levanta_exatamente

from .conftest import make_source, mock_source_meta

CSV_OI = (
    Path(__file__).resolve().parents[1]
    / "golden_data"
    / "b3"
    / "pregoes_20260921_20260922"
    / "b3_oi_20260922_agribusiness.csv"
)


def _mock_ajustes_df():
    return pd.DataFrame(
        {
            "data": pd.to_datetime(["2025-03-05"]),
            "ticker": ["BGI"],
            "descricao": ["BOI GORDO"],
            "vencimento_codigo": ["G25"],
            "vencimento_mes": [2],
            "vencimento_ano": [2025],
            "ajuste_anterior": [310.0],
            "ajuste_atual": [312.5],
            "variacao": [2.5],
            "ajuste_por_contrato": [312.5],
            "unidade": ["BRL/@"],
        }
    )


def _mock_posicoes_df():
    return pd.DataFrame(
        {
            "data": pd.to_datetime(["2025-03-05"]),
            "ticker": ["BGI"],
            "descricao": ["BOI GORDO"],
            "ticker_completo": ["BGIG25"],
            "vencimento_codigo": ["G25"],
            "vencimento_mes": [2],
            "vencimento_ano": [2025],
            "tipo": ["futuro"],
            "posicoes_abertas": [50000],
            "variacao_posicoes": [1200],
            "unidade": ["BRL/@"],
        }
    )


class TestFuturosSnapshot:
    async def test_futuros_snapshot_casos_1(self):
        with collect_failures() as check:
            case = "test_snapshot_sets_data_for_ajustes"
            with check(case), isolated_dataset_case(case):
                from agrobr.datasets.deterministic import deterministic

                dataset = FuturosAgricolasDataset()
                mock_fn = make_source(_mock_ajustes_df())
                dataset.info.sources[0].fetch_fn = mock_fn

                async with deterministic("2025-03-05"):
                    await dataset.fetch("boi")

                _, kwargs = mock_fn.call_args
                assert kwargs["data"] == "2025-03-05"
            case = "test_snapshot_sets_data_for_posicoes"
            with check(case), isolated_dataset_case(case):
                from agrobr.datasets.deterministic import deterministic

                dataset = FuturosAgricolasDataset()
                mock_fn = make_source(_mock_posicoes_df())
                dataset.info.sources[0].fetch_fn = mock_fn

                async with deterministic("2025-03-05"):
                    await dataset.fetch("boi", tipo="posicoes")

                _, kwargs = mock_fn.call_args
                assert kwargs["data"] == "2025-03-05"
            case = "test_snapshot_does_not_set_data_for_historico"
            with check(case), isolated_dataset_case(case):
                from agrobr.datasets.deterministic import deterministic

                dataset = FuturosAgricolasDataset()
                mock_fn = make_source(_mock_ajustes_df())
                dataset.info.sources[0].fetch_fn = mock_fn

                async with deterministic("2025-03-05"):
                    await dataset.fetch(
                        "boi", tipo="historico", inicio="2025-01-01", fim="2025-03-05"
                    )

                _, kwargs = mock_fn.call_args
                assert kwargs["data"] is None


class TestFuturosAgricolasFetchFunctions:
    async def test_futuros_agricolas_fetch_functions_casos_1(self):
        with collect_failures() as check:
            case = "test_fetch_b3_ajustes_default"
            with check(case), isolated_dataset_case(case):
                df = _mock_ajustes_df()
                meta = mock_source_meta()
                with patch(
                    "agrobr.b3.ajustes", new_callable=AsyncMock, return_value=(df, meta)
                ) as mock_fn:
                    from agrobr.datasets.futuros_agricolas import _fetch_b3

                    result_df, result_meta = await _fetch_b3("boi", data="2025-03-05")
                mock_fn.assert_called_once_with(data="2025-03-05", contrato="boi", return_meta=True)
                assert len(result_df) == 1
            case = "test_fetch_b3_historico"
            with check(case), isolated_dataset_case(case):
                df = _mock_ajustes_df()
                meta = mock_source_meta()
                with (
                    patch("agrobr.b3.ajustes", new_callable=AsyncMock, return_value=(df, meta)),
                    patch(
                        "agrobr.b3.historico", new_callable=AsyncMock, return_value=(df, meta)
                    ) as mock_fn,
                ):
                    from agrobr.datasets.futuros_agricolas import _fetch_b3

                    await _fetch_b3(
                        "boi",
                        tipo="historico",
                        inicio="2025-01-01",
                        fim="2025-03-05",
                        vencimento="G25",
                    )
                mock_fn.assert_called_once_with(
                    contrato="boi",
                    inicio="2025-01-01",
                    fim="2025-03-05",
                    vencimento="G25",
                    return_meta=True,
                )
            case = "test_fetch_b3_posicoes"
            with check(case), isolated_dataset_case(case):
                df = _mock_posicoes_df()
                meta = mock_source_meta()
                with patch(
                    "agrobr.b3.posicoes_abertas", new_callable=AsyncMock, return_value=(df, meta)
                ) as mock_fn:
                    from agrobr.datasets.futuros_agricolas import _fetch_b3

                    await _fetch_b3("boi", tipo="posicoes", data="2025-03-05")
                mock_fn.assert_called_once_with(data="2025-03-05", contrato="boi", return_meta=True)
            case = "test_fetch_b3_empty_produto"
            with check(case), isolated_dataset_case(case):
                df = _mock_ajustes_df()
                meta = mock_source_meta()
                with patch(
                    "agrobr.b3.ajustes", new_callable=AsyncMock, return_value=(df, meta)
                ) as mock_fn:
                    from agrobr.datasets.futuros_agricolas import _fetch_b3

                    await _fetch_b3("", data="2025-03-05")
                mock_fn.assert_called_once_with(data="2025-03-05", contrato=None, return_meta=True)


class TestFuturosValidateProduto:
    def test_empty_produto_allowed(self):
        dataset = FuturosAgricolasDataset()
        try:
            dataset._validate_produto("")
        except Exception as erro:
            raise AssertionError(f"produto vazio recusado: {erro!r}") from erro


class TestFuturosValidation:
    async def test_futuros_validation_casos_1(self):
        with collect_failures() as check:
            case = "test_invalid_tipo"
            with check(case), isolated_dataset_case(case):
                dataset = FuturosAgricolasDataset()

                with levanta_exatamente(ValueError, match="tipo deve ser"):
                    await dataset.fetch("boi", tipo="outro")
            case = "test_historico_requires_produto"
            with check(case), isolated_dataset_case(case):
                dataset = FuturosAgricolasDataset()

                with levanta_exatamente(ValueError, match="produto é obrigatório"):
                    await dataset.fetch(tipo="historico", inicio="2025-01-01", fim="2025-03-05")
            case = "test_historico_requires_inicio_fim"
            with check(case), isolated_dataset_case(case):
                dataset = FuturosAgricolasDataset()

                with levanta_exatamente(ValueError, match="inicio e fim"):
                    await dataset.fetch("boi", tipo="historico")
            case = "test_soja_fob_posicoes_raises"
            with check(case), isolated_dataset_case(case):
                dataset = FuturosAgricolasDataset()

                with levanta_exatamente(ValueError, match="soja_fob"):
                    await dataset.fetch("soja_fob", tipo="posicoes", data="2025-03-05")


async def test_historico_sem_periodo_explica_no_dataset():
    with levanta_exatamente(InvalidParameterError, match="obrigatórios para tipo='historico'"):
        await datasets.futuros_agricolas("boi", tipo="historico")


async def test_pregao_recente_nao_pula_dia_quando_a_vez_do_oi_passa_do_teto(monkeypatch):
    monkeypatch.setenv("AGROBR_HTTP_TIMEOUT_READ", "0.2")
    modulo = importlib.import_module("agrobr.datasets.futuros_agricolas")
    monkeypatch.setattr(
        modulo, "_dias_uteis_recentes", lambda: ["2026-09-25", "2026-09-24", "2026-09-23"]
    )
    pedido = AsyncMock(return_value=(CSV_OI.read_bytes(), "https://arquivos.b3.com.br/test"))
    monkeypatch.setattr(client, "_fetch_posicoes_abertas", pedido)
    assert client._OI_LOCK.acquire(blocking=False)
    solta = asyncio.Event()

    def soltar():
        client._OI_LOCK.release()
        solta.set()

    asyncio.get_running_loop().call_later(0.3, soltar)
    try:
        with levanta_exatamente(
            ResourceLimitError, match="outra consulta das posições em aberto segura a vez"
        ):
            await datasets.futuros_agricolas("boi", tipo="posicoes")
    finally:
        await solta.wait()
    assert pedido.await_args_list == []
