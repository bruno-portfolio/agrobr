from __future__ import annotations

from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import duckdb
import httpx
import pytest
from typer.testing import CliRunner

from agrobr import constants
from agrobr.cache import duckdb_store
from agrobr.cli import app
from agrobr.constants import Fonte
from agrobr.exceptions import CacheMigrationError
from agrobr.health import checker, doctor
from agrobr.health.registry import HEALTH_REGISTRY, SourceHealthConfig
from agrobr.health.reporter import HealthReport


async def test_cache_migration_error_visible():
    with (
        patch.object(doctor, "get_store", side_effect=CacheMigrationError(8, "blocked")),
        patch.object(
            doctor, "_check_source", return_value=doctor.SourceStatus("CEPEA", "", "ok", 1)
        ),
    ):
        result = await doctor.run_diagnostics()
    assert result.overall_status == "error"
    assert "CacheMigrationError" in result.cache.error


@pytest.mark.parametrize("method", ["GET", "HEAD"])
async def test_probe_honors_registry_options(method):
    config = SourceHealthConfig(
        Fonte.CEPEA,
        "https://example.test",
        method=method,
        timeout=37,
        verify=False,
        follow_redirects=False,
    )
    response = httpx.Response(200, text="ok")
    with patch("httpx.AsyncClient") as factory:
        http = factory.return_value.__aenter__.return_value
        http.get.return_value = response
        http.head.return_value = response
        result = await doctor._check_source(config)
    assert result.status == "ok"
    assert factory.call_args.kwargs["timeout"] == 37
    assert factory.call_args.kwargs["verify"] is False
    getattr(http, method.lower()).assert_awaited_once_with(config.url, follow_redirects=False)


async def test_missing_credentials_not_verified_without_network(monkeypatch):
    monkeypatch.delenv("AGROBR_TEST_MISSING_KEY", raising=False)
    config = SourceHealthConfig(
        Fonte.USDA,
        "https://example.test",
        requires_api_key=True,
        api_key_env_var="AGROBR_TEST_MISSING_KEY",
    )
    with patch("httpx.AsyncClient") as factory:
        result = await doctor._check_source(config)
        verificacao = await checker._check_http(config)
    assert (result.status, result.category) == ("not_verified", "api_key_missing")
    assert verificacao.status == checker.CheckStatus.NOT_VERIFIED
    assert verificacao.message.startswith("não verificado: AGROBR_TEST_MISSING_KEY ausente")
    factory.assert_not_called()
    relatorio = HealthReport([verificacao])
    assert (relatorio.summary["ok"], relatorio.summary["not_verified"]) == (0, ["usda"])
    assert "- Not verified: usda" in relatorio.to_markdown()
    assert "? USDA: not_verified" in checker.format_results([verificacao])
    assert (
        "[NOT VERIFIED] USDA"
        in doctor.DiagnosticsResult(
            version="x",
            timestamp=verificacao.timestamp,
            sources=[result],
            cache=doctor.CacheStats(location="", size_bytes=0, total_records=0, by_source={}),
            last_collections={},
            cache_expiry={},
            config={},
            overall_status="degraded",
        ).to_rich()
    )


async def test_body_error_has_same_classification_as_health():
    config = SourceHealthConfig(Fonte.ANA, "https://example.test", body_error_markers=('"error"',))
    with patch("httpx.AsyncClient") as factory:
        factory.return_value.__aenter__.return_value.get.return_value = httpx.Response(
            200, json={"error": "unavailable"}
        )
        health = await checker._check_http(config)
        result = await doctor._check_source(config)
    assert health.status == checker.CheckStatus.FAILED
    assert result.status == "error"
    assert result.category == health.category


def test_cli_doctor_error_returns_nonzero():
    result = MagicMock(overall_status="error", sources=[])
    result.to_dict.return_value = {"overall_status": "error"}
    with patch.object(doctor, "run_diagnostics", new_callable=AsyncMock, return_value=result):
        output = CliRunner().invoke(app, ["doctor", "--json"])
    assert output.exit_code == 1


async def test_sonda_do_usda_leva_a_chave_so_no_cabecalho(monkeypatch):
    monkeypatch.setenv("AGROBR_USDA_API_KEY", "chave-de-teste")
    config = HEALTH_REGISTRY[Fonte.USDA]
    pedidos: list[httpx.Request] = []
    original = httpx.AsyncClient

    def responder(request: httpx.Request) -> httpx.Response:
        pedidos.append(request)
        return httpx.Response(200, json=[{"commodityCode": "2222000"}], request=request)

    monkeypatch.setattr(
        httpx,
        "AsyncClient",
        lambda **kwargs: original(transport=httpx.MockTransport(responder), **kwargs),
    )
    resultado = await checker._check_http(config)
    assert resultado.status == checker.CheckStatus.OK
    assert [str(p.url) for p in pedidos] == ["https://api.fas.usda.gov/api/psd/commodities"]
    assert pedidos[0].headers.get("X-Api-Key") == "chave-de-teste"
    assert "chave-de-teste" not in resultado.message + str(resultado.details)


def test_estatisticas_do_cache_leem_e_soltam_o_arquivo(tmp_path):
    store = duckdb_store.DuckDBStore(constants.CacheSettings(cache_dir=tmp_path))
    linha = {
        "produto": "soja",
        "praca": "paranagua",
        "data": datetime(2026, 9, 25),
        "valor": 140.5,
        "unidade": "BRL/sc",
        "fonte": "cepea",
    }
    assert store.indicadores_upsert([linha]) == 1

    with patch.object(doctor, "get_store", return_value=store):
        estatisticas = doctor._get_cache_stats()
        coletas = doctor._get_last_collections()

    assert estatisticas.total_records == 1
    assert estatisticas.by_source["cepea"]["newest"] == "2026-09-25"
    assert list(coletas) == ["cepea"]
    with duckdb.connect(str(store.db_path), read_only=True) as conexao:
        assert conexao.execute("SELECT count(*) FROM indicadores").fetchone() == (1,)


def test_estatisticas_com_o_cache_indisponivel(tmp_path):
    store = duckdb_store.DuckDBStore(constants.CacheSettings(cache_dir=tmp_path))

    with (
        patch.object(doctor, "get_store", return_value=store),
        patch.object(store, "_get_conn", return_value=None),
    ):
        estatisticas = doctor._get_cache_stats()
        with pytest.raises(RuntimeError, match="indisponível"):
            doctor._get_last_collections()

    assert estatisticas.status == "error"
    assert "indisponível" in estatisticas.error
