"""Pytest configuration and fixtures."""

from __future__ import annotations

import hashlib
import socket
import sys
from pathlib import Path

import pytest

from tests.helpers import local_socketpair

try:
    import _duckdb

    sys.modules.setdefault("_duckdb._sqltypes", _duckdb._sqltypes)
    sys.modules.setdefault("_duckdb._func", _duckdb._func)
except (ImportError, AttributeError):
    pass


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--live-matrix-report",
        type=Path,
        help="Grava cobertura e resultados da matriz de datasets live em JSON",
    )


def pytest_configure(config: pytest.Config) -> None:
    if sys.platform == "win32":
        monkeypatch = pytest.MonkeyPatch()
        monkeypatch.setattr(socket, "socketpair", local_socketpair)
        config.add_cleanup(monkeypatch.undo)


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    for item in items:
        if item.get_closest_marker("integration") is not None:
            item.add_marker(pytest.mark.enable_socket)


@pytest.fixture(scope="session")
def _cache_root(tmp_path_factory):
    return tmp_path_factory.mktemp("agrobr-cache")


@pytest.fixture(autouse=True)
def _isolated_duckdb_cache(_cache_root, monkeypatch, request):
    from agrobr.cache import duckdb_store

    key = hashlib.sha256(request.node.nodeid.encode()).hexdigest()[:16]
    monkeypatch.delenv("AGROBR_CACHE_DIR", raising=False)
    monkeypatch.setenv("AGROBR_CACHE_CACHE_DIR", str(_cache_root / key))
    monkeypatch.setattr(duckdb_store, "_store", None)
    yield
    if duckdb_store._store is not None:
        duckdb_store._store.close()


@pytest.fixture(autouse=True)
def _fast_retry(monkeypatch, request):
    if any(request.node.get_closest_marker(marker) for marker in ("benchmark", "integration")):
        return
    monkeypatch.setenv("AGROBR_HTTP_RETRY_BASE_DELAY", "0.001")
    monkeypatch.setenv("AGROBR_HTTP_RETRY_MAX_DELAY", "0.01")
    from agrobr import constants

    for field in constants.HTTPSettings.model_fields:
        if field.startswith("rate_limit_"):
            monkeypatch.setenv(f"AGROBR_HTTP_{field.upper()}", "0.001")


@pytest.fixture
def serie_historica():
    """Liga a série histórica do CEPEA, que os testes desligam por padrão: sem rede, ela só avisaria."""


@pytest.fixture(autouse=True)
def _sem_serie_historica_do_cepea(monkeypatch, request):
    if "serie_historica" in request.fixturenames or request.node.get_closest_marker("integration"):
        return
    from agrobr.cepea import api

    monkeypatch.setattr(api, "_precisa_da_serie", lambda *_args: False)


@pytest.fixture(autouse=True)
def _reset_global_state():
    yield
    import structlog

    from agrobr.config import reset_config
    from agrobr.http.rate_limiter import RateLimiter

    reset_config()
    RateLimiter.reset()

    from agrobr.utils.warnings import warn_once_reset

    warn_once_reset()
    structlog.reset_defaults()


@pytest.fixture
def sample_html_cepea() -> str:
    """HTML mínimo para testes de parsing CEPEA."""
    return """
    <html>
    <head><title>CEPEA - Indicador</title></head>
    <body>
        <div id="content">
            <table class="indicador" id="imagenet-indicador1">
                <tr>
                    <th></th>
                    <th>Valor R$*</th>
                    <th>Var./Dia</th>
                    <th>Var./Mês</th>
                    <th>Valor US$*</th>
                </tr>
                <tr>
                    <td>01/02/2024</td>
                    <td>145,50</td>
                    <td>+0,5%</td>
                    <td>+1,2%</td>
                    <td>28,50</td>
                </tr>
                <tr>
                    <td>31/01/2024</td>
                    <td>144,78</td>
                    <td>-0,3%</td>
                    <td>-0,1%</td>
                    <td>28,30</td>
                </tr>
            </table>
        </div>
        <p>Indicador CEPEA/ESALQ</p>
    </body>
    </html>
    """


@pytest.fixture
def sample_html_empty() -> str:
    """HTML sem tabelas para testar erros."""
    return "<html><body><p>No data available</p></body></html>"
