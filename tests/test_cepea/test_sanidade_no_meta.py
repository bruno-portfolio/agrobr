from __future__ import annotations

import warnings
from datetime import date
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from agrobr import constants
from agrobr.cache import duckdb_store
from agrobr.cepea import api, client
from tests.helpers import sem_excecao

PAGINA = (
    Path(__file__).parents[1]
    / "golden_data"
    / "cepea"
    / "cache_ttl_20260923"
    / "soja_20260923.html"
)
RESUMO = (
    "cepea: a sanidade marcou 2 de 15 linhas (excessive_change: valor; out_of_range: valor); "
    "veja a coluna anomalies"
)


@pytest.fixture
def pagina_com_preco_100x(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """A página real de 23/09 com o preço de 22/09 multiplicado por 100 (161,93 → 16.193,00)."""
    store = duckdb_store.DuckDBStore(constants.CacheSettings(cache_dir=tmp_path / "cache"))
    monkeypatch.setattr(api, "get_store", lambda: store)
    monkeypatch.setattr(duckdb_store, "get_store", lambda: store)
    monkeypatch.setattr(api, "_today", lambda: date(2026, 9, 23))
    html = PAGINA.read_bytes().decode("utf-8").replace("<td>161,93</td>", "<td>16.193,00</td>")
    pagina = client.FetchResult(html, "cepea")
    monkeypatch.setattr(api.client, "fetch_indicador_page", AsyncMock(return_value=pagina))
    yield
    store.close()


@pytest.mark.usefixtures("pagina_com_preco_100x")
@pytest.mark.parametrize("validate_sanity", [True, False])
async def test_sanidade_resume_as_linhas_marcadas_no_meta(validate_sanity):
    with warnings.catch_warnings(record=True) as emitidos, sem_excecao():
        warnings.simplefilter("always")
        frame, meta = await api.indicador(
            "soja",
            inicio="2026-09-02",
            fim="2026-09-23",
            validate_sanity=validate_sanity,
            return_meta=True,
        )

    esperado = [RESUMO] if validate_sanity else []
    resumo = [aviso for aviso in meta.validation_warnings if aviso.startswith("cepea: a sanidade")]
    assert resumo == esperado
    assert [
        str(aviso.message) for aviso in emitidos if "a sanidade" in str(aviso.message)
    ] == esperado
    assert int(frame["anomalies"].notna().sum()) == (2 if validate_sanity else 0)
