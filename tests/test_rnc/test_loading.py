from __future__ import annotations

import csv
import io
from pathlib import Path
from unittest.mock import AsyncMock, Mock

import pytest

from agrobr import rnc
from agrobr.exceptions import InvalidParameterError, ParseError
from agrobr.rnc import client, loading, snapshot
from tests.helpers import levanta_exatamente, rnc_csv_acquisition, sem_excecao

GOLDEN = Path(__file__).parents[1] / "golden_data/rnc/selecao_20260907"


@pytest.fixture(autouse=True)
def isolated_cache(tmp_path, monkeypatch):
    monkeypatch.setattr(snapshot, "cache_dir", lambda: tmp_path)
    return tmp_path


def _csv(kind="registradas"):
    return (GOLDEN / f"{kind}.csv").read_bytes()


def _invalid_unselected_row():
    rows = list(csv.reader(io.StringIO(_csv().decode("utf-8-sig"))))
    identity = rows[1][rows[0].index("Nº REGISTRO")]
    rows[-1][rows[0].index("DATA DO REGISTRO")] = "31/02/2026"
    output = io.StringIO()
    csv.writer(output).writerows(rows)
    return output.getvalue().encode("utf-8"), identity


@pytest.mark.parametrize("kind", ["registradas", "protegidas"])
@pytest.mark.asyncio
async def test_unknown_source_parameter_fails_before_cache_or_http(kind, monkeypatch):
    load = AsyncMock()
    monkeypatch.setattr(loading, "load", load)
    with levanta_exatamente(InvalidParameterError, match="Parâmetros desconhecidos"):
        await getattr(rnc, kind)(data_inexistente="2026")
    load.assert_not_called()


@pytest.mark.parametrize("flag", ["use_cache", "as_polars", "return_meta"])
@pytest.mark.asyncio
async def test_non_boolean_source_flag_fails_before_cache_or_http(flag, monkeypatch):
    load = AsyncMock()
    monkeypatch.setattr(loading, "load", load)
    with levanta_exatamente(InvalidParameterError, match=f"{flag} deve ser booleano"):
        await rnc.registradas(**{flag: 1})
    load.assert_not_called()


@pytest.mark.parametrize(
    ("kind", "field"),
    [
        ("registradas", "nr_registro"),
        ("registradas", "nr_formulario"),
        ("protegidas", "nr_processo"),
        ("protegidas", "nr_certificado"),
    ],
)
@pytest.mark.asyncio
async def test_identifier_filter_is_exact_and_empty_selection_keeps_schema(
    kind, field, monkeypatch
):
    captured = rnc_csv_acquisition(_csv(kind), kind)
    monkeypatch.setattr(client, f"fetch_{kind}_bundle", AsyncMock(return_value=captured))
    fetch = getattr(rnc, kind)
    full = await fetch(use_cache=False)
    identifiers = set(full[field])
    value = next(v for v in full[field] if len(v) > 3 and v[1:-1] not in identifiers)
    exact = await fetch(**{field: f" {value} "}, use_cache=False)
    assert exact[field].tolist() == [value] * int(full[field].eq(value).sum())
    partial = await fetch(**{field: value[1:-1]}, use_cache=False)
    assert partial.empty and partial.dtypes.equals(full.dtypes)


@pytest.mark.asyncio
async def test_acquisition_of_other_family_is_rejected(isolated_cache, monkeypatch):
    captured = rnc_csv_acquisition(_csv("protegidas"), "protegidas")
    monkeypatch.setattr(client, "fetch_registradas_bundle", AsyncMock(return_value=captured))
    with levanta_exatamente(ParseError, match="Família da aquisição diferente"):
        await rnc.registradas()
    assert not list(isolated_cache.iterdir())


@pytest.mark.parametrize("kind", ["registradas", "protegidas"])
@pytest.mark.asyncio
async def test_search_total_mismatch_aborts_before_cache(kind, isolated_cache, monkeypatch):
    captured = rnc_csv_acquisition(_csv(kind), kind, reported_total=1)
    fetch = AsyncMock(return_value=captured)
    monkeypatch.setattr(client, f"fetch_{kind}_bundle", fetch)
    with levanta_exatamente(ParseError, match="Total da pesquisa 1 incompatível"):
        await getattr(rnc, kind)(cultivar="no-match", return_meta=False)
    assert not list(isolated_cache.iterdir())


@pytest.mark.asyncio
async def test_missing_search_total_keeps_coverage_unknown(monkeypatch):
    captured = rnc_csv_acquisition(_csv(), "registradas", reported_total=None)
    monkeypatch.setattr(client, "fetch_registradas_bundle", AsyncMock(return_value=captured))
    frame, meta = await rnc.registradas(return_meta=True)
    assert len(frame) == 29
    assert meta.source_details["coverage"]["status"] == "unknown"
    assert meta.source_details["coverage"]["reported_total"] is None
    assert meta.source_details["coverage"]["transactional_snapshot"] is False


@pytest.mark.asyncio
async def test_cache_population_is_revalidated_before_filtered_return(monkeypatch):
    invalid, identity = _invalid_unselected_row()
    snapshot.write_acquisition(rnc_csv_acquisition(invalid, "registradas"))
    fresh = rnc_csv_acquisition(_csv(), "registradas", reported_total=29)
    fetch = AsyncMock(return_value=fresh)
    monkeypatch.setattr(client, "fetch_registradas_bundle", fetch)
    with sem_excecao():
        frame, meta = await rnc.registradas(nr_registro=identity, return_meta=True)
    assert frame["nr_registro"].tolist() == [identity]
    fetch.assert_awaited_once()
    assert not meta.from_cache and meta.raw_content_hash == fresh.resource.sha256
    restored = snapshot.read_acquisition("registradas")
    assert restored is not None and restored.content == _csv()


@pytest.mark.asyncio
async def test_cache_write_failure_keeps_valid_data_and_honest_metadata(
    isolated_cache, monkeypatch
):
    captured = rnc_csv_acquisition(_csv(), "registradas")
    monkeypatch.setattr(client, "fetch_registradas_bundle", AsyncMock(return_value=captured))
    monkeypatch.setattr(snapshot, "write_acquisition", Mock(side_effect=OSError("disk full")))
    with sem_excecao():
        frame, meta = await rnc.registradas(return_meta=True)
    assert len(frame) == 29 and not meta.from_cache
    assert meta.cache_key is None and meta.cache_expires_at is None
    assert meta.source_details["cache_status"] == "write_failed"
    assert meta.raw_content_hash == captured.resource.sha256
    assert not list(isolated_cache.iterdir())


@pytest.mark.asyncio
async def test_bypass_skips_both_cache_read_and_write(monkeypatch):
    captured = rnc_csv_acquisition(_csv(), "registradas")
    monkeypatch.setattr(client, "fetch_registradas_bundle", AsyncMock(return_value=captured))
    read = Mock(side_effect=AssertionError("cache read during bypass"))
    write = Mock(side_effect=AssertionError("cache write during bypass"))
    monkeypatch.setattr(snapshot, "read_acquisition", read)
    monkeypatch.setattr(snapshot, "write_acquisition", write)
    _, meta = await rnc.registradas(use_cache=False, return_meta=True)
    read.assert_not_called()
    write.assert_not_called()
    assert meta.source_details["cache_status"] == "bypassed"
    assert meta.cache_key is None and meta.cache_expires_at is None
