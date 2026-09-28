from __future__ import annotations

import hashlib
import json
from datetime import UTC, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, Mock
from urllib.parse import parse_qs

import httpx
import pandas as pd
import pytest

from agrobr import contracts, datasets, rnc
from agrobr.datasets.deterministic import deterministic
from agrobr.exceptions import ContractViolationError, InvalidParameterError
from agrobr.rnc import client, snapshot
from tests.helpers import levanta_exatamente

FILTERS = {
    "registradas": [
        "cultivar",
        "especie",
        "grupo",
        "situacao",
        "mantenedor",
        "nr_registro",
        "nr_formulario",
    ],
    "protegidas": ["cultivar", "especie", "situacao", "titular", "nr_processo", "nr_certificado"],
}


GOLDEN = Path(__file__).parents[1] / "golden_data/rnc/selecao_20260907"
FAMILIES = ["registradas", "protegidas"]
DATE_FIELDS = {"data_registro", "data_validade", "inicio_protecao", "termino_protecao"}


@pytest.fixture
def cultivar_http(monkeypatch, tmp_path):
    original = httpx.AsyncClient
    bodies = {kind: (GOLDEN / f"{kind}.csv").read_bytes() for kind in FAMILIES}
    calls = []
    tokens = dict.fromkeys(FAMILIES, 0)
    expected = json.loads((GOLDEN / "expected.json").read_text(encoding="utf-8"))
    monkeypatch.setattr(snapshot, "cache_dir", lambda: tmp_path)
    monkeypatch.setattr(client, "MIN_CSV_SIZE", 1)

    def handler(request):
        kind = next(kind for kind in FAMILIES if f"cultivares_{kind}.php" in request.url.path)
        calls.append(request)
        if request.method == "GET":
            tokens[kind] += 1
            body = (
                (GOLDEN / f"{kind}_form.html")
                .read_bytes()
                .replace(b"REDACTED_CSRF_TOKEN", f"synthetic-{kind}-{tokens[kind]}".encode())
            )
            return httpx.Response(200, content=body, headers={"content-type": "text/html"})
        assert request.method == "POST"
        form = parse_qs(request.content.decode())
        assert form["csrf_token"] == [f"synthetic-{kind}-{tokens[kind]}"]
        if form.get("exportar") == ["csv"]:
            return httpx.Response(
                200, content=bodies[kind], headers={"content-type": "text/csv; charset=UTF-8"}
            )
        assert form["acao"] == ["Pesquisar"]
        return httpx.Response(
            200,
            content=(GOLDEN / f"{kind}_search_subset.html").read_bytes(),
            headers={"content-type": "text/html"},
        )

    monkeypatch.setattr(
        httpx,
        "AsyncClient",
        lambda *args, **kwargs: original(
            *args, **{**kwargs, "transport": httpx.MockTransport(handler)}
        ),
    )
    return {"calls": calls, "bodies": bodies, "expected": expected, "cache": tmp_path}


@pytest.mark.parametrize("kind", FAMILIES)
async def test_official_replay_compares_all_cells_through_public_dataset(kind, cultivar_http):
    name = f"cultivares_{kind}"
    frame, meta = await getattr(datasets, name)(use_cache=False, return_meta=True)
    expected = cultivar_http["expected"][kind]
    assert frame.columns.tolist() == expected["columns"]
    assert len(frame) == len(expected["rows"])
    for actual, published in zip(frame.to_dict("records"), expected["rows"], strict=True):
        for column, value in published.items():
            if column in DATE_FIELDS:
                assert (
                    pd.isna(actual[column])
                    if value is None
                    else actual[column] == pd.Timestamp(value)
                )
            else:
                assert actual[column] == value
    assert contracts.get_contract(f"rnc_{kind}").validate(frame) == (True, [])
    assert meta.dataset == name and meta.source == f"datasets.{name}/rnc_{kind}"
    assert meta.selected_source == f"rnc_{kind}" and meta.attempted_sources == [f"rnc_{kind}"]
    assert meta.schema_version == meta.contract_version == "1.0" and meta.parser_version == 2
    assert meta.source_method == "dataset" and meta.snapshot is None and not meta.from_cache
    assert meta.columns == expected["columns"] and meta.records_count == len(frame)
    assert meta.raw_content_hash == hashlib.sha256(cultivar_http["bodies"][kind]).hexdigest()
    assert meta.raw_content_size == len(cultivar_http["bodies"][kind])
    assert meta.fetched_at.utcoffset() == UTC.utcoffset(None)
    assert meta.fetch_timestamp.utcoffset() == timedelta(0)
    assert meta.cache_key is None and meta.cache_expires_at is None
    details = meta.source_details
    resource = details["acquisition"]["resource"]
    assert resource["sha256"] == meta.raw_content_hash
    assert resource["size_bytes"] == meta.raw_content_size
    assert resource["method"] == "POST" and resource["status_code"] == 200
    assert pd.Timestamp(resource["received_at"]) == meta.fetched_at
    assert details["acquisition"]["kind"] == kind
    assert details["acquisition"]["search"]["reported_total"] == len(frame)
    assert details["parser"]["source_rows"] == len(frame)
    assert details["selection"] == {"filters": {}, "output_rows": len(frame)}
    assert details["coverage"]["status"] == "count_matched"
    assert details["coverage"]["source_rows"] == len(frame)
    assert details["coverage"]["transactional_snapshot"] is False
    assert "csrf_token" not in json.dumps(details)
    assert len(cultivar_http["calls"]) == 4
    assert not list(cultivar_http["cache"].iterdir())
    assert all(
        str(frame[column].dtype) == "datetime64[ns]" for column in frame if column in DATE_FIELDS
    )


@pytest.mark.parametrize("kind", FAMILIES)
@pytest.mark.parametrize("flag", ["use_cache", "as_polars", "return_meta"])
@pytest.mark.parametrize("value", [0, 1, "true", None])
async def test_flags_fail_before_cache_and_http(kind, flag, value, cultivar_http, monkeypatch):
    reader = Mock(side_effect=AssertionError("cache accessed"))
    monkeypatch.setattr(snapshot, "read_acquisition", reader)
    with pytest.raises(InvalidParameterError):
        await getattr(datasets, f"cultivares_{kind}")(**{flag: value})
    reader.assert_not_called()
    assert cultivar_http["calls"] == []


@pytest.mark.parametrize("kind", FAMILIES)
async def test_deterministic_fails_before_cache_and_http(kind, cultivar_http, monkeypatch):
    reader = Mock(side_effect=AssertionError("cache accessed"))
    monkeypatch.setattr(snapshot, "read_acquisition", reader)
    async with deterministic("2025-01-01"):
        with pytest.raises(InvalidParameterError, match="deterministic"):
            await getattr(datasets, f"cultivares_{kind}")()
    reader.assert_not_called()
    assert cultivar_http["calls"] == []


@pytest.mark.parametrize("kind", FAMILIES)
@pytest.mark.parametrize("return_meta", [False, True])
async def test_dataset_revalidates_the_source_result(kind, return_meta, cultivar_http, monkeypatch):
    frame, meta = await getattr(rnc, kind)(return_meta=True, use_cache=False)
    frame = frame.drop(columns="cultivar")
    source = AsyncMock(return_value=(frame, meta))
    monkeypatch.setattr(rnc, kind, source)
    with levanta_exatamente(ContractViolationError):
        await getattr(datasets, f"cultivares_{kind}")(return_meta=return_meta, use_cache=False)
    source.assert_awaited_once()
    assert len(cultivar_http["calls"]) == 4


@pytest.mark.parametrize(
    "kind,field", [(kind, field) for kind in FAMILIES for field in FILTERS[kind]]
)
@pytest.mark.parametrize("value", [1, False, [], "", "  "])
async def test_invalid_filters_fail_before_cache_and_http(
    kind, field, value, cultivar_http, monkeypatch
):
    reader = Mock(side_effect=AssertionError("cache accessed"))
    monkeypatch.setattr(snapshot, "read_acquisition", reader)
    with levanta_exatamente(InvalidParameterError):
        await getattr(datasets, f"cultivares_{kind}")(**{field: value})
    reader.assert_not_called()
    assert cultivar_http["calls"] == []


@pytest.mark.parametrize("kind", FAMILIES)
async def test_produto_fails_before_cache_and_http(kind, cultivar_http, monkeypatch):
    reader = Mock(side_effect=AssertionError("cache accessed"))
    monkeypatch.setattr(snapshot, "read_acquisition", reader)
    with levanta_exatamente(InvalidParameterError, match="não aceita produto"):
        await datasets.get_dataset(f"cultivares_{kind}").fetch("soja")
    reader.assert_not_called()
    assert cultivar_http["calls"] == []
