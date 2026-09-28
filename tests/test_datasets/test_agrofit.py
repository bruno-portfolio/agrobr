from __future__ import annotations

import hashlib
import json
import zipfile
from pathlib import Path
from typing import Any
from unittest.mock import Mock

import httpx
import pytest

from agrobr import contracts, datasets
from agrobr.datasets.deterministic import deterministic
from agrobr.defensivos import cache, client, snapshot
from agrobr.exceptions import InvalidParameterError

GOLDEN = Path(__file__).parents[1] / "golden_data/defensivos/selecao_20260906"
TABLES = [
    ("defensivos_formulados", "formulados", "formulados", {}, 12, "1.1"),
    ("defensivos_tecnicos", "tecnicos", "tecnicos", {}, 10, "1.1"),
    ("autorizacoes_defensivos", "formulados", "autorizacoes", {}, 10, "1.1"),
    ("composicao_defensivos", "formulados", "composicao", {}, 9, "1.0"),
    ("composicao_defensivos", "tecnicos", "composicao", {"tipo": "tecnicos"}, 9, "1.0"),
]


@pytest.fixture
def agrofit_http(monkeypatch, tmp_path):
    original = httpx.AsyncClient
    bodies = {
        kind: (GOLDEN / f"{kind}_curated_rows.csv").read_bytes()
        for kind in ("formulados", "tecnicos")
    }
    requests = []
    monkeypatch.setattr(cache, "_cache_dir", lambda: tmp_path)
    monkeypatch.setattr(client, "MIN_CSV_FORMULADOS", 1)
    monkeypatch.setattr(client, "MIN_CSV_TECNICOS", 1)

    def handler(request):
        assert request.method == "GET"
        assert str(request.url) in {client.FORMULADOS_URL, client.TECNICOS_URL}
        kind = "formulados" if str(request.url) == client.FORMULADOS_URL else "tecnicos"
        requests.append(request)
        return httpx.Response(200, content=bodies[kind], headers={"content-type": "text/csv"})

    monkeypatch.setattr(
        httpx,
        "AsyncClient",
        lambda *args, **kwargs: original(
            *args, **{**kwargs, "transport": httpx.MockTransport(handler)}
        ),
    )
    return {"bodies": bodies, "requests": requests, "cache": tmp_path}


@pytest.mark.parametrize("name,kind,table,kwargs,width,version", TABLES)
async def test_official_subset_replay_through_dataset_keeps_contract_and_resource(
    name, kind, table, kwargs, width, version, agrofit_http
):
    frame, meta = await getattr(datasets, name)(**kwargs, return_meta=True, use_cache=False)
    assert len(frame.columns) == width and not frame.empty
    assert contracts.get_contract(f"agrofit_{table}").validate(frame) == (True, [])
    assert datasets.get_dataset(name)._contract_name(**kwargs) == f"agrofit_{table}"
    assert meta.dataset == name and meta.source == f"datasets.{name}/defensivos"
    assert meta.schema_version == meta.contract_version == version and meta.parser_version == 3
    assert meta.selected_source == "defensivos" and meta.attempted_sources == ["defensivos"]
    assert meta.records_count == len(frame) and meta.columns == frame.columns.tolist()
    assert meta.snapshot is None and not meta.from_cache
    raw = agrofit_http["bodies"][kind]
    assert meta.raw_content_hash == hashlib.sha256(raw).hexdigest()
    assert meta.raw_content_size == len(raw)
    assert meta.cache_key is None and meta.cache_expires_at is None
    assert meta.source_details["kind"] == kind and meta.source_details["query"]["table"] == table
    assert meta.source_details["resource"]["sha256"] == meta.raw_content_hash
    assert meta.source_details["resource"]["bytes"] == len(raw)
    assert len(agrofit_http["requests"]) == 1 and not list(agrofit_http["cache"].iterdir())
    assert frame["nr_registro"].map(type).eq(str).all()
    if table == "composicao":
        assert str(frame["ordem_componente"].dtype) == "Int64"
        assert str(frame["concentracao_valor"].dtype) == "Float64"


@pytest.mark.parametrize("name", sorted({item[0] for item in TABLES}))
@pytest.mark.parametrize("flag", ["as_polars", "return_meta", "use_cache"])
@pytest.mark.parametrize("value", [1, None, "false"])
async def test_flags_rejected_before_cache_or_http(name, flag, value, monkeypatch, agrofit_http):
    read = Mock(side_effect=AssertionError("cache must not be touched"))
    monkeypatch.setattr(snapshot, "read_snapshot", read)
    with pytest.raises(InvalidParameterError):
        await getattr(datasets, name)(**{flag: value})
    assert not agrofit_http["requests"] and not read.called


@pytest.mark.parametrize("name", sorted({item[0] for item in TABLES}))
async def test_deterministic_rejected_before_cache_or_http(name, monkeypatch, agrofit_http):
    read = Mock(side_effect=AssertionError("cache must not be touched"))
    monkeypatch.setattr(snapshot, "read_snapshot", read)
    async with deterministic("2026-09-06"):
        with pytest.raises(InvalidParameterError):
            await getattr(datasets, name)()
    assert not agrofit_http["requests"] and not read.called


@pytest.mark.parametrize(
    "name,kind,filters",
    [
        (
            "defensivos_formulados",
            "formulados",
            {
                "classe_toxicologica": "CLASSE_TOXICOLOGICA",
                "classe_ambiental": "CLASSE_AMBIENTAL",
                "titular": "TITULAR_DE_REGISTRO",
                "organicos": "ORGANICOS",
                "marca": "MARCA_COMERCIAL",
                "formulacao": "FORMULACAO",
                "classe": "CLASSE",
                "nr_registro": "NR_REGISTRO",
                "situacao": "SITUACAO",
            },
        ),
        (
            "defensivos_tecnicos",
            "tecnicos",
            {
                "titular": "TITULAR_REGISTRO",
                "classe": "CLASSE",
                "marca": "PRODUTO_TECNICO_MARCA_COMERCIAL",
                "nr_registro": "NUMERO_REGISTRO",
            },
        ),
        (
            "autorizacoes_defensivos",
            "formulados",
            {
                "nr_registro": "NR_REGISTRO",
                "cultura": "CULTURA",
                "classe": "CLASSE",
                "situacao": "SITUACAO",
            },
        ),
    ],
)
async def test_every_named_filter_reaches_source_query_and_matches_published_cells(
    name, kind, filters, agrofit_http
):
    raw = json.loads((GOLDEN / f"{kind}_curated_oracles.json").read_text(encoding="utf-8"))[0][
        "values"
    ]
    for parameter, published in filters.items():
        value = raw[published].strip()
        frame, meta = await getattr(datasets, name)(**{parameter: value}, return_meta=True)
        column = "marca_comercial" if parameter == "marca" else parameter
        assert not frame.empty and frame[column].str.contains(value, regex=False).all()
        assert meta.source_details["query"]["filters"] == {parameter: value}
    assert len(agrofit_http["requests"]) == 1


async def test_invalid_typed_cache_metadata_triggers_fresh_acquisition(
    agrofit_http: dict[str, Any],
):
    _, original = await datasets.defensivos_formulados(return_meta=True)
    path = cache.snapshot_path("formulados")
    with zipfile.ZipFile(path) as bundle:
        contents = {name: bundle.read(name) for name in bundle.namelist()}
    manifest = json.loads(contents["manifest.json"])
    manifest["meta"]["records_count"] = "invalid integer"
    contents["manifest.json"] = json.dumps(manifest).encode("utf-8")
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as bundle:
        for name, content in contents.items():
            bundle.writestr(name, content)
    frame, acquired = await datasets.defensivos_formulados(return_meta=True)
    assert len(agrofit_http["requests"]) == 2
    assert not acquired.from_cache
    assert acquired.records_count == len(frame) == original.records_count
    assert acquired.raw_content_hash == original.raw_content_hash


@pytest.mark.parametrize("name", sorted({item[0] for item in TABLES}))
@pytest.mark.parametrize("produto", ["soja", None, 0])
async def test_registry_internal_product_accepts_only_empty_string(name, produto, agrofit_http):
    with pytest.raises(InvalidParameterError):
        await datasets.get_dataset(name).fetch(produto)
    assert not agrofit_http["requests"]
