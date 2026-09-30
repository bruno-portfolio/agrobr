from __future__ import annotations

import asyncio
import hashlib
import json
import zipfile
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest

from agrobr import contracts
from agrobr.defensivos import cache, parser, snapshot
from agrobr.models import MetaInfo


@pytest.fixture
def acquisition(tmp_path, monkeypatch):
    monkeypatch.setattr(cache, "_cache_dir", lambda: tmp_path)
    tables = {}
    for name in ("tecnicos", "composicao"):
        tables[name] = contracts.get_contract(f"agrofit_{name}").empty_frame().reindex(range(3))
    texto = parser.TEXTO
    tables["tecnicos"]["nr_registro"] = pd.Series(["001", "TC02523", "003"], dtype=texto)
    tables["tecnicos"]["marca_comercial"] = pd.Series(["TRUE", "", float("nan")], dtype=texto)
    component = tables["composicao"]
    component["tipo"] = pd.Series(["tecnicos"] * 3, dtype=texto)
    component["nr_registro"] = pd.Series(["001", "TC02523", "003"], dtype=texto)
    component["ordem_componente"] = pd.Series([1, 1, 1], dtype="Int64")
    component["componente_texto"] = pd.Series(
        ["A (X) (0 g/kg)", "B", "C (X) (950 Kg)"], dtype=texto
    )
    component["concentracao_valor"] = pd.Series([0.0, float("nan"), 950.0])
    component["concentracao_unidade"] = pd.Series(["g/kg", float("nan"), "Kg"], dtype=texto)
    meta = MetaInfo(
        source="defensivos",
        source_url="https://dados.agricultura.gov.br/test.csv",
        source_method="replay",
        fetched_at=datetime.now(UTC),
        parser_version=parser.PARSER_VERSION,
        schema_version="1.1",
        raw_content_hash=hashlib.sha256(b"fixture").hexdigest(),
        raw_content_size=7,
        source_details={"resource": {"edition": "fixture"}},
    )
    return tables, meta, tmp_path / "tecnicos.v3.zip"


def _members(path: Path) -> dict[str, bytes]:
    with zipfile.ZipFile(path) as bundle:
        return {name: bundle.read(name) for name in bundle.namelist()}


def _write_members(path: Path, members: dict[str, bytes]) -> None:
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as bundle:
        for name, content in members.items():
            bundle.writestr(name, content)


def test_snapshot_roundtrip_preserves_nullable_numeric_types_text_and_metadata(acquisition):
    tables, meta, _ = acquisition
    snapshot.write_snapshot("tecnicos", tables, meta)
    result = snapshot.read_snapshot("tecnicos")
    assert result is not None
    for name, frame in tables.items():
        pd.testing.assert_frame_equal(result.tables[name], frame)
    assert result.tables["tecnicos"]["nr_registro"].tolist() == ["001", "TC02523", "003"]
    assert result.tables["tecnicos"]["marca_comercial"].iloc[0] == "TRUE"
    assert result.tables["tecnicos"]["marca_comercial"].iloc[1] == ""
    assert pd.isna(result.tables["tecnicos"]["marca_comercial"].iloc[2])
    assert pd.isna(result.tables["composicao"]["concentracao_valor"].iloc[1])
    assert result.meta.fetched_at == meta.fetched_at
    assert result.meta.raw_content_hash == meta.raw_content_hash
    assert result.meta.source_details == meta.source_details
    assert result.meta.from_cache == meta.from_cache


@pytest.mark.parametrize(
    "age_seconds,available", [(0, True), (86399, True), (86400, False), (86401, False), (-1, False)]
)
def test_snapshot_ttl_uses_acquisition_time_not_file_mtime(
    age_seconds, available, acquisition, monkeypatch
):
    tables, meta, path = acquisition
    now = datetime(2026, 9, 7, 12, tzinfo=UTC)
    meta.fetched_at = now - timedelta(seconds=age_seconds)
    snapshot.write_snapshot("tecnicos", tables, meta)

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return now if tz is not None else now.replace(tzinfo=None)

    monkeypatch.setattr(snapshot, "datetime", FixedDateTime)
    assert path.exists()
    assert (snapshot.read_snapshot("tecnicos") is not None) is available


@pytest.mark.parametrize(
    "field,value",
    [
        ("format_version", 999),
        ("parser_version", 2),
        ("kind", "formulados"),
        ("schema_version", "99.0"),
        ("rows", 99),
        ("sha256", "0" * 64),
        ("dtypes", {"nr_registro": "int64"}),
        ("raw_content_hash", "invalid"),
        ("meta_schema_version", "9.9"),
        ("fetched_at_shift", timedelta(seconds=-1)),
        ("fetched_at_offset", timedelta(hours=-3)),
        ("parser_version_coerente", parser.PARSER_VERSION - 1),
    ],
)
def test_snapshot_rejects_incompatible_or_inconsistent_manifest(field, value, acquisition):
    tables, meta, path = acquisition
    snapshot.write_snapshot("tecnicos", tables, meta)
    members = _members(path)
    manifest = json.loads(members["manifest.json"])
    if field in {"schema_version", "rows", "sha256", "dtypes"}:
        manifest["tables"]["tecnicos"][field] = value
    elif field == "raw_content_hash":
        manifest["meta"][field] = value
    elif field == "meta_schema_version":
        manifest["meta"]["schema_version"] = value
    elif field.startswith("fetched_at_"):
        instant = datetime.fromisoformat(manifest["fetched_at"])
        if field == "fetched_at_shift":
            instant += value
        else:
            instant = instant.astimezone(timezone(value))
        manifest["fetched_at"] = instant.isoformat()
    elif field == "parser_version_coerente":
        manifest["parser_version"] = manifest["meta"]["parser_version"] = value
    else:
        manifest[field] = value
    members["manifest.json"] = json.dumps(manifest).encode()
    _write_members(path, members)
    assert snapshot.read_snapshot("tecnicos") is None
    assert path.exists()


@pytest.mark.parametrize(
    "damage",
    ["missing_table", "missing_manifest", "unexpected_member", "changed_csv", "truncated_zip"],
)
def test_snapshot_corruption_never_returns_partial_family(damage, acquisition):
    tables, meta, path = acquisition
    snapshot.write_snapshot("tecnicos", tables, meta)
    members = _members(path)
    if damage == "missing_table":
        del members["composicao.csv"]
    elif damage == "missing_manifest":
        del members["manifest.json"]
    elif damage == "unexpected_member":
        members["extra.csv"] = b"unexpected"
    elif damage == "changed_csv":
        members["tecnicos.csv"] += b"changed"
    else:
        path.write_bytes(path.read_bytes()[:50])
        assert snapshot.read_snapshot("tecnicos") is None
        return
    _write_members(path, members)
    assert snapshot.read_snapshot("tecnicos") is None


def test_snapshot_write_failure_preserves_previous_complete_bundle(acquisition, monkeypatch):
    tables, meta, path = acquisition
    snapshot.write_snapshot("tecnicos", tables, meta)
    before = path.read_bytes()
    original_write = zipfile.ZipFile.writestr

    def failing_write(self, name, data, *args, **kwargs):
        if name == "manifest.json":
            raise OSError("simulated disk failure")
        return original_write(self, name, data, *args, **kwargs)

    monkeypatch.setattr(zipfile.ZipFile, "writestr", failing_write)
    with pytest.raises(OSError, match="simulated disk failure"):
        snapshot.write_snapshot("tecnicos", tables, meta)
    assert path.read_bytes() == before
    assert snapshot.read_snapshot("tecnicos") is not None


def test_snapshot_cache_invalidation_removes_modern_and_legacy_files(acquisition):
    tables, meta, path = acquisition
    snapshot.write_snapshot("tecnicos", tables, meta)
    (path.parent / "tecnicos.csv").write_text("nr_registro\n001\n", encoding="utf-8")
    cache.invalidate()
    assert not path.exists()
    assert not (path.parent / "tecnicos.csv").exists()


async def test_snapshot_acquisition_lock_serializes_family_and_separates_families():
    technical = snapshot.acquisition_lock("tecnicos")
    assert technical is snapshot.acquisition_lock("tecnicos")
    assert technical is not snapshot.acquisition_lock("formulados")
    entered = asyncio.Event()

    async def waiting():
        async with snapshot.acquisition_lock("tecnicos"):
            entered.set()

    async with technical:
        task = asyncio.create_task(waiting())
        await asyncio.sleep(0)
        assert not entered.is_set()
        async with snapshot.acquisition_lock("formulados"):
            assert not entered.is_set()
    await asyncio.wait_for(task, timeout=5)
    assert entered.is_set()


def test_snapshot_lock_is_separate_for_each_event_loop():
    async def capture():
        return snapshot.acquisition_lock("tecnicos")

    first = asyncio.run(capture())
    second = asyncio.run(capture())
    assert first is not second


def _formulated_tables(tables: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    product = contracts.get_contract("agrofit_formulados").empty_frame().reindex(range(3))
    product["nr_registro"] = tables["tecnicos"]["nr_registro"].copy()
    auth = contracts.get_contract("agrofit_autorizacoes").empty_frame().reindex(range(3))
    auth["nr_registro"] = product["nr_registro"].copy()
    components = tables["composicao"].copy()
    components["tipo"] = pd.Series(["formulados"] * 3, dtype=object)
    return {"formulados": product, "autorizacoes": auth, "composicao": components}


@pytest.mark.parametrize("damage", ["wrong_family", "orphan_component", "orphan_authorization"])
def test_snapshot_write_rejects_semantic_family_errors_preserving_previous_bundle(
    damage, acquisition
):
    tables, meta, path = acquisition
    kind = "formulados" if damage == "orphan_authorization" else "tecnicos"
    if kind == "formulados":
        tables = _formulated_tables(tables)
        path = path.with_name("formulados.v3.zip")
    snapshot.write_snapshot(kind, tables, meta)
    before = path.read_bytes()
    if damage == "wrong_family":
        tables["composicao"].loc[0, "tipo"] = "formulados"
    elif damage == "orphan_component":
        tables["composicao"].loc[0, "nr_registro"] = "ORPHAN"
    else:
        tables["autorizacoes"].loc[0, "nr_registro"] = "ORPHAN"
    with pytest.raises(ValueError):
        snapshot.write_snapshot(kind, tables, meta)
    assert path.read_bytes() == before
    assert snapshot.read_snapshot(kind) is not None


@pytest.mark.parametrize("damage", ["wrong_family", "orphan_component", "orphan_authorization"])
def test_snapshot_read_rejects_semantic_errors_even_with_matching_hashes(damage, acquisition):
    tables, meta, path = acquisition
    kind = "formulados" if damage == "orphan_authorization" else "tecnicos"
    if kind == "formulados":
        tables = _formulated_tables(tables)
        path = path.with_name("formulados.v3.zip")
    snapshot.write_snapshot(kind, tables, meta)
    members = _members(path)
    table = "autorizacoes" if damage == "orphan_authorization" else "composicao"
    body = members[f"{table}.csv"]
    if damage == "wrong_family":
        body = body.replace(b"tecnicos;", b"formulados;", 1)
    elif damage == "orphan_component":
        body = body.replace(b";001;", b";ORPHAN;", 1)
    else:
        body = body.replace(b"\n001;", b"\nORPHAN;", 1)
    assert body != members[f"{table}.csv"]
    members[f"{table}.csv"] = body
    manifest = json.loads(members["manifest.json"])
    manifest["tables"][table]["sha256"] = hashlib.sha256(body).hexdigest()
    members["manifest.json"] = json.dumps(manifest).encode()
    _write_members(path, members)
    assert snapshot.read_snapshot(kind) is None
