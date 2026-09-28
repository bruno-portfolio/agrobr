from __future__ import annotations

import json
import struct
import warnings
import zipfile
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from agrobr import constants
from agrobr.rnc import snapshot
from tests.helpers import levanta_exatamente, rnc_csv_acquisition, sem_excecao

GOLDEN = Path(__file__).parents[1] / "golden_data/rnc/selecao_20260907"


@pytest.fixture
def isolated_cache(tmp_path, monkeypatch):
    monkeypatch.setattr(snapshot, "cache_dir", lambda: tmp_path)
    return tmp_path


def _capture(kind="registradas", **kwargs):
    return rnc_csv_acquisition((GOLDEN / f"{kind}.csv").read_bytes(), kind, **kwargs)


def _cache_path(directory, kind="registradas"):
    return directory / f"{kind}.acquisition.v1.zip"


def _rewrite(path, mutate, *, duplicate=False, extra=False):
    with zipfile.ZipFile(path) as bundle:
        manifest = json.loads(bundle.read("manifest.json"))
        source = bundle.read("source.csv")
    mutate(manifest)
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_STORED) as bundle:
        bundle.writestr("manifest.json", json.dumps(manifest))
        bundle.writestr("source.csv", source)
        if duplicate:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", UserWarning)
                bundle.writestr("source.csv", source)
        if extra:
            bundle.writestr("unexpected.txt", b"extra")


@pytest.mark.parametrize("kind", ["registradas", "protegidas"])
def test_raw_cache_round_trip_preserves_acquisition_and_original_bytes(kind, isolated_cache):
    captured = _capture(kind, reported_total=29 if kind == "registradas" else 27)
    snapshot.write_acquisition(captured)
    restored = snapshot.read_acquisition(kind)
    assert restored is not None
    assert restored.content == captured.content
    assert restored.provenance() == captured.provenance()
    with zipfile.ZipFile(_cache_path(isolated_cache, kind)) as bundle:
        assert set(bundle.namelist()) == {"source.csv", "manifest.json"}
        assert bundle.testzip() is None


@pytest.mark.parametrize("age", [constants.RNC_CACHE_TTL_SECONDS, -60])
def test_cache_rejects_expired_or_future_acquisition_even_with_fresh_file(age, isolated_cache):
    captured = _capture(received_at=datetime.now(UTC) - timedelta(seconds=age))
    snapshot.write_acquisition(captured)
    assert snapshot.read_acquisition("registradas") is None
    assert _cache_path(isolated_cache).exists()


@pytest.mark.parametrize(
    ("field", "value"),
    [("format_version", 0), ("parser_version", 0), ("schema_version", "99.0")],
)
def test_cache_rejects_incompatible_version(field, value, isolated_cache):
    snapshot.write_acquisition(_capture())
    _rewrite(_cache_path(isolated_cache), lambda manifest: manifest.update({field: value}))
    assert snapshot.read_acquisition("registradas") is None


@pytest.mark.parametrize("kind", ["duplicate", "unexpected", "encrypted", "crc", "truncated"])
def test_cache_rejects_corrupted_or_ambiguous_zip(kind, isolated_cache):
    snapshot.write_acquisition(_capture())
    path = _cache_path(isolated_cache)
    _rewrite(path, lambda _: None, duplicate=kind == "duplicate", extra=kind == "unexpected")
    content = bytearray(path.read_bytes())
    if kind == "encrypted":
        central = content.index(b"PK\x01\x02")
        flags = struct.unpack_from("<H", content, central + 8)[0]
        struct.pack_into("<H", content, central + 8, flags | 1)
    elif kind == "crc":
        with zipfile.ZipFile(path) as bundle:
            member = bundle.getinfo("source.csv")
        offset = member.header_offset + 30 + len(member.filename.encode()) + len(member.extra)
        content[offset] ^= 1
    elif kind == "truncated":
        content = content[: len(content) // 2]
    path.write_bytes(content)
    with sem_excecao():
        assert snapshot.read_acquisition("registradas") is None


@pytest.mark.parametrize("kind", ["deflate", "method", "directory"])
def test_cache_ignores_unreadable_package(kind, isolated_cache):
    snapshot.write_acquisition(_capture())
    path = _cache_path(isolated_cache)
    content = bytearray(path.read_bytes())
    with zipfile.ZipFile(path) as bundle:
        member = bundle.getinfo("source.csv")
    if kind == "deflate":
        offset = member.header_offset + 30 + len(member.filename.encode()) + len(member.extra)
        content[offset + 10] ^= 0xFF
        path.write_bytes(content)
    elif kind == "method":
        struct.pack_into("<H", content, member.header_offset + 8, 99)
        central = content.index(b"PK\x01\x02")
        while content[central + 46 : central + 46 + len(b"source.csv")] != b"source.csv":
            central = content.index(b"PK\x01\x02", central + 4)
        struct.pack_into("<H", content, central + 10, 99)
        path.write_bytes(content)
    else:
        path.unlink()
        path.mkdir()
    with sem_excecao():
        assert snapshot.read_acquisition("registradas") is None


def test_cache_of_other_family_is_ignored(isolated_cache):
    snapshot.write_acquisition(_capture("protegidas"))
    _cache_path(isolated_cache, "protegidas").rename(_cache_path(isolated_cache))
    assert snapshot.read_acquisition("registradas") is None
    assert _cache_path(isolated_cache).exists()


def test_unknown_family_never_reaches_the_cache(isolated_cache):
    with levanta_exatamente(ValueError, match="Família RNC/SNPC desconhecida"):
        snapshot.read_acquisition("../registradas")
    assert not list(isolated_cache.iterdir())
