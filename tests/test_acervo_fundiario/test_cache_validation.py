from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from agrobr.acervo_fundiario import client


@pytest.mark.asyncio
@pytest.mark.usefixtures("isolated_cache")
@pytest.mark.parametrize(
    ("cached_modified", "remote_modified", "cached_etag", "remote_etag", "redownload"),
    [
        ("", "", "", "", True),
        ("same", "same", "old", "new", True),
        ("same", "same", "same", "same", False),
        ("", "", "same", "same", False),
        ("old", "new", "", "", True),
        ("same", "same", "", "", False),
    ],
)
async def test_cache_requires_matching_remote_validator(
    monkeypatch: pytest.MonkeyPatch,
    cached_modified: str,
    remote_modified: str,
    cached_etag: str,
    remote_etag: str,
    redownload: bool,
):
    payload = b"PK\x03\x04" + b"x" * 1000
    target = client._zip_path("sigef", "ES")
    target.parent.mkdir(parents=True)
    target.write_bytes(payload)
    client._save_meta(
        client._meta_path("sigef", "ES"),
        {
            "last_modified": cached_modified,
            "etag": cached_etag,
            "size_bytes": len(payload),
            "fetched_at": "2026-09-21T09:55:19+00:00",
        },
    )
    monkeypatch.setattr(
        client,
        "_head",
        AsyncMock(
            return_value={
                "last_modified": remote_modified,
                "etag": remote_etag,
                "content_length": len(payload),
            }
        ),
    )
    download = AsyncMock(return_value=(len(payload), "new-hash"))
    monkeypatch.setattr(client, "_stream_download", download)

    assert (await client.download_and_cache("sigef", "ES")).zip_path == target
    assert download.await_count == int(redownload)


@pytest.mark.parametrize(("cached_size", "remote_size"), [(1005, 1004), (1004, 1005)])
def test_cache_rejects_changed_size(tmp_path: Path, cached_size: int, remote_size: int):
    target = tmp_path / "data.zip"
    target.write_bytes(b"PK\x03\x04" + b"x" * 1000)
    metadata = {"size_bytes": cached_size, "etag": "same", "last_modified": "same"}
    remote = {"content_length": remote_size, "etag": "same", "last_modified": "same"}

    assert not client._cache_matches_remote(target, metadata, remote)
