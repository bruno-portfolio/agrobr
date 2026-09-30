from __future__ import annotations

import hashlib
import os
from pathlib import Path

import httpx
import pytest

from agrobr.acervo_fundiario import client


@pytest.mark.parametrize("failures", [1, 4])
async def test_download_replace_retries_without_repeating_transfer(tmp_path, monkeypatch, failures):
    target = tmp_path / "MT.zip"
    target.write_bytes(b"previous archive")
    payload = b"PK\x03\x04" + b"new archive" * 100
    original = os.replace
    attempted = []
    requests = []

    def replace(source, destination):
        attempted.append(Path(source))
        if len(attempted) <= failures:
            assert target.read_bytes() == b"previous archive"
            raise PermissionError("transient sharing conflict")
        original(source, destination)

    def handler(request):
        requests.append(request)
        return httpx.Response(200, content=payload)

    monkeypatch.setattr(os, "replace", replace)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        try:
            result = await client._stream_download(http, "https://audit.invalid/file", target)
        except Exception as exc:
            result = exc

    assert not isinstance(result, Exception), result
    assert result == (len(payload), hashlib.sha256(payload).hexdigest())
    assert len(requests) == 1
    assert len(attempted) == failures + 1
    assert len(set(attempted)) == 1
    assert target.read_bytes() == payload
    assert not list(tmp_path.glob("*.tmp"))
