from __future__ import annotations

import pytest

from agrobr.acervo_fundiario import client
from agrobr.exceptions import SourceUnavailableError


class TestCacheKeyHelpers:
    def test_cache_key_with_uf(self):
        assert client._cache_key("sigef", "GO") == "sigef:GO"

    def test_zip_path_with_uf(self, isolated_cache):  # noqa: ARG002
        path = client._zip_path("sigef", "GO")
        assert path.name == "GO.zip"
        assert "sigef" in str(path)


async def test_get_lock_returns_same_for_same_key():
    first = await client._get_lock("sigef:GO")
    second = await client._get_lock("sigef:GO")
    assert first is second


@pytest.mark.asyncio
@pytest.mark.usefixtures("isolated_cache")
class TestDownloadAndCache:
    async def test_invalid_tema_raises(self):
        try:
            await client.download_and_cache("invalid_tema", "GO")
        except Exception as exc:
            caught = exc
        else:
            caught = None
        assert isinstance(caught, ValueError) and "tema invalido" in str(caught), caught


@pytest.mark.asyncio
async def test_head_404_raises_source_unavailable():
    class FakeResp:
        status_code = 404
        headers: dict[str, str] = {}

        def raise_for_status(self):
            pass

    class FakeClient:
        async def head(self, _url, timeout=None):  # noqa: ARG002
            return FakeResp()

    with pytest.raises(SourceUnavailableError, match="HTTP 404"):
        await client._head(FakeClient(), "https://example.com/missing.zip")  # type: ignore[arg-type]


@pytest.mark.parametrize(("valor", "desligado"), [("true", True), ("YES", True), ("0", False)])
def test_cache_disabled_aceita_booleanos(monkeypatch, valor, desligado):
    monkeypatch.setenv("AGROBR_ACERVO_FUNDIARIO_CACHE_DISABLED", valor)
    assert client._cache_disabled() is desligado
