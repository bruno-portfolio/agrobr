from __future__ import annotations

import httpx
import pytest

from agrobr import constants
from agrobr.acervo_fundiario import client as acervo
from agrobr.exceptions import ResourceLimitError


class Stream(httpx.AsyncByteStream):
    def __init__(self) -> None:
        self.closed = False

    async def __aiter__(self):
        yield b"PK\x03\x04" + b"x" * (65536 - 4)
        yield b"x"

    async def aclose(self):
        self.closed = True


async def test_acervo_interrompe_download_e_preserva_destino_anterior(monkeypatch, tmp_path):
    monkeypatch.setattr(constants, "ACERVO_MAX_DOWNLOAD_BYTES", 65536)
    destination = tmp_path / "dados.zip"
    destination.write_bytes(b"previous download")
    stream = Stream()
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, stream=stream))
    ) as http:
        with pytest.raises(ResourceLimitError, match="orçamento"):
            await acervo._stream_download(http, "https://example.test/data.zip", destination)
    assert stream.closed
    assert destination.read_bytes() == b"previous download"
    assert list(tmp_path.iterdir()) == [destination]
