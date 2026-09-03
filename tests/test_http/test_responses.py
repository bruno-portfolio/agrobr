from __future__ import annotations

import httpx
import pytest

from agrobr.exceptions import SourceUnavailableError
from agrobr.http.responses import parse_json_response


def _response(content: bytes, content_type: str) -> httpx.Response:
    return httpx.Response(
        200,
        content=content,
        headers={"content-type": content_type},
        request=httpx.Request("GET", "https://example.test/data"),
    )


class TestParseJsonResponse:
    def test_json_valido(self):
        response = _response(b'{"ok": true}', "application/json")
        assert parse_json_response(response, source="test", url=str(response.url)) == {"ok": True}

    def test_html_com_status_200(self):
        response = _response(b"<html>\nService Unavailable</html>", "text/html")

        with pytest.raises(SourceUnavailableError, match="provável WAF ou manutenção") as exc_info:
            parse_json_response(response, source="test", url=str(response.url))

        assert "\n" not in exc_info.value.last_error
        assert "text/html" in exc_info.value.last_error

    def test_texto_vazio(self):
        response = _response(b"", "text/plain")

        with pytest.raises(SourceUnavailableError, match="Resposta não é JSON"):
            parse_json_response(response, source="test", url=str(response.url))
