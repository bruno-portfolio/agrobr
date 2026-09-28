from __future__ import annotations

import httpx
import pytest

from agrobr.exceptions import SourceUnavailableError
from agrobr.http.responses import (
    arcgis_error_message,
    parse_json_response,
    raise_for_service_error,
)
from tests.helpers import sem_excecao


def _response(content: bytes, content_type: str) -> httpx.Response:
    return httpx.Response(
        200,
        content=content,
        headers={"content-type": content_type},
        request=httpx.Request("GET", "https://example.test/data"),
    )


class TestParseJsonResponse:
    def test_html_com_status_200(self):
        response = _response(b"<html>\nService Unavailable</html>", "text/html")

        with pytest.raises(SourceUnavailableError, match="provável WAF ou manutenção") as exc_info:
            parse_json_response(response, source="test", url=str(response.url))

        assert "\n" not in exc_info.value.last_error
        assert "text/html" in exc_info.value.last_error


@pytest.mark.parametrize(
    ("dados", "mensagem"),
    [
        ({"error": {"code": 500, "message": "Offline"}}, "ArcGIS error 500: Offline"),
        ({"error": "fora do ar"}, "ArcGIS error unknown: fora do ar"),
        ({"error": {}}, "ArcGIS error unknown: unknown error"),
        ({"features": []}, None),
    ],
)
def test_arcgis_error_message(dados, mensagem):
    assert arcgis_error_message(dados) == mensagem


def test_corpo_sem_marca_de_erro_passa_pela_checagem_de_servico():
    with sem_excecao():
        raise_for_service_error(
            _response(b"id;nome\n1;x\n", "text/csv"), source="teste", url="https://example.test"
        )
