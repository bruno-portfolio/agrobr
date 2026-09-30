from __future__ import annotations

import json
from urllib.parse import quote

import httpx
import pytest

from agrobr.exceptions import SourceUnavailableError
from agrobr.http.responses import (
    arcgis_error_message,
    parse_json_response,
    raise_for_service_error,
)
from tests.helpers import levanta_exatamente, sem_excecao


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


SEGREDO = 'segredo/dd+1 "x"'


@pytest.mark.parametrize(
    "variavel",
    [
        "AGROBR_USDA_API_KEY",
        "AGROBR_INMET_TOKEN",
        "AGROBR_COMTRADE_API_KEY",
        "AGROBR_MAPBIOMAS_ALERTA_TOKEN",
        "AGROBR_CONAB_CEASA_PASS",
        "AGROBR_ALERT_SLACK_WEBHOOK",
        "AGROBR_ALERT_DISCORD_WEBHOOK",
        "AGROBR_ALERT_SENDGRID_API_KEY",
    ],
)
@pytest.mark.parametrize(
    "eco", [SEGREDO, quote(SEGREDO, safe=""), json.dumps(SEGREDO)[1:-1]], ids=["cru", "url", "json"]
)
def test_previa_nao_json_mascara_a_credencial_do_ambiente(monkeypatch, variavel, eco):
    monkeypatch.setenv(variavel, SEGREDO)
    response = _response(f"gateway rejected credential: {eco}".encode(), "text/plain")

    with levanta_exatamente(SourceUnavailableError, r"credential: \[REDACTED\]") as erro:
        parse_json_response(response, source="test", url=str(response.url))

    assert eco not in str(erro.value)


def test_previa_mascara_a_credencial_do_argumento_e_ignora_vazio(monkeypatch):
    monkeypatch.setenv("AGROBR_USDA_API_KEY", "")
    response = _response(b"rejected: chave-do-argumento", "text/plain")

    with levanta_exatamente(SourceUnavailableError, r"'rejected: \[REDACTED\]'"):
        parse_json_response(
            response, source="test", url="u", secrets=("chave-do-argumento", "", None)
        )
    with levanta_exatamente(SourceUnavailableError, r"'rejected: chave-do-argumento'"):
        parse_json_response(response, source="test", url="u", secrets=("", None))


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
