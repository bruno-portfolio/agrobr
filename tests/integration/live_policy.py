from __future__ import annotations

import os
from typing import Any

import requests

from agrobr.antaq import client as antaq_client
from agrobr.exceptions import SourceUnavailableError

ANTAQ_2024_URL = "https://estatistica.antaq.gov.br/ea/txt/2024.zip"
USDA_MISSING_KEY = "USDA: AGROBR_USDA_API_KEY/api_key ausente; consulta não executada"
ANTAQ_2024_522 = f"ANTAQ: HTTP 522 confirmado no download de {ANTAQ_2024_URL}"


def missing_usda_key(dataset_name: str, kwargs: dict[str, Any]) -> bool:
    return dataset_name == "oferta_demanda_global" and not (
        kwargs.get("api_key") or os.environ.get("AGROBR_USDA_API_KEY")
    )


def known_antaq_2024_outage(error: SourceUnavailableError, ano: int) -> bool:
    cause = error.__cause__
    expected_request = (
        ano == 2024 and error.source == "antaq" and error.url == ANTAQ_2024_URL and not error.errors
    )
    if expected_request and isinstance(error, antaq_client.OfficialOutageError):
        return True
    return (
        expected_request
        and isinstance(cause, requests.exceptions.HTTPError)
        and cause.response is not None
        and cause.response.status_code == 522
        and cause.response.url == ANTAQ_2024_URL
    )
