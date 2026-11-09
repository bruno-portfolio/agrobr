from __future__ import annotations

import os
import ssl
from typing import Any
from urllib.parse import urlsplit

import requests

from agrobr.antaq import client as antaq_client
from agrobr.conab.ceasa import models as ceasa_models
from agrobr.exceptions import SourceUnavailableError

ANTAQ_2024_URL = "https://estatistica.antaq.gov.br/ea/txt/2024.zip"
USDA_MISSING_KEY = "USDA: AGROBR_USDA_API_KEY/api_key ausente; consulta não executada"
ANTAQ_2024_522 = f"ANTAQ: HTTP 522 confirmado no download de {ANTAQ_2024_URL}"
CONAB_CEASA_HOST = str(urlsplit(ceasa_models.PENTAHO_BASE).hostname)
CONAB_CEASA_OUTRO_HOST = f"CONAB CEASA: {CONAB_CEASA_HOST} responde com o certificado de outro host"
X509_V_ERR_HOSTNAME_MISMATCH = 62


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


def known_conab_ceasa_outage(error: SourceUnavailableError) -> bool:
    """O httpx guarda o erro de TLS no contexto do ConnectError do httpcore, e não na causa."""
    cause = error.__cause__
    while cause is not None and not isinstance(cause, ssl.SSLCertVerificationError):
        cause = cause.__cause__ or cause.__context__
    return (
        error.source == "conab_ceasa"
        and not error.errors
        and cause is not None
        and cause.verify_code == X509_V_ERR_HOSTNAME_MISMATCH
        and CONAB_CEASA_HOST in cause.verify_message
    )
