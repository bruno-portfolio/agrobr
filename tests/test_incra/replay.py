from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
import pandas as pd

from agrobr.http import wfs_transport
from agrobr.incra.andamento import client

GOLDEN = Path(__file__).parents[1] / "golden_data/incra"
NATIONAL = GOLDEN / "nacional_20260908"
JUNE = GOLDEN / "andamento_20260608"
SEPTEMBER = GOLDEN / "andamento_20260903"
PAGE_URL = "https://www.gov.br/incra/pt-br/assuntos/governanca-fundiaria/quilombolas"
PDF_URL = PAGE_URL + "/andamento_dos_processos_quilombolas-08_06_2026.pdf/@@display-file/file"


def publicado(coluna: str, bruto: Any) -> Any:
    """Valor que a saída pública traz para o literal da fonte: datas tipadas (fora de 1900–2099
    viram ausente) e o cadastro em UTC."""
    if bruto is None or not coluna.startswith("data_"):
        return bruto
    if coluna == "data_cadastro":
        return pd.Timestamp(bruto).tz_convert("UTC")
    return pd.Timestamp(bruto) if 1900 <= int(bruto[:4]) <= 2099 else None


def national_features() -> list[dict]:
    first = json.loads((NATIONAL / "page_001.json").read_bytes())["features"]
    second = json.loads((NATIONAL / "page_002.json").read_bytes())["features"]
    return first + second[1:]


def install_national_wfs(monkeypatch) -> list[tuple[dict[str, str], dict[str, str]]]:
    resources = json.loads((NATIONAL / "metadata.json").read_bytes())["resources"]
    probes = iter(item for item in resources if item["role"] != "page")
    pages = {
        (item["parameters"]["startIndex"], item["parameters"]["count"]): item
        for item in resources
        if item["role"] == "page"
    }
    calls: list[tuple[dict[str, str], dict[str, str]]] = []

    def respond(request):
        params = request.url.params
        item = (
            next(probes)
            if params.get("count") == "1"
            else pages[(params.get("startIndex"), params.get("count"))]
        )
        calls.append((dict(params), item["parameters"]))
        return httpx.Response(200, content=(NATIONAL / item["file"]).read_bytes(), request=request)

    namespace = SimpleNamespace(**vars(httpx))
    namespace.AsyncClient = lambda **kwargs: httpx.AsyncClient(
        transport=httpx.MockTransport(respond), **kwargs
    )
    monkeypatch.setattr(wfs_transport, "httpx", namespace)
    return calls


def install_publication(monkeypatch, *, publisher_status=200, pdf_body=None, folder=JUNE):
    calls = []

    def handler(request):
        calls.append(request)
        if str(request.url) == PAGE_URL:
            return httpx.Response(
                publisher_status, content=(folder / "publisher.html").read_bytes()
            )
        assert str(request.url) == PDF_URL
        return httpx.Response(
            200,
            content=(folder / "publication.pdf").read_bytes() if pdf_body is None else pdf_body,
        )

    def factory(**kwargs):
        return httpx.AsyncClient(transport=httpx.MockTransport(handler), **kwargs)

    monkeypatch.setattr(
        client, "httpx", SimpleNamespace(AsyncClient=factory, HTTPError=httpx.HTTPError)
    )
    return calls
