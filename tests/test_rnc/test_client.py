from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from pathlib import Path
from urllib.parse import parse_qs

import httpx
import pytest

from agrobr.exceptions import ParseError, SourceUnavailableError
from agrobr.rnc import client
from tests.helpers import levanta_exatamente, sem_excecao

GOLDEN_DIR = Path(__file__).resolve().parent.parent / "golden_data" / "rnc" / "snpc_20260906"


@pytest.fixture
def install_transport(monkeypatch):
    original = httpx.AsyncClient

    def install(handler: Callable[[httpx.Request], httpx.Response]) -> None:
        monkeypatch.setattr(
            client.httpx,
            "AsyncClient",
            lambda **kwargs: original(transport=httpx.MockTransport(handler), **kwargs),
        )

    return install


def _form(kind: str, token: str) -> bytes:
    return (
        (GOLDEN_DIR / f"{kind}_form.html")
        .read_bytes()
        .replace(b"REDACTED_CSRF_TOKEN", token.encode())
    )


class PublicSession:
    def __init__(self, kind: str = "protegidas", fault: str = "", phase: str = "search") -> None:
        self.kind = kind
        self.fault = fault
        self.phase = phase
        self.gets = 0
        self.posts: list[dict[str, list[str]]] = []
        self.used_tokens: set[str] = set()
        self.failed = False
        self.csv = (GOLDEN_DIR / f"{kind}.csv").read_bytes()
        self.export_type = "text/csv; charset=UTF-8"
        self.export_url = json.loads((GOLDEN_DIR / "expected.json").read_text(encoding="utf-8"))[
            kind
        ]["source_url"]

    def __call__(self, request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            self.gets += 1
            return httpx.Response(
                200,
                content=_form(self.kind, f"fresh-{self.gets}"),
                headers={
                    "Set-Cookie": "public_session=fixture; Path=/",
                    "Content-Type": "text/html",
                },
            )
        assert "public_session=fixture" in request.headers.get("cookie", "")
        data = parse_qs(request.content.decode())
        token = data["csrf_token"][0]
        assert token == f"fresh-{self.gets}"
        assert token not in self.used_tokens
        self.used_tokens.add(token)
        self.posts.append(data)
        phase = "export" if "exportar" in data else "search"
        if self.fault and phase == self.phase and not self.failed:
            self.failed = True
            if self.fault == "timeout":
                raise httpx.ReadTimeout("connection interrupted", request=request)
            if self.fault == "cancel":
                raise asyncio.CancelledError
            if self.fault == "csrf":
                return httpx.Response(
                    403, text="Requisição inválida. Token CSRF ausente ou incorreto."
                )
            return httpx.Response(503)
        if phase == "search":
            assert data["acao"] == ["Pesquisar"]
            assert data["postado"] == ["1"]
            return httpx.Response(
                200, content=(GOLDEN_DIR / f"{self.kind}_search.html").read_bytes()
            )
        assert str(request.url) == self.export_url
        assert data["exportar"] == ["csv"]
        return httpx.Response(200, content=self.csv, headers={"Content-Type": self.export_type})


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("fault", "reason"),
    [
        ("removed", "Formulario CSRF ausente"),
        ("empty", "Token CSRF ausente"),
        ("get", "Formulario CSRF ausente"),
        ("orphan", "Formulario CSRF ausente"),
    ],
)
async def test_missing_csrf_is_layout_error_without_post(fault, reason, install_transport):
    requests = []
    changes = {
        "removed": (b'name="csrf_token"', b'name="removed"'),
        "empty": (b"REDACTED_CSRF_TOKEN", b"REDACTED_CSRF_TOKEN"),
        "get": (b'method="POST"', b'method="GET"'),
        "orphan": (b"<form ", b"<div "),
    }

    def handler(request):
        requests.append(request)
        old, new = changes[fault]
        content = _form("protegidas", "" if fault == "empty" else "example")
        return httpx.Response(200, content=content.replace(old, new))

    install_transport(handler)
    with levanta_exatamente(ParseError, match=reason):
        await client.fetch_protegidas_bundle()
    assert [request.method for request in requests] == ["GET"]


@pytest.mark.asyncio
async def test_export_outside_family_page_is_rejected(install_transport, monkeypatch):
    body = (GOLDEN_DIR / "protegidas.csv").read_bytes()
    search = (
        (GOLDEN_DIR / "protegidas_search.html")
        .read_bytes()
        .replace(b'action="?acao=', b'action="cultivares_registradas.php?acao=')
    )
    requests = []

    def handler(request):
        requests.append(request)
        if request.method == "GET":
            return httpx.Response(200, content=_form("protegidas", f"token-{len(requests)}"))
        if b"exportar=csv" in request.content:
            return httpx.Response(200, content=body, headers={"Content-Type": "text/csv"})
        return httpx.Response(200, content=search)

    install_transport(handler)
    monkeypatch.setattr(client, "MIN_CSV_SIZE", 1)
    with levanta_exatamente(ParseError, match="Metadados da aquisição"):
        await client.fetch_protegidas_bundle()
    assert requests[-1].url.path.endswith("/cultivares_registradas.php")


@pytest.mark.asyncio
async def test_csrf_rejection_exhausts_retry_without_token_leak(install_transport, caplog):
    requests = []
    token = "sensitive-public-session-token"

    def handler(request):
        requests.append(request)
        if request.method == "GET":
            return httpx.Response(200, content=_form("protegidas", token))
        return httpx.Response(403, text="Token CSRF ausente ou incorreto")

    install_transport(handler)
    with levanta_exatamente(SourceUnavailableError, match="CSRF") as error:
        await client.fetch_protegidas_bundle()
    assert [request.method for request in requests] == ["GET", "POST"] * 3
    assert token not in str(error.value)
    assert token not in caplog.text


@pytest.mark.asyncio
async def test_other_forbidden_is_not_retried(install_transport):
    requests = []

    def handler(request):
        requests.append(request)
        if request.method == "GET":
            return httpx.Response(200, content=_form("protegidas", "token"))
        return httpx.Response(403, text="Forbidden")

    install_transport(handler)
    with levanta_exatamente(SourceUnavailableError, match="HTTP 403: a fonte recusou o pedido"):
        await client.fetch_protegidas_bundle()
    assert len(requests) == 2


@pytest.mark.asyncio
async def test_form_page_http_error_is_source_unavailable(install_transport):
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(404, text="Not Found")

    install_transport(handler)
    with levanta_exatamente(SourceUnavailableError, match="HTTP 404: o recurso não existe na URL"):
        await client.fetch_protegidas_bundle()
    assert [request.method for request in requests] == ["GET"]


@pytest.mark.asyncio
async def test_non_retried_transport_error_is_source_unavailable_with_cause(install_transport):
    requests = []

    def handler(request):
        requests.append(request)
        raise httpx.ProxyError("proxy recusou", request=request)

    install_transport(handler)
    with levanta_exatamente(SourceUnavailableError, match="ProxyError: proxy recusou") as caught:
        await client.fetch_protegidas_bundle()
    assert isinstance(caught.value.__cause__, httpx.ProxyError)
    assert len(requests) == 1


@pytest.mark.asyncio
async def test_get_failure_retries_before_post(install_transport, monkeypatch):
    server = PublicSession()
    requests = []

    def handler(request):
        requests.append(request)
        if len(requests) == 1:
            return httpx.Response(503)
        return server(request)

    install_transport(handler)
    monkeypatch.setattr(client, "MIN_CSV_SIZE", 1)
    with sem_excecao():
        await client.fetch_protegidas_bundle()
    assert [request.method for request in requests] == ["GET", "GET", "POST", "GET", "POST"]


@pytest.mark.parametrize(
    "action",
    [
        "https://outside.example/export",
        "//outside.example/export",
        "http://sistemas.agricultura.gov.br/export",
        "https://user:password@sistemas.agricultura.gov.br/export",
        "http://[invalid",
        "https://sistemas.agricultura.gov.br:8443/export",
        "https://[invalid]/export",
    ],
)
def test_form_rejects_invalid_or_foreign_origin(action):
    reason = "URL de formulario invalida" if "[invalid]" in action else "origem diferente"
    with levanta_exatamente(ParseError, match=reason):
        client._form_url(client._PROTEGIDAS_URL, action)


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["search", "export"])
async def test_form_page_from_another_origin_is_rejected_before_post(
    phase, install_transport, monkeypatch
):
    server = PublicSession()
    redirected = 1 if phase == "search" else 2
    gets = []
    requests = []

    def handler(request):
        requests.append(request)
        if request.url.host == "outside.example":
            return httpx.Response(200, content=_form("protegidas", "foreign"))
        if request.method == "GET":
            gets.append(request)
            if len(gets) == redirected:
                return httpx.Response(302, headers={"Location": "https://outside.example/form"})
        return server(request)

    install_transport(handler)
    monkeypatch.setattr(client, "MIN_CSV_SIZE", 1)
    with levanta_exatamente(ParseError, match="origem diferente"):
        await client.fetch_protegidas_bundle()
    assert requests[-1].url.host == "outside.example"
    assert len(server.posts) == redirected - 1


@pytest.mark.asyncio
async def test_post_redirect_does_not_forward_token(install_transport):
    requests = []

    def handler(request):
        requests.append(request)
        if request.method == "GET":
            return httpx.Response(200, content=_form("protegidas", "token"))
        return httpx.Response(307, headers={"Location": "https://outside.example/leak"})

    install_transport(handler)
    with levanta_exatamente(SourceUnavailableError, match="HTTP 307: Temporary Redirect"):
        await client.fetch_protegidas_bundle()
    assert len(requests) == 2


@pytest.mark.asyncio
async def test_missing_export_form_is_layout_error(install_transport):
    server = PublicSession()

    def handler(request):
        response = server(request)
        if request.method == "POST":
            return httpx.Response(
                200, content=response.content.replace(b'value="csv"', b'value="removed"')
            )
        return response

    install_transport(handler)
    with levanta_exatamente(ParseError, match="Formulario de exportacao CSV ausente"):
        await client.fetch_protegidas_bundle()
    assert len(server.posts) == 1


@pytest.mark.asyncio
async def test_fetch_raises_on_small_csv(install_transport):
    server = PublicSession()
    install_transport(server)
    with levanta_exatamente(SourceUnavailableError, match="too small"):
        await client.fetch_protegidas_bundle()


@pytest.mark.asyncio
@pytest.mark.parametrize("disguise", ["body", "content_type"])
async def test_export_html_is_rejected_above_size_threshold(disguise, install_transport):
    server = PublicSession()
    if disguise == "body":
        server.csv = b"<!doctype html><html>" + b"unavailable " * 50_000 + b"</html>"
    else:
        server.csv *= client.MIN_CSV_SIZE // len(server.csv) + 1
        server.export_type = "text/html; charset=UTF-8"
    install_transport(server)
    with levanta_exatamente(ParseError, match="retornou HTML em vez de CSV"):
        await client.fetch_protegidas_bundle()
