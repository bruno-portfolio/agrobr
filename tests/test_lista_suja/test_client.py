from __future__ import annotations

import hashlib
from datetime import UTC

import httpx
import pytest

from agrobr.exceptions import InvalidParameterError, ParseError, SourceUnavailableError
from agrobr.http import user_agents
from agrobr.lista_suja import client, discovery, models
from tests import helpers


@pytest.mark.parametrize("formato", ["auto", "csv", "pdf"])
async def test_replay_discovers_main_publication_and_selects_requested_format(
    formato, replay_http, publication_files
):
    requests = replay_http()
    acquired = await client.fetch_empregadores(formato=formato)
    selected = "pdf" if formato == "pdf" else "csv"
    assert isinstance(acquired, models.Acquisition)
    assert acquired.formato == selected
    assert acquired.selected_source == f"lista_suja_{selected}"
    assert acquired.attempted_sources == [f"lista_suja_{selected}"]
    assert acquired.resource.content == publication_files[selected]
    assert acquired.resource.sha256 == hashlib.sha256(publication_files[selected]).hexdigest()
    assert acquired.resource.size_bytes == len(publication_files[selected])
    assert acquired.resource.fetched_at.utcoffset() == UTC.utcoffset(None)
    assert acquired.discovery.content == publication_files["html"]
    assert acquired.discovery.url == publication_files["url"]
    assert acquired.fallback is None
    assert all("2026_0014" not in request.url.path for request in requests)
    assert {request.headers["user-agent"] for request in requests} == {user_agents.get_bot_ua()}
    if selected == "csv":
        assert acquired.companion is not None
        assert acquired.companion.content == publication_files["txt"]
        assert not any(request.url.path.endswith(".pdf") for request in requests)
    else:
        assert acquired.companion is None
        assert not any(request.url.path.endswith((".csv", ".txt")) for request in requests)


@pytest.mark.parametrize("status", [403, 404, 408, 410, 429, 500, 501, 503])
async def test_auto_http_eligible_failure_selects_pdf_and_preserves_cause(status, replay_http):
    requests = replay_http({"csv": (status, b"unavailable", "text/plain")})
    with helpers.sem_excecao():
        acquired = await client.fetch_empregadores()
    assert acquired.selected_source == "lista_suja_pdf"
    assert acquired.attempted_sources == ["lista_suja_csv", "lista_suja_pdf"]
    assert acquired.fallback["reason"] == "transport_unavailable"
    assert acquired.fallback["status_code"] == status
    assert acquired.fallback["url"].endswith("cadastro_de_empregadores.csv")
    assert acquired.warnings
    assert any(request.url.path.endswith(".pdf") for request in requests)
    assert not any(request.url.path.endswith(".txt") for request in requests)


@pytest.mark.parametrize(
    "error_type,recorded",
    [
        (httpx.ReadTimeout, "SourceUnavailableError"),
        (httpx.ConnectError, "SourceUnavailableError"),
        (httpx.RemoteProtocolError, "SourceUnavailableError"),
        (httpx.ProxyError, "ProxyError"),
    ],
)
async def test_auto_transport_failure_can_select_pdf(error_type, recorded, replay_http):
    replay_http({"csv": error_type("simulated transport failure")})
    with helpers.sem_excecao():
        acquired = await client.fetch_empregadores()
    assert acquired.formato == "pdf"
    assert acquired.attempted_sources == ["lista_suja_csv", "lista_suja_pdf"]
    assert acquired.fallback["reason"] == "transport_unavailable"
    assert acquired.fallback["error_type"] == recorded
    assert "simulated transport failure" in acquired.fallback["error"]
    assert "status_code" not in acquired.fallback


@pytest.mark.parametrize("status", [400, 401, 422])
async def test_noneligible_csv_http_failure_does_not_select_pdf(status, replay_http):
    requests = replay_http({"csv": (status, b"request rejected", "text/plain")})
    with pytest.raises(SourceUnavailableError, match=f"HTTP {status}"):
        await client.fetch_empregadores()
    assert not any(request.url.path.endswith(".pdf") for request in requests)


@pytest.mark.parametrize("formato,status", [("csv", 404), ("csv", 503), ("pdf", 404)])
async def test_explicit_format_never_uses_another_format(formato, status, replay_http):
    requests = replay_http({formato: (status, b"unavailable", "text/plain")})
    with pytest.raises(SourceUnavailableError):
        await client.fetch_empregadores(formato=formato)
    other = ".pdf" if formato == "csv" else ".csv"
    assert not any(request.url.path.endswith(other) for request in requests)


@pytest.mark.parametrize(
    "content",
    [
        b"",
        b"<html>login</html>",
        b"Erro 503 <html><body>indisponivel</body></html>",
        b"<?xml version='1.0'?><erro/>",
        b"%PDF-wrong resource",
        b"PK\x03\x04workbook",
        b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1",
        b"header\x00body",
    ],
)
async def test_successful_http_with_invalid_csv_body_does_not_fall_back(content, replay_http):
    requests = replay_http({"csv": (200, content, "text/csv")})
    with helpers.levanta_exatamente(ParseError, match="Corpo CSV incompatível"):
        await client.fetch_empregadores()
    assert not any(request.url.path.endswith(".pdf") for request in requests)


async def test_pdf_com_corpo_que_nao_e_pdf_falha_na_aquisicao(replay_http):
    replay_http({"pdf": (200, b"<html>login</html>", "application/pdf")})
    with helpers.levanta_exatamente(ParseError, match="Corpo PDF incompatível"):
        await client.fetch_empregadores(formato="pdf")


async def test_txt_http_failure_leaves_companion_absent_with_warning(replay_http):
    replay_http({"txt": (404, b"unavailable", "text/plain")})
    acquired = await client.fetch_empregadores()
    assert acquired.formato == "csv"
    assert acquired.companion is None
    assert acquired.warnings
    assert acquired.attempted_sources == ["lista_suja_csv"]


@pytest.mark.parametrize("formato", [None, 1, [], "xlsx", "AUTO"])
async def test_invalid_format_fails_before_http(formato, replay_http):
    requests = replay_http()
    with pytest.raises(InvalidParameterError):
        await client.fetch_empregadores(formato=formato)
    assert requests == []


@pytest.mark.parametrize(
    "html,message",
    [
        ('<h2>CEAC</h2><a href="documentos-outros/2026_0014.csv">CSV</a>', "sem recursos"),
        (
            '<h2>Lista Suja</h2><a href="one.csv">CSV</a><a href="two.csv">CSV</a>',
            "Mais de um recurso principal para csv",
        ),
        (
            '<table><tr><td><p>Lista Suja</p><p>CEAC</p></td><td><p><a href="one.csv">CSV</a></p></td></tr></table>',
            "sem correspondência entre títulos e links",
        ),
        (
            '<p>Layout desconhecido</p><a href="cadastro_de_empregadores.pdf">PDF antigo</a>',
            "sem recursos",
        ),
        ('<h2>Lista Suja</h2><a href="https://example.org/data.csv">CSV</a>', "fora do domínio"),
        (
            '<h2>Lista Suja</h2><p><a href="um.csv">CSV</a> <a href="dois.csv">CSV</a></p>',
            "Publicação ambígua para formato csv",
        ),
        (
            '<h2>Lista Suja</h2><a href="um.csv">CSV</a><h2>Lista Suja</h2><a href="dois.csv">CSV</a>',
            "publicações principais ambíguas",
        ),
    ],
)
def test_ambiguous_or_unrecognized_discovery_is_an_error(html, message, publication_files):
    with helpers.levanta_exatamente(ParseError, match=message):
        discovery.parse_publication(html.encode(), publication_files["url"])


@pytest.mark.parametrize(
    "html",
    [
        '<h2>Lista Suja</h2><p><a href="cadastro_de_empregadores">Baixar (.csv)</a></p>',
        '<h2>Lista Suja</h2><p><a href="cadastro_de_empregadores.csv">CSV</a><a href="ceac/outro.csv">CSV</a></p>',
        '<table><tr></tr><tr><td>Lista Suja</td><td><a href="cadastro_de_empregadores.csv">CSV</a></td></tr></table>',
        '<h2>Lista Suja</h2> texto <a href="cadastro_de_empregadores.csv">CSV</a>',
        '<table><tr><td>Lista Suja</td><td><a href="cadastro_de_empregadores.csv">CSV</a> <a href="outro.csv">CSV do CEAC</a></td></tr></table>',
        '<h2>Lista Suja</h2><p><a href="cadastro_de_empregadores.csv">CSV</a><a href="outro.csv"><img src="x.png"/></a></p>',
        '<h2>Lista Suja</h2><a href="cadastro_de_empregadores.csv">CSV</a><h3>Outras publicações</h3><a href="outro.csv">CSV</a>',
        '<h2>Lista Suja</h2><a href="cadastro_de_empregadores.csv">CSV</a><p>Cadastro CEAC</p><a href="outro.csv">CSV</a>',
    ],
    ids=[
        "extensao_no_rotulo",
        "link_ceac_ignorado",
        "linha_vazia",
        "texto_solto",
        "ceac_no_rotulo",
        "link_sem_rotulo",
        "outro_titulo_encerra",
        "paragrafo_ceac_encerra",
    ],
)
def test_discovery_aceita_variacoes_do_bloco_principal(html, publication_files):
    with helpers.sem_excecao():
        publication = discovery.parse_publication(html.encode(), publication_files["url"])
    arquivo = "cadastro_de_empregadores" + ("" if "(.csv)" in html else ".csv")
    assert publication.resources == {
        "csv": f"{publication_files['url'].rsplit('/', 1)[0]}/{arquivo}"
    }


async def test_txt_com_erro_nao_elegivel_propaga_em_vez_de_virar_aviso(replay_http):
    requests = replay_http({"txt": (400, b"rejected", "text/plain")})
    with helpers.levanta_exatamente(SourceUnavailableError, "HTTP 400"):
        await client.fetch_empregadores()
    assert not any(request.url.path.endswith(".pdf") for request in requests)


async def test_formato_explicito_nao_anunciado_nao_troca_de_formato(replay_http):
    html = b'<h2>Lista Suja</h2><a href="cadastro_de_empregadores.pdf">PDF</a>'
    requests = replay_http({"portal": (200, html, "text/html")})
    with helpers.levanta_exatamente(SourceUnavailableError, match="Formato csv não anunciado"):
        await client.fetch_empregadores(formato="csv")
    assert len(requests) == 1


async def test_erro_que_nao_e_de_transporte_nem_http_nao_aciona_o_pdf(replay_http):
    requests = replay_http({"csv": httpx.TooManyRedirects("loop")})
    with helpers.levanta_exatamente(httpx.TooManyRedirects):
        await client.fetch_empregadores()
    assert not any(request.url.path.endswith(".pdf") for request in requests)


async def test_csv_not_advertised_can_select_pdf_without_claiming_csv_attempt(replay_http):
    html = b'<h2>Lista Suja</h2><a href="cadastro_de_empregadores.pdf">PDF</a>'
    replay_http({"portal": (200, html, "text/html")})
    acquired = await client.fetch_empregadores()
    assert acquired.attempted_sources == ["lista_suja_pdf"]
    assert acquired.fallback["reason"] == "csv_not_advertised"


async def test_csv_without_advertised_txt_retains_warning(replay_http):
    html = b'<h2>Lista Suja</h2><a href="cadastro_de_empregadores.csv">CSV</a>'
    replay_http({"portal": (200, html, "text/html")})
    acquired = await client.fetch_empregadores()
    assert acquired.companion is None
    assert acquired.warnings


@pytest.mark.parametrize(
    "url",
    [
        "http://www.gov.br/trabalho-e-emprego/pt-br/assuntos/inspecao-do-trabalho/c.csv",
        "https://dados.gov.br/trabalho-e-emprego/pt-br/assuntos/inspecao-do-trabalho/c.csv",
        "https://u@www.gov.br/trabalho-e-emprego/pt-br/assuntos/inspecao-do-trabalho/c.csv",
        "https://www.gov.br/trabalho-e-emprego/pt-br/assuntos/outro/c.csv",
        "https://www.gov.br/trabalho-e-emprego/pt-br/assuntos/inspecao-do-trabalho/%2e%2e/outro/c.csv",
    ],
    ids=["http", "outro_host", "usuario", "fora_do_caminho", "ponto_ponto_codificado"],
)
def test_validate_url_recusa_fora_do_dominio_e_do_caminho_oficiais(url, publication_files):
    with helpers.levanta_exatamente(ParseError, match="fora do domínio/caminho oficial"):
        discovery.validate_url(url, publication_files["url"])


def test_validate_url_resolve_link_relativo_sem_fragmento(publication_files):
    pasta = publication_files["url"].rsplit("/", 1)[0]
    resolvida = discovery.validate_url(
        "cadastro_de_empregadores.csv#topo", publication_files["url"]
    )
    assert resolvida == f"{pasta}/cadastro_de_empregadores.csv"


@pytest.mark.parametrize(
    "destino",
    [
        "https://www.gov.br/trabalho-e-emprego/pt-br/assuntos/inspecao-do-trabalho/arquivos/cadastro_de_empregadores.csv",
        "https://example.org/cadastro_de_empregadores.csv",
    ],
    ids=["caminho_oficial", "outro_dominio"],
)
async def test_redirecionamento_do_csv_valida_o_destino_e_registra_as_duas_urls(
    destino, publication_files, monkeypatch
):
    original = httpx.AsyncClient
    anunciados = discovery.parse_publication(
        publication_files["html"], publication_files["url"]
    ).resources

    def responder(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if request.url.path == httpx.URL(publication_files["url"]).path:
            return httpx.Response(200, content=publication_files["html"])
        if url == anunciados["csv"]:
            return httpx.Response(302, headers={"location": destino})
        if url == destino:
            return httpx.Response(200, content=publication_files["csv"])
        if url == anunciados["txt"]:
            return httpx.Response(200, content=publication_files["txt"])
        raise AssertionError(f"URL não anunciada: {url}")

    monkeypatch.setattr(
        httpx,
        "AsyncClient",
        lambda *args, **kwargs: original(*args, transport=httpx.MockTransport(responder), **kwargs),
    )
    if destino.startswith("https://www.gov.br/"):
        with helpers.sem_excecao():
            acquired = await client.fetch_empregadores(formato="csv")
        assert acquired.resource.requested_url == anunciados["csv"]
        assert acquired.resource.url == destino
        assert acquired.resource.content == publication_files["csv"]
    else:
        with helpers.levanta_exatamente(ParseError, match="fora do domínio/caminho oficial"):
            await client.fetch_empregadores(formato="csv")
