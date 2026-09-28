from __future__ import annotations

import hashlib
import importlib
import json
import warnings
from datetime import date, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import httpx
import pandas as pd
import pytest

from agrobr import constants, deterministic
from agrobr.exceptions import InvalidParameterError, ParseError, SourceUnavailableError
from agrobr.incra.andamento import api, client, models, parser
from tests.test_incra import replay

FIXTURE = replay.JUNE
SEPTEMBER = replay.SEPTEMBER
PAGE_URL = replay.PAGE_URL
FIELD_MAP = {
    "regional": "regional_layout",
    "processo": "processo_texto",
    "comunidade": "comunidade_texto",
    "municipio": "municipio_texto",
    "edital_rtid_1": "edital_rtid_1_texto",
    "edital_rtid_2": "edital_rtid_2_texto",
    "retificacao_edital_1": "retificacao_edital_1_texto",
    "retificacao_edital_2": "retificacao_edital_2_texto",
    "portaria": "portaria_texto",
    "retificacao_portaria": "retificacao_portaria_texto",
    "decreto": "decreto_texto",
    "titulo": "titulo_texto",
}


def test_andamento_september_all_cells_match_pdfium_oracle(september_publication):
    oracle = json.loads((SEPTEMBER / "oracle.json").read_bytes())
    assert not isinstance(september_publication, ParseError), september_publication
    assert september_publication.frame.to_dict("records") == [
        row["cells"] for row in oracle["records"]
    ]
    assert september_publication.declared_total == oracle["declared_total"][0] == 649
    assert september_publication.edition == date(2026, 9, 3)
    assert oracle["edition_dates_before_source_line"] == ["03/09/2026"]


@pytest.mark.slow
def test_andamento_real_all_cells_match_independent_oracle(june_publication):
    oracle = json.loads((FIXTURE / "oracle.json").read_bytes())
    expected = [
        {
            name: int(row["cells"][name])
            if name == "numero_publicado"
            else row["cells"][FIELD_MAP.get(name, name)]
            for name in models.columns()
        }
        for row in oracle
    ]
    assert not isinstance(june_publication, ParseError), june_publication
    assert june_publication.frame.to_dict("records") == expected
    assert june_publication.declared_total == len(expected)
    assert june_publication.edition == date(2026, 6, 8)
    assert june_publication.page_count == 8
    assert (
        june_publication.source_sha256
        == hashlib.sha256((FIXTURE / "publication.pdf").read_bytes()).hexdigest()
    )


def test_andamento_real_dtypes_and_empty_strings(june_publication):
    assert str(june_publication.frame.numero_publicado.dtype) == "Int64"
    for name in models.columns():
        if name != "numero_publicado":
            assert june_publication.frame[name].dtype == pd.StringDtype(storage="python")
    assert not june_publication.frame.isna().any().any()
    assert june_publication.frame.area_ha_texto.eq("").sum() == 237


def test_andamento_real_structural_groups_and_clipping(june_publication):
    assert len(june_publication.regional_groups) == 27
    assert sum(len(group.pages) > 1 for group in june_publication.regional_groups) == 6
    assert june_publication.frame.iloc[45].comunidade.endswith("\n(Volta)")
    assert june_publication.frame.iloc[46].comunidade == "Olhos D'Água do Basílio"
    assert june_publication.frame.iloc[63].comunidade.endswith("\nTabatinga")
    assert june_publication.frame.iloc[64].comunidade == "Iúna"
    assert june_publication.frame.iloc[113].comunidade.endswith("\nBom Jardim")
    assert june_publication.frame.iloc[114].comunidade == "Três Irmãos"
    assert june_publication.diagnostics["partially_clipped_glyphs"] == 1349


def test_andamento_pdf_page_budget_fails(monkeypatch):
    monkeypatch.setattr(constants, "INCRA_ANDAMENTO_MAX_PAGES", 1)
    with pytest.raises(ParseError, match="Orçamento"):
        parser.parse_publication((FIXTURE / "publication.pdf").read_bytes())


def test_andamento_record_after_total_rejected():
    borders = [float(index * 10) for index in range(16)]
    table = SimpleNamespace(
        extract=lambda: [
            ["TOTAL", "1 processos com algum tipo de andamento no INCRA"] + [None] * 13,
            [None, "1"] + [None] * 13,
        ],
        rows=[
            SimpleNamespace(cells=[]),
            SimpleNamespace(cells=[None, (10.0, 100.0, 20.0, 110.0), *[None] * 13]),
        ],
    )
    with pytest.raises(ValueError, match="após total"):
        parser._table_rows(table, 1, borders)


def test_andamento_header_with_swapped_acts_rejected():
    header = list(constants.INCRA_ANDAMENTO_HEADERS)
    header[11], header[13] = header[13], header[11]
    boxes = [(float(index), 0.0, index + 1.0, 1.0) for index in range(15)]
    table = SimpleNamespace(
        extract=lambda: [["ANDAMENTO DOS PROCESSOS - QUADRO GERAL", *[None] * 14], header],
        rows=[None, SimpleNamespace(cells=boxes)],
    )
    with pytest.raises(ValueError, match="Cabeçalho administrativo divergente"):
        parser._header(table)


def test_andamento_hidden_data_glyph_rejected(monkeypatch):
    backend = importlib.import_module("agrobr.incra.andamento._pdf")
    original = backend.collect

    def collect(page):
        device = original(page)
        glyph = next(item for item in device.glyphs if 140 < item["bbox"][0] < 200)
        left, bottom, right, _ = glyph["bbox"]
        device.glyphs.append({**glyph, "clip": (left, bottom - 50, right, bottom - 40)})
        return device

    monkeypatch.setattr(backend, "collect", collect)
    with pytest.raises(ParseError, match="totalmente cortado"):
        parser.parse_publication((SEPTEMBER / "publication.pdf").read_bytes())


@pytest.mark.slow
def test_andamento_declared_total_above_rows_rejected(monkeypatch):
    original = parser._table_rows

    def inflate_total(table, page_number, borders):
        rows, total, footer = original(table, page_number, borders)
        return rows, None if total is None else total + 1, footer

    monkeypatch.setattr(parser, "_table_rows", inflate_total)
    with pytest.raises(ParseError, match="População"):
        parser.parse_publication((SEPTEMBER / "publication.pdf").read_bytes())


@pytest.mark.parametrize(
    "href",
    [
        "https://example.com/incra/pt-br/assuntos/governanca-fundiaria/quilombolas/andamento_dos_processos_quilombolas-08_06_2026.pdf/@@display-file/file",
        "/incra/andamento_dos_processos_quilombolas-08_06_2026.pdf",
        PAGE_URL + "/andamento_dos_processos_quilombolas-99_06_2026.pdf/@@display-file/file",
    ],
)
def test_andamento_page_invalid_resource_rejected(href):
    with pytest.raises(ParseError):
        client.resolve_edition(f'<a href="{href}">Andamento</a>'.encode(), PAGE_URL)


def test_andamento_page_ambiguous_current_edition_rejected():
    first = (FIXTURE / "publisher.html").read_bytes()
    extra = f'<a href="{PAGE_URL}/andamento_dos_processos_quilombolas-09_06_2026.pdf/@@display-file/file">Outra</a>'.encode()
    with pytest.raises(ParseError, match="exatamente um"):
        client.resolve_edition(first.replace(b"</body>", extra + b"</body>"), PAGE_URL)


@pytest.mark.parametrize(
    "value", [True, 20260608, "08/06/2026", "2026-6-8", "2026-02-30", datetime(2026, 6, 8)]
)
def test_andamento_edition_guard_rejects_invalid(value):
    with pytest.raises(InvalidParameterError):
        api.validate_edition(value)


@pytest.mark.parametrize(
    "kwargs,error",
    [
        ({"uf": "BA"}, TypeError),
        ({"as_polars": 1}, InvalidParameterError),
        ({"return_meta": "yes"}, InvalidParameterError),
        ({"edicao": "2026-02-30"}, InvalidParameterError),
    ],
)
async def test_andamento_guards_precede_optional_and_http(monkeypatch, kwargs, error):
    optional = Mock(side_effect=AssertionError("optional before guards"))
    fetch = AsyncMock(side_effect=AssertionError("HTTP before guards"))
    monkeypatch.setattr(parser, "check_pdf", optional)
    monkeypatch.setattr(client, "fetch_publication", fetch)
    with pytest.raises(error):
        await api.andamento_quilombola(**kwargs)
    optional.assert_not_called()
    fetch.assert_not_called()


async def test_andamento_redirect_is_classified_before_http_status():
    downloader = client.Download()
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                302, headers={"location": "https://example.com/bloqueio"}, request=request
            )
        )
    ) as http:
        with pytest.raises((SourceUnavailableError, httpx.HTTPStatusError)) as caught:
            await downloader.fetch(http, PAGE_URL, "publisher")
    assert caught.type is SourceUnavailableError
    assert "Redirecionamento administrativo" in str(caught.value)
    assert [resource.status for resource in downloader.resources] == [302]


async def test_andamento_download_body_budget_preserves_partial(monkeypatch):
    monkeypatch.setattr(constants, "INCRA_ANDAMENTO_MAX_BODY_BYTES", 2)
    downloader = client.Download()
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _request: httpx.Response(200, content=b"abc"))
    ) as http:
        with pytest.raises(SourceUnavailableError, match="Orçamento"):
            await downloader.fetch(http, PAGE_URL, "publisher")
    resource = downloader.resources[0]
    assert resource.size_bytes == 3 and not resource.complete_body
    assert resource.sha256 == hashlib.sha256(b"abc").hexdigest()


async def test_andamento_download_total_budget_counts_previous_response(monkeypatch):
    monkeypatch.setattr(constants, "INCRA_ANDAMENTO_MAX_TOTAL_BODY_BYTES", 5)
    downloader = client.Download()
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _request: httpx.Response(200, content=b"abc"))
    ) as http:
        await downloader.request(http, PAGE_URL, "publisher")
        with pytest.raises(SourceUnavailableError, match="Orçamento"):
            await downloader.request(http, PAGE_URL, "pdf")
    assert downloader.total_bytes == 6
    assert [resource.size_bytes for resource in downloader.resources] == [3, 3]


async def test_andamento_download_attempt_budget_prevents_send(monkeypatch):
    monkeypatch.setattr(constants, "INCRA_ANDAMENTO_MAX_ATTEMPTS", 1)
    sent = Mock(return_value=httpx.Response(200, content=b"body"))
    downloader = client.Download()
    async with httpx.AsyncClient(transport=httpx.MockTransport(sent)) as http:
        await downloader.request(http, PAGE_URL, "publisher")
        with pytest.raises(SourceUnavailableError, match="envios"):
            await downloader.request(http, PAGE_URL, "pdf")
    assert sent.call_count == 1


@pytest.mark.parametrize("edicao", [None, "2026-09-03", date(2026, 9, 3)])
async def test_andamento_republished_file_uses_internal_edition(
    monkeypatch, september_publication, edicao
):
    pdf = (SEPTEMBER / "publication.pdf").read_bytes()

    def parse(content):
        assert content == pdf
        return september_publication

    replay.install_publication(monkeypatch, folder=SEPTEMBER)
    monkeypatch.setattr(parser, "parse_publication", parse)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        frame, meta = await api.andamento_quilombola(edicao=edicao, return_meta=True)
    messages = [str(item.message) for item in caught if "arquivo nomeado" in str(item.message)]
    assert messages == [
        "INCRA: arquivo nomeado 08/06/2026, conteúdo de 03/09/2026; usada a data interna"
    ]
    assert len(frame) == 649 and meta.validation_warnings[-1] == messages[0]
    details = meta.source_details
    assert details["query"]["resolved_edition"] == "2026-09-03"
    assert details["publication"]["internal_edition"] == "2026-09-03"
    assert details["publication"]["file_date"] == "2026-06-08"


async def test_andamento_unpublished_edition_names_current_publication(
    monkeypatch, september_publication
):
    replay.install_publication(monkeypatch, folder=SEPTEMBER)
    monkeypatch.setattr(parser, "parse_publication", lambda _content: september_publication)
    with pytest.raises(InvalidParameterError, match="2026-06-08.*2026-09-03"):
        await api.andamento_quilombola(edicao="2026-06-08")


async def test_andamento_client_invalid_pdf_preserves_successful_http_receipts(monkeypatch):
    calls = replay.install_publication(monkeypatch, pdf_body=b"<html>error</html>")
    with pytest.raises(ParseError) as caught:
        await client.fetch_publication()
    assert len(calls) == 2
    assert [resource["status"] for resource in caught.value.resources] == [200, 200]


@pytest.mark.parametrize("body", [b"", b"<html>error</html>"])
def test_andamento_malformed_document_fails(body):
    with pytest.raises(ParseError, match="assinatura"):
        parser.parse_publication(body)


def test_andamento_additional_data_table_rejected(monkeypatch):
    pdfplumber = parser.check_pdf()
    original = pdfplumber.page.Page.find_tables

    def duplicated_table(page, *args, **kwargs):
        tables = original(page, *args, **kwargs)
        return [*tables, tables[0]] if page.page_number == 1 else tables

    monkeypatch.setattr(pdfplumber.page.Page, "find_tables", duplicated_table)
    with pytest.raises(ParseError, match="tabelas adicionais"):
        parser.parse_publication((FIXTURE / "publication.pdf").read_bytes())


def test_andamento_group_multiple_labels_rejected():
    group = parser.Groups()
    box = models.OrdinalCell(ordinal=1, page=1, top=10.0, bottom=20.0)
    with pytest.raises(ValueError, match="exatamente um"):
        group.add_page(
            [box],
            [{}],
            [
                {"label": "SR(01)PA", "text_object": 1, "y": 15.0},
                {"label": "SR(02)CE", "text_object": 2, "y": 16.0},
            ],
            [{"top_y": 20.0}],
        )


async def test_andamento_deterministic_precedes_optional_and_http(monkeypatch):
    optional = Mock(side_effect=AssertionError("optional before guards"))
    monkeypatch.setattr(parser, "check_pdf", optional)
    async with deterministic("2026-09-07"):
        with pytest.raises(InvalidParameterError, match="deterministic"):
            await api.andamento_quilombola()
    optional.assert_not_called()


async def test_andamento_api_missing_polars_precedes_http(monkeypatch):
    fetch = AsyncMock()
    monkeypatch.setattr(client, "fetch_publication", fetch)
    monkeypatch.setattr(parser, "check_pdf", lambda: None)
    monkeypatch.setattr(
        api, "importlib", SimpleNamespace(import_module=Mock(side_effect=ImportError("absent")))
    )
    with pytest.raises(ImportError, match="polars"):
        await api.andamento_quilombola(as_polars=True)
    fetch.assert_not_called()
