from __future__ import annotations

import asyncio
import codecs
import io
from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import AsyncMock

import httpx
import pandas as pd
import pytest

from agrobr import constants
from agrobr.alt.mapa_psr import api, client, models, parser
from agrobr.exceptions import ParseError, ResourceLimitError, SourceUnavailableError
from tests.helpers import RETRY_SLEEP, binary_stream, levanta_exatamente, sem_excecao

HEADER = (
    "ANO_APOLICE;SG_UF_PROPRIEDADE;NM_CULTURA_GLOBAL;NR_APOLICE;"
    "NM_MUNICIPIO_PROPRIEDADE;VALOR_INDENIZAÇÃO;EVENTO_PREPONDERANTE;NR_AREA_TOTAL\n"
)
CSV = (HEADER + "2023;MT;SOJA;A;SORRISO;1.234,56;SECA;100,25\n").encode()
GOLDEN = Path(__file__).parents[1] / "golden_data/mapa_psr/sinistros_sample/response.csv"


class ScriptedStream(httpx.AsyncByteStream):
    def __init__(self, parts, *, error=None, waiting=None):
        self.parts = parts
        self.error = error
        self.waiting = waiting
        self.closed = False
        self.consumed = False

    async def __aiter__(self):
        self.consumed = True
        for part in self.parts:
            yield part
        if self.waiting is not None:
            self.waiting.set()
            await asyncio.Event().wait()
        if self.error is not None:
            raise self.error

    async def aclose(self):
        self.closed = True


@pytest.fixture
def transport(monkeypatch):
    original = httpx.AsyncClient

    def install(handler):
        monkeypatch.setattr(
            client.httpx,
            "AsyncClient",
            lambda **kwargs: original(transport=httpx.MockTransport(handler), **kwargs),
        )

    return install


@pytest.fixture
def temporary_files(monkeypatch, tmp_path):
    original = client.tempfile.TemporaryFile
    files = []

    def create(**kwargs):
        stream = original(dir=tmp_path, **kwargs)
        files.append(stream)
        return stream

    monkeypatch.setattr(client.tempfile, "TemporaryFile", create)
    return files


@pytest.mark.asyncio
async def test_stream_retries_from_zero_after_partial_network_failure(
    transport, temporary_files, monkeypatch
):
    monkeypatch.setattr(RETRY_SLEEP, AsyncMock())
    streams = [
        ScriptedStream([b"partial;" * 10000], error=httpx.ReadError("interrupted")),
        ScriptedStream([CSV[:50], CSV[50:]]),
    ]
    attempts = iter(streams)
    transport(lambda _request: httpx.Response(200, stream=next(attempts)))
    async with client.open_periodo("2016-2024") as stream:
        assert stream.read() == CSV
        assert not stream.closed
    assert all(stream.closed for stream in streams)
    assert len(temporary_files) == 1
    assert temporary_files[0].closed


async def test_orcamento_de_transferencia_inclui_tentativas_anteriores(
    transport, temporary_files, monkeypatch
):
    monkeypatch.setattr(constants, "MAPA_PSR_MAX_TRANSFER_BYTES", 65536 + len(CSV) - 1)
    streams = [
        ScriptedStream([b"x" * 65536], error=httpx.ReadError("interrompido")),
        ScriptedStream([CSV]),
    ]
    attempts = iter(streams)
    transport(lambda _: httpx.Response(200, stream=next(attempts)))
    with pytest.raises(ResourceLimitError, match="orçamento"):
        async with client.open_periodo("2025"):
            raise AssertionError("download acima do limite entregue")
    assert all(stream.closed for stream in streams)
    assert temporary_files[0].closed


@pytest.mark.asyncio
async def test_retryable_status_closes_stream_before_next_attempt(
    transport, temporary_files, monkeypatch
):
    monkeypatch.setattr(RETRY_SLEEP, AsyncMock())
    rejected = ScriptedStream([b"error" * 10000])
    accepted = ScriptedStream([CSV])
    calls = 0

    def handler(_request):
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(503, stream=rejected)
        assert rejected.closed
        assert not rejected.consumed
        return httpx.Response(200, stream=accepted)

    transport(handler)
    async with client.open_periodo("2025") as stream:
        assert stream.read() == CSV
    assert calls == 2
    assert accepted.closed
    assert temporary_files[0].closed


@pytest.mark.parametrize(
    "status,body,error",
    [
        (404, CSV, SourceUnavailableError),
        (200, b"x", SourceUnavailableError),
        (200, b"<html>" + b"x" * 200, SourceUnavailableError),
    ],
)
@pytest.mark.asyncio
async def test_download_failure_never_yields_partial_file(
    status, body, error, transport, temporary_files
):
    response_stream = ScriptedStream([body])
    transport(lambda _request: httpx.Response(status, stream=response_stream))
    with pytest.raises(error):
        async with client.open_periodo("2025"):
            raise AssertionError("download inválido entregue")
    assert response_stream.closed
    assert temporary_files[0].closed


@pytest.mark.parametrize("encoding", ["utf-8", "utf-8-sig", "windows-1252", "iso-8859-1"])
def test_encoding_validates_past_first_ascii_block(encoding):
    text = HEADER.replace("INDENIZAÇÃO", "INDENIZACAO") + (
        "2023;MT;SOJA;A;SORRISO;0;;1\n" * 3000 + "2023;MG;CAFÉ;B;PATROCÍNIO;10;GEADA;2\n"
    )
    if encoding == "iso-8859-1":
        text += "2023;MG;CAFÉ;C;PATROCÍNIO\x81;10;GEADA;2\n"
    with sem_excecao():
        chunks = parser.iter_apolices(
            io.BytesIO(text.encode(encoding)), cultura="cafe", chunk_size=37
        )
        actual = pd.concat(chunks)
    assert actual["nr_apolice"].tolist() == (["B", "C"] if encoding == "iso-8859-1" else ["B"])
    assert actual["cultura"].eq("CAFÉ").all()


def test_invalid_utf8_bom_late_in_file_does_not_return_partial_result():
    content = codecs.BOM_UTF8 + CSV * 500 + b"\xff"
    with levanta_exatamente(ParseError, match="Erro ao ler CSV"):
        parser.parse_apolices(content)


@pytest.mark.parametrize("chunk_size", [1, 2, 10000])
def test_unclosed_quote_after_valid_rows_cannot_return_partial_data(chunk_size):
    content = CSV + b'2023;MT;SOJA;B;"UNFINISHED\n'
    with levanta_exatamente(ParseError, match="Erro ao ler CSV"):
        list(parser.iter_apolices(io.BytesIO(content), chunk_size=chunk_size))


@pytest.mark.parametrize(
    ("content", "motivo"),
    [
        (b"", "CSV vazio"),
        (HEADER.encode(), "CSV vazio"),
        (b"UNKNOWN;FIELD\na;b\n", "Colunas criticas faltando"),
    ],
)
def test_invalid_or_empty_layout_is_an_error_before_filters(content, motivo):
    with levanta_exatamente(ParseError, match=motivo):
        list(parser.iter_apolices(io.BytesIO(content), cultura="absent", chunk_size=1))


def test_only_selected_rows_reach_numeric_conversion(monkeypatch):
    original = parser.parse_numeric_br
    converted = []

    def convert(value):
        converted.append(value)
        return original(value)

    monkeypatch.setattr(parser, "parse_numeric_br", convert)
    text = HEADER + (
        "2019;MT;SOJA;A;SORRISO;999;SECA;999\n"
        "2023;PR;SOJA;B;SORRISO;999;SECA;999\n"
        "2023;MT;MILHO;C;SORRISO;999;SECA;999\n"
        "2023;MT;SOJA;D;OUTRO;999;SECA;999\n"
        "2023.0; mt ;SOJA;E;SORRISO (MT);1.234,56;SECA;100,25\n"
    )
    actual = pd.concat(
        parser.iter_apolices(
            io.BytesIO(text.encode()),
            cultura="soja",
            uf="mt",
            municipio={"codigo_ibge": 5107925, "nome": "Sorriso (MT)", "uf": "MT"},
            ano_inicio=2020,
            ano_fim=2023,
            chunk_size=2,
        )
    )
    assert actual["nr_apolice"].tolist() == ["E"]
    assert sorted(converted) == ["1.234,56", "100,25"]


def test_missing_indemnity_stays_nullable_and_claims_still_require_column():
    content = b"ANO_APOLICE;SG_UF_PROPRIEDADE;NM_CULTURA_GLOBAL\n2023;MT;SOJA\n"
    with sem_excecao():
        actual = parser.parse_apolices(content)
    assert str(actual["valor_indenizacao"].dtype) == "float64"
    assert actual["valor_indenizacao"].isna().all()
    with levanta_exatamente(ParseError, match="valor_indenizacao"):
        parser.parse_sinistros(content, cultura="absent")


@pytest.mark.asyncio
async def test_periods_are_closed_sequentially_and_range_filters_are_inclusive(monkeypatch):
    events = []

    @asynccontextmanager
    async def period(periodo):
        events.append(("open", periodo))
        text = HEADER + "".join(
            f"{year};MT;SOJA;A{year};SORRISO;10;SECA;20\n" for year in (2014, 2015, 2016, 2017)
        )
        years = (2014, 2015) if periodo == "2006-2015" else (2016, 2017)
        text = HEADER + "".join(
            line + "\n" for line in text.splitlines()[1:] if int(line[:4]) in years
        )
        async with binary_stream(text.encode()) as stream:
            yield stream
        events.append(("close", periodo))

    monkeypatch.setattr(client, "open_periodo", period)
    frame, meta = await api.apolices(ano_inicio=2015, ano_fim=2016, return_meta=True)
    assert frame["ano_apolice"].tolist() == [2015, 2016]
    assert frame["valor_indenizacao"].tolist() == [10.0, 10.0]
    assert events == [
        ("open", "2006-2015"),
        ("close", "2006-2015"),
        ("open", "2016-2024"),
        ("close", "2016-2024"),
    ]
    assert meta.records_count == 2
    assert meta.selected_source == "mapa_psr"
    assert meta.source_url == models.get_csv_url("2006-2015")


def test_linha_em_branco_nao_conta_como_registro():
    linha_b = b"2023;MT;SOJA;B;SORRISO;0;;5\n"
    content = CSV + b"\n" + linha_b + b"\n"
    with sem_excecao():
        actual = parser.parse_apolices(content)
    assert actual["nr_apolice"].tolist() == ["A", "B"]


def test_cabecalho_em_outra_caixa_mapeia_a_coluna():
    content = (
        b"ANO_APOLICE;SG_UF_PROPRIEDADE;NM_CULTURA_GLOBAL;NIVELDECOBERTURA;pe_taxa\n"
        b"2023;MT;SOJA;0,65;0,1369\n"
    )
    with sem_excecao():
        actual = parser.parse_apolices(content)
    assert {"nivel_cobertura", "taxa"} <= set(actual.columns)
    assert actual[["nivel_cobertura", "taxa"]].values.tolist() == [[0.65, 0.1369]]


def test_sinistros_sem_coluna_de_evento_seguem_pela_indenizacao():
    content = (
        b"ANO_APOLICE;SG_UF_PROPRIEDADE;NM_CULTURA_GLOBAL;NR_APOLICE;VALOR_INDENIZACAO\n"
        b"2023;MT;SOJA;A;10\n2023;MT;SOJA;B;0\n"
    )
    with sem_excecao():
        actual = parser.parse_sinistros(content)
    assert actual["nr_apolice"].tolist() == ["A"]
    assert "evento" not in actual.columns


def test_indenizacao_positiva_sem_evento_fica_fora_dos_sinistros():
    content = CSV + b"2023;MT;SOJA;B;SORRISO;500;;10\n"
    with sem_excecao():
        sinistros = parser.parse_sinistros(content)
        apolices = parser.parse_apolices(content)
    assert sinistros["nr_apolice"].tolist() == ["A"]
    assert apolices["nr_apolice"].tolist() == ["A", "B"]


def _sem_rede(_periodo):
    raise AssertionError("não deveria acessar a rede")


async def test_ano_sem_publicacao_devolve_vazio_com_o_esquema(monkeypatch):
    monkeypatch.setattr(client, "open_periodo", _sem_rede)
    with sem_excecao():
        apolices, meta = await api.apolices(ano=2026, return_meta=True)
        sinistros = await api.sinistros(ano=2026)
    assert apolices.empty and sinistros.empty
    assert list(apolices.columns) == models.COLUNAS_APOLICES
    assert list(sinistros.columns) == models.COLUNAS_SINISTROS
    assert meta.source_url == models.CATALOGO_URL
    assert meta.validation_warnings == ["O catálogo do PSR não tem arquivo para 2026"]


async def test_download_segue_redirecionamento(transport, temporary_files):
    destino = "https://dados.agricultura.gov.br/arquivos/psr_2025.csv"

    def responder(request):
        if str(request.url) == destino:
            return httpx.Response(200, stream=ScriptedStream([CSV]))
        return httpx.Response(302, headers={"location": destino})

    transport(responder)
    with sem_excecao():
        async with client.open_periodo("2025") as stream:
            recebido = stream.read()
    assert recebido == CSV
    assert temporary_files[0].closed


def test_numero_de_apolice_publicado_com_nbsp_sai_sem_o_espaco():
    content = (HEADER + "2008;SP;SOJA;\xa016223;ITAPEVA;0;;10\n").encode("windows-1252")
    with sem_excecao():
        actual = parser.parse_apolices(content)
    assert actual["nr_apolice"].tolist() == ["16223"]


def test_windows_1252_vem_antes_de_iso_8859_1():
    content = (HEADER + "2023;MT;SOJA;A;SÃO JOSÉ – DISTRITO;0;;1\n").encode("windows-1252")
    with sem_excecao():
        actual = parser.parse_apolices(content)
    assert actual["municipio"].tolist() == ["SÃO JOSÉ – DISTRITO"]


async def test_sem_filtro_de_ano_baixa_os_tres_periodos(monkeypatch):
    pedidos = []

    def periodo(nome):
        pedidos.append(nome)
        return binary_stream(CSV.replace(b";A;", f";{nome};".encode()))

    monkeypatch.setattr(client, "open_periodo", periodo)
    with sem_excecao():
        frame = await api.apolices()
    assert pedidos == ["2006-2015", "2016-2024", "2025"]
    assert sorted(frame["nr_apolice"]) == pedidos
