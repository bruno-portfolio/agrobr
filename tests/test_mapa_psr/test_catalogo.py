from __future__ import annotations

import hashlib
import json
import warnings
from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock

import httpx
import pytest

from agrobr import datasets
from agrobr.alt.mapa_psr import api, client, models, parser
from agrobr.exceptions import ParseError, SourceUnavailableError
from agrobr.utils import time as time_utils
from tests.helpers import RETRY_SLEEP, binary_stream, levanta_exatamente, sem_excecao

from .conftest import CATALOGO_OFICIAL

HEADER = (
    "ANO_APOLICE;SG_UF_PROPRIEDADE;NM_CULTURA_GLOBAL;NR_APOLICE;"
    "NM_MUNICIPIO_PROPRIEDADE;VALOR_INDENIZAÇÃO;EVENTO_PREPONDERANTE;NR_AREA_TOTAL\n"
)
URL_2026 = (
    f"https://dados.agricultura.gov.br/dataset/{models.DATASET_ID}"
    "/resource/00000000-0000-4000-8000-000000002026/download/dados_abertos_psr_2026csv.csv"
)
SEM_2026 = "O catálogo do PSR não tem arquivo para 2026"
FETCH_CATALOGO_REAL = client.fetch_catalogo


def _csv(*anos: int) -> bytes:
    return (HEADER + "".join(f"{ano};MT;SOJA;A{ano};SORRISO;10;SECA;20\n" for ano in anos)).encode()


def _catalogo(*extras: dict[str, str]) -> bytes:
    pacote = json.loads(CATALOGO_OFICIAL.read_bytes())
    pacote["result"]["resources"].extend(extras)
    return json.dumps(pacote).encode()


def _com_2026() -> bytes:
    return _catalogo({"id": "x", "name": "PSR - 2026", "format": "CSV", "url": URL_2026})


def _em(ano: int, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(time_utils, "utcnow_aware", lambda: datetime(ano, 3, 1, 15, tzinfo=UTC))


@pytest.fixture
def servido(monkeypatch):
    """Serve o catálogo pedido e os CSV por período fixo ou por URL, anotando cada pedido."""
    pedidos: list[str] = []

    def servir_catalogo(corpo: bytes) -> None:
        async def catalogo() -> bytes:
            pedidos.append("catalogo")
            return corpo

        monkeypatch.setattr(client, "fetch_catalogo", catalogo)

    def periodo(nome: str):
        pedidos.append(nome)
        anos = {"2006-2015": (2015,), "2016-2024": (2024,), "2025": (2025,)}[nome]
        return binary_stream(_csv(*anos))

    def url(endereco: str):
        pedidos.append(endereco)
        return binary_stream(_csv(2026))

    monkeypatch.setattr(client, "open_periodo", periodo)
    monkeypatch.setattr(client, "open_url", url)
    servir_catalogo(CATALOGO_OFICIAL.read_bytes())
    return pedidos, servir_catalogo


def test_catalogo_oficial_tem_os_3_csv_do_dicionario_fixo():
    manifesto = json.loads((CATALOGO_OFICIAL.parent / "manifest.json").read_bytes())
    corpo = CATALOGO_OFICIAL.read_bytes().replace(b"\r\n", b"\n")
    assert hashlib.sha256(corpo).hexdigest() == manifesto["arquivo_sha256"]
    catalogo = parser.parse_catalogo(CATALOGO_OFICIAL.read_bytes())
    assert catalogo == {periodo: models.get_csv_url(periodo) for periodo in models.CSV_RESOURCES}
    assert models.ULTIMO_ANO_FIXO == 2025


def test_parse_catalogo_so_aceita_csv_do_portal():
    corpo = _catalogo(
        {"url": URL_2026.replace("https://", "http://").replace("2026csv", "2027csv")},
        {"url": URL_2026.replace("dados.agricultura.gov.br", "exemplo.com")},
        {"url": URL_2026.replace(".gov.br/", ".gov.br:8443/").replace("2026csv", "2028csv")},
        {"url": URL_2026.replace("https://", "https://u:p@").replace("2026csv", "2031csv")},
        {"url": URL_2026.replace("2026csv.csv", "2029.xlsx")},
        {"url": URL_2026.replace("2026csv", "2026a2030csv")},
        {"nome": "sem url"},
    )
    with sem_excecao():
        catalogo = parser.parse_catalogo(corpo)
    assert sorted(catalogo) == ["2006-2015", "2016-2024", "2025", "2026-2030"]
    for invalido in (b"<html>", b'{"success": false}', b'{"success": true, "result": {}}'):
        with levanta_exatamente(ParseError, match="lista de recursos"):
            parser.parse_catalogo(invalido)


async def _chamar(chamada: Any) -> tuple[Any, list[str]]:
    with warnings.catch_warnings(record=True) as avisos, sem_excecao():
        warnings.simplefilter("always")
        resultado = await chamada
    return resultado, [
        str(aviso.message) for aviso in avisos if "atálogo do PSR" in str(aviso.message)
    ]


async def test_ano_publicado_depois_da_versao_sai_do_catalogo(servido):
    pedidos, servir_catalogo = servido
    servir_catalogo(_com_2026())
    (frame, meta), avisos = await _chamar(api.apolices(ano=2026, return_meta=True))
    assert pedidos == ["catalogo", URL_2026]
    assert frame["ano_apolice"].tolist() == [2026]
    assert meta.source_url == URL_2026
    assert meta.validation_warnings == avisos == []


async def test_ano_sem_arquivo_no_catalogo_avisa_e_aponta_o_catalogo(servido):
    pedidos, _ = servido
    (frame, meta), avisos = await _chamar(api.apolices(ano=2026, return_meta=True))
    assert pedidos == ["catalogo"]
    assert frame.empty
    assert meta.source_url == models.CATALOGO_URL
    assert meta.validation_warnings == avisos == [SEM_2026]
    (_, meta_dataset), avisos = await _chamar(datasets.seguro_rural(ano=2026, return_meta=True))
    assert avisos == [SEM_2026]
    assert SEM_2026 in meta_dataset.validation_warnings


async def test_periodo_aberto_vai_ate_o_ano_corrente(servido, monkeypatch):
    pedidos, servir_catalogo = servido
    servir_catalogo(_com_2026())
    _em(2027, monkeypatch)
    (frame, meta), avisos = await _chamar(api.apolices(ano_inicio=2025, return_meta=True))
    assert pedidos == ["catalogo", "2025", URL_2026]
    assert frame["ano_apolice"].tolist() == [2025, 2026]
    assert meta.validation_warnings == avisos == ["O catálogo do PSR não tem arquivo para 2027"]


async def test_ano_do_dicionario_fixo_nao_consulta_o_catalogo(servido):
    pedidos, _ = servido
    with sem_excecao():
        frame = await api.apolices(ano=2024)
    assert pedidos == ["2016-2024"]
    assert frame["ano_apolice"].tolist() == [2024]


async def test_catalogo_fora_do_ar(servido, monkeypatch):
    pedidos, _ = servido

    async def fora() -> bytes:
        raise SourceUnavailableError(source="mapa_psr", last_error="HTTP 503")

    monkeypatch.setattr(client, "fetch_catalogo", fora)
    with levanta_exatamente(SourceUnavailableError, match="HTTP 503"):
        await api.apolices(ano=2026)
    _em(2026, monkeypatch)
    (frame, meta), avisos = await _chamar(api.apolices(ano_inicio=2025, return_meta=True))
    assert pedidos == ["2025"]
    assert frame["ano_apolice"].tolist() == [2025]
    assert meta.source_url == models.get_csv_url("2025")
    assert meta.validation_warnings == avisos
    assert len(avisos) == 1
    assert "HTTP 503" in avisos[0]
    assert avisos[0].endswith("os anos depois de 2025 ficam de fora")


async def test_fetch_catalogo_pede_o_pacote_e_devolve_o_corpo(monkeypatch):
    original = httpx.AsyncClient
    monkeypatch.setattr(RETRY_SLEEP, AsyncMock())
    corpo = CATALOGO_OFICIAL.read_bytes()
    pedidos: list[str] = []

    def responder(request: httpx.Request) -> httpx.Response:
        pedidos.append(str(request.url))
        return httpx.Response(200 if len(pedidos) == 1 else 404, content=corpo)

    monkeypatch.setattr(
        client.httpx,
        "AsyncClient",
        lambda **kwargs: original(transport=httpx.MockTransport(responder), **kwargs),
    )
    with sem_excecao():
        recebido = await FETCH_CATALOGO_REAL()
    assert recebido == corpo
    assert pedidos == [models.CATALOGO_URL]
    with levanta_exatamente(SourceUnavailableError, match="404"):
        await FETCH_CATALOGO_REAL()
