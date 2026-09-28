from __future__ import annotations

import csv
import io
import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock

import certifi
import httpx
import pandas as pd
import pytest

from agrobr import comexstat, constants
from agrobr.comexstat import _numbers, _tls, client, transport_models
from agrobr.comexstat import query as comex_query
from agrobr.datasets import _comercio_exterior
from agrobr.exceptions import (
    ContractViolationError,
    InvalidParameterError,
    SourceUnavailableError,
)
from agrobr.http import rate_limiter
from agrobr.models import MetaInfo
from tests.helpers import comexstat_csv, install_comexstat_http, levanta_exatamente, sem_excecao

URL = f"{constants.URLS[constants.Fonte.COMEXSTAT]['bulk_csv']}/EXP_2024.csv"
INTEGRIDADE = Path(__file__).parents[1] / "golden_data" / "comexstat" / "integridade20260908"


@pytest.fixture
def servidor(monkeypatch):
    constructor = httpx.AsyncClient
    monkeypatch.delenv("SSL_CERT_FILE", raising=False)
    monkeypatch.delenv("SSL_CERT_DIR", raising=False)
    monkeypatch.setattr(client.retry.asyncio, "sleep", AsyncMock())
    monkeypatch.setattr(rate_limiter, "_async_sleep", AsyncMock())

    def install(handler):
        def factory(**kwargs):
            return constructor(transport=httpx.MockTransport(handler), **kwargs)

        monkeypatch.setattr(client.httpx, "AsyncClient", factory)

    return install


@pytest.mark.parametrize(
    "funcao,valor,motivo",
    [
        (_numbers.integer_token, "0" * 19 + "1", "capacidade Int64"),
        (_numbers.money_token, "1" * (constants.COMEXSTAT_MAX_NUMBER_CHARS + 1), "limite lexical"),
        (_numbers.money_token, 12, "não é texto"),
        (_numbers.money_token, f"1.5e-{constants.COMEXSTAT_MAX_NUMBER_EXPONENT}", "expoente"),
    ],
)
def test_token_numerico_fora_do_limite_e_recusado(funcao, valor, motivo):
    with levanta_exatamente(ValueError, match=motivo):
        funcao(valor)


@pytest.mark.parametrize("variavel", ["SSL_CERT_FILE", "SSL_CERT_DIR"])
async def test_origem_da_ac_declarada_no_metainfo(variavel, monkeypatch, tmp_path):
    monkeypatch.delenv("SSL_CERT_FILE", raising=False)
    monkeypatch.delenv("SSL_CERT_DIR", raising=False)
    monkeypatch.setenv(variavel, certifi.where() if variavel == "SSL_CERT_FILE" else str(tmp_path))
    install_comexstat_http(monkeypatch, comexstat_csv())

    _, meta = await comexstat.exportacao("soja", ano=2024, return_meta=True)

    assert meta.source_details["acquisition"]["tls"]["ca_source"] == variavel
    assert _tls.ca_source() == variavel


@pytest.mark.parametrize(
    "resposta,motivo",
    [
        (lambda _: httpx.Response(203, stream=httpx.ByteStream(b"CO_ANO;X\n")), "necessário 200"),
        (lambda _: httpx.Response(302, stream=httpx.ByteStream(b"")), "único Location"),
        (
            lambda request: httpx.Response(
                302, headers={"location": str(request.url)}, stream=httpx.ByteStream(b"")
            ),
            "Loop",
        ),
    ],
)
async def test_resposta_http_fora_do_protocolo_e_recusada(resposta, motivo, servidor):
    servidor(resposta)
    with levanta_exatamente(SourceUnavailableError, match=motivo):
        async with client.open_csv(fluxo="exportacao", ano=2024):
            pass


async def test_erro_ao_fechar_sem_erro_anterior_e_recusado(servidor, monkeypatch):
    servidor(lambda _: httpx.Response(200, stream=httpx.ByteStream(comexstat_csv())))
    monkeypatch.setattr(client, "_close", AsyncMock(return_value=OSError("disco cheio")))
    with levanta_exatamente(SourceUnavailableError, match="Erro de fechamento: OSError"):
        async with client.open_csv(fluxo="exportacao", ano=2024):
            pass


async def test_dicionario_desconhecido_recusado():
    with levanta_exatamente(ValueError, match="unidades, paises, vias ou urfs"):
        async with client.open_dictionary("municipios"):
            pass


def test_recurso_sem_arquivo_aberto_recusa_leitura():
    recurso = transport_models.DownloadedResource(
        resource=transport_models.ResourceSpec(kind="annual", url=URL, fluxo="exportacao", ano=2024)
    )
    with levanta_exatamente(RuntimeError, match="ainda não foi aberto"):
        _ = recurso.file


def test_consulta_de_fluxo_desconhecido_recusada():
    with levanta_exatamente(InvalidParameterError, match="exportacao ou importacao"):
        comex_query.build_query(fluxo="reexportacao", produto="soja", ano=2024)


async def test_sem_limite_de_linhas_quando_pedido(monkeypatch):
    install_comexstat_http(monkeypatch, comexstat_csv())

    with sem_excecao():
        frame, meta = await comexstat.exportacao(
            "soja", ano=2024, max_linhas=None, return_meta=True
        )

    assert len(frame) == 3
    assert meta.source_details["query"]["limites"]["max_linhas"] is None


def test_retorno_nao_booleano_recusado_no_dataset():
    with levanta_exatamente(InvalidParameterError, match="return_meta deve ser booleano"):
        _comercio_exterior.validate_options("sim")


@pytest.mark.parametrize("valores", [[1e308, 1e308], [float("inf"), 1.0]])
def test_soma_nao_finita_viola_o_contrato(valores):
    frame = pd.DataFrame(
        {
            "ano": [2024, 2024],
            "mes": [1, 1],
            "produto": ["soja", "soja"],
            "ncm": ["12019000", "12010090"],
            "kg_liquido": valores,
        }
    )
    with levanta_exatamente(ContractViolationError, match="Soma não finita"):
        _comercio_exterior.agregar_ncms(frame)


def test_agregacao_sem_medida_so_descarta_o_ncm():
    frame = pd.DataFrame({"ano": [2024, 2024], "ncm": ["12019000", "12010090"], "uf": ["MT", "MT"]})
    saida = _comercio_exterior.agregar_ncms(frame)
    assert saida.columns.tolist() == ["ano", "uf"]
    assert len(saida) == 2


def test_vazio_sem_tipos_registrados_fica_como_veio():
    vazio = pd.DataFrame()
    meta = MetaInfo(
        source="comexstat", source_url="u", source_method="httpx", fetched_at=datetime.now(UTC)
    )
    assert _comercio_exterior.restore_empty_comexstat(vazio, meta) is vazio


async def test_falha_de_conexao_vira_fonte_indisponivel(servidor):
    def recusar(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("conexão recusada", request=request)

    servidor(recusar)
    with levanta_exatamente(SourceUnavailableError, match="ConnectError"):
        async with client.open_csv(fluxo="exportacao", ano=2024):
            pass


async def test_falha_de_tls_preserva_o_erro_original(servidor, monkeypatch, tmp_path):
    servidor(lambda _: httpx.Response(200, stream=httpx.ByteStream(comexstat_csv())))
    monkeypatch.setenv("SSL_CERT_FILE", str(tmp_path / "ausente.pem"))
    with levanta_exatamente(SourceUnavailableError, match="FileNotFoundError"):
        async with client.open_csv(fluxo="exportacao", ano=2024):
            pass


async def test_zeros_e_ausentes_das_medidas_conferem_com_o_csv_oficial(monkeypatch):
    bruto = (INTEGRIDADE / "EXP_2025.csv").read_bytes()
    linhas = list(csv.DictReader(io.StringIO(bruto.decode("utf-8")), delimiter=";"))
    zerado = [linha for linha in linhas if linha["CO_NCM"] == "83100000"]
    assert zerado and all(Decimal(linha["VL_FOB"]) == 0 for linha in zerado)
    install_comexstat_http(monkeypatch, bruto)

    _, meta = await comexstat.exportacao("83100000", ano=2025, return_meta=True)

    estatisticas = meta.source_details["parsing"]["statistics"]
    for coluna, medida in (
        ("QT_ESTAT", "qtd_estatistica"),
        ("KG_LIQUIDO", "kg_liquido"),
        ("VL_FOB", "valor_fob_usd"),
    ):
        valores = [linha[coluna] for linha in linhas]
        fonte = estatisticas["source"][medida]
        assert fonte["count"] == len(valores)
        assert fonte["nulls"] == sum(valor == "" for valor in valores)
        assert fonte["zeros"] == sum(valor != "" and Decimal(valor) == 0 for valor in valores)
        assert 0 < fonte["zeros"] < fonte["count"]
    assert estatisticas["selected"]["valor_fob_usd"]["exact_sum"] == "0"


@pytest.mark.parametrize(
    "tabela,arquivo,codificacao",
    [("unidades", "NCM_UNIDADE.csv", "utf-8-sig"), ("paises", "PAIS.csv", "cp1252")],
)
async def test_dicionario_publico_confere_o_arquivo_oficial(
    tabela, arquivo, codificacao, monkeypatch
):
    bruto = (INTEGRIDADE / arquivo).read_bytes()
    linhas = list(csv.reader(io.StringIO(bruto.decode(codificacao), newline=""), delimiter=";"))
    manifesto = json.loads((INTEGRIDADE / "metadata.json").read_text(encoding="utf-8"))
    install_comexstat_http(monkeypatch, bruto)

    with sem_excecao():
        frame, meta = await comexstat.dicionario(tabela, return_meta=True)

    assert list(frame.itertuples(index=False, name=None)) == [tuple(linha) for linha in linhas[1:]]
    assert meta.source_url == constants.COMEXSTAT_DICTIONARY_URLS[tabela]
    assert meta.raw_content_hash == manifesto["artifacts"][arquivo]["sha256"]
