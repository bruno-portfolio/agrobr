from __future__ import annotations

import io
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
import requests

from agrobr import datasets
from agrobr.antaq import api, client
from agrobr.exceptions import InvalidParameterError, SourceUnavailableError
from agrobr.utils import time as time_utils
from tests.helpers import levanta_exatamente, sem_excecao

AMOSTRA = Path(__file__).resolve().parents[1] / "golden_data/antaq/movimentacao_sample"
BASE = "https://estatistica.antaq.gov.br/ea/txt"


def _zip(membros: dict[str, str]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as arquivo:
        for nome, amostra in membros.items():
            arquivo.writestr(nome, (AMOSTRA / amostra).read_bytes())
    return buffer.getvalue()


@pytest.fixture(autouse=True)
def em_2027(monkeypatch):
    monkeypatch.setattr(time_utils, "utcnow_aware", lambda: datetime(2027, 3, 1, 15, 0, tzinfo=UTC))


@pytest.fixture
def pedidos(monkeypatch) -> list[str]:
    publicados = {
        f"{BASE}/2026.zip": _zip(
            {"2026Atracacao.txt": "atracacao.txt", "2026Carga.txt": "carga.txt"}
        ),
        f"{BASE}/Mercadoria.zip": _zip({"Mercadoria.txt": "mercadoria.txt"}),
    }
    vistos: list[str] = []

    def get(url: str, **_kwargs: Any) -> requests.Response:
        vistos.append(url)
        resposta = requests.Response()
        resposta._content = publicados.get(url, b"")
        resposta.status_code = 200 if url in publicados else 404
        resposta.url = url
        resposta.headers["Content-Type"] = "application/zip"
        return resposta

    monkeypatch.setattr(client.requests, "get", get)
    return vistos


async def test_ano_depois_do_ultimo_publicado_na_versao_vai_a_fonte(pedidos):
    with sem_excecao():
        frame = await api.movimentacao(2026)
        agregado = await datasets.movimentacao_portuaria(ano=2026)
    assert pedidos == [f"{BASE}/2026.zip", f"{BASE}/Mercadoria.zip"] * 2
    assert len(frame) > 0
    assert len(agregado) > 0


async def test_ano_corrente_ainda_sem_arquivo_e_erro_da_fonte(pedidos):
    with levanta_exatamente(SourceUnavailableError, match="404"):
        await api.movimentacao(2027)
    assert pedidos == [f"{BASE}/2027.zip"]


async def test_ano_depois_do_corrente_recusado_antes_da_rede(pedidos):
    with levanta_exatamente(InvalidParameterError, match="entre 2010 e 2027, recebido: 2028"):
        await api.movimentacao(2028)
    assert pedidos == []
