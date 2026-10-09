from __future__ import annotations

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from agrobr.b3 import client
from agrobr.exceptions import ResourceLimitError, SourceUnavailableError
from tests.helpers import levanta_exatamente


async def test_vez_presa_sem_soltar_tem_teto(monkeypatch):
    monkeypatch.setenv("AGROBR_HTTP_TIMEOUT_READ", "0.2")
    pedido = AsyncMock(return_value=(b"", "url"))
    monkeypatch.setattr(client, "_fetch_posicoes_abertas", pedido)
    assert client._OI_LOCK.acquire(blocking=False)
    try:
        inicio = time.monotonic()
        with levanta_exatamente(
            ResourceLimitError, match="outra consulta das posições em aberto segura a vez"
        ):
            await asyncio.wait_for(client.fetch_posicoes_abertas("2026-09-21"), timeout=5)
        espera = time.monotonic() - inicio
    finally:
        client._OI_LOCK.release()
    assert 0.15 <= espera < 2
    assert pedido.await_count == 0


async def test_fila_que_anda_nao_estoura_o_teto(monkeypatch):
    monkeypatch.setenv("AGROBR_HTTP_TIMEOUT_READ", "0.5")
    ativos = pico = 0

    async def pedido(data):
        nonlocal ativos, pico
        ativos += 1
        pico = max(pico, ativos)
        await asyncio.sleep(0.1)
        ativos -= 1
        return b"", data

    monkeypatch.setattr(client, "_fetch_posicoes_abertas", pedido)
    datas = [f"2026-09-{dia:02d}" for dia in range(14, 26)]
    inicio = time.monotonic()
    respostas = await asyncio.gather(*(client.fetch_posicoes_abertas(data) for data in datas))
    assert time.monotonic() - inicio > 1
    assert [url for _, url in respostas] == datas
    assert pico == 1


class TestFetchPosicoesAbertas:
    @pytest.mark.asyncio
    async def test_empty_token_raises(self):
        token_response = MagicMock()
        token_response.status_code = 200
        token_response.json.return_value = {"token": ""}
        token_response.raise_for_status = MagicMock()

        with (
            patch(
                "agrobr.b3.client.retry_on_status",
                new_callable=AsyncMock,
                return_value=token_response,
            ),
            pytest.raises(SourceUnavailableError, match="Token vazio"),
        ):
            await client.fetch_posicoes_abertas("2025-12-19")


def _resposta(conteudo: bytes) -> MagicMock:
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.content = conteudo
    mock_response.raise_for_status = MagicMock()
    return mock_response


class TestFetchAjustesZip:
    @pytest.mark.asyncio
    async def test_zip_vazio_indica_pregao_nao_publicado(self):
        zip_vazio = b"PK\x05\x06" + bytes(18)
        with (
            patch(
                "agrobr.b3.client.retry_on_status",
                new_callable=AsyncMock,
                return_value=_resposta(zip_vazio),
            ),
            levanta_exatamente(client.PregaoNaoPublicadoError, "ainda não publicado") as erro,
        ):
            await client.fetch_ajustes_zip("12/06/2026")
        assert erro.value.conteudo == zip_vazio

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "conteudo", [b'{"message":"Temporarily unavailable"}', b"\x00" * 22, b""]
    )
    async def test_corpo_curto_que_nao_e_zip_vazio_e_falha(self, conteudo):
        with (
            patch(
                "agrobr.b3.client.retry_on_status",
                new_callable=AsyncMock,
                return_value=_resposta(conteudo),
            ),
            levanta_exatamente(SourceUnavailableError, "muito pequeno"),
        ):
            await client.fetch_ajustes_zip("12/06/2026")
