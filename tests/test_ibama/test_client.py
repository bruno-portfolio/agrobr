from __future__ import annotations

import io
import zipfile
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from agrobr.exceptions import SourceUnavailableError
from agrobr.ibama import client
from agrobr.ibama.models import MIN_CSV_BYTES


def _make_zip(files: dict[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_STORED) as zf:
        for name, data in files.items():
            zf.writestr(name, data)
    return buf.getvalue()


def _mock_response(content: bytes) -> MagicMock:
    resp = MagicMock()
    resp.status_code = 200
    resp.content = content
    resp.raise_for_status = MagicMock()
    return resp


class TestFetchEmbargosZip:
    @pytest.mark.asyncio
    async def test_extrai_csv_do_zip(self):
        csv_data = b"x" * (MIN_CSV_BYTES + 10)
        zip_bytes = _make_zip({"termo_embargo.csv": csv_data})

        with patch(
            "agrobr.ibama.client.retry_on_status",
            new_callable=AsyncMock,
            return_value=_mock_response(zip_bytes),
        ):
            content, url = await client.fetch_embargos_zip()

        assert content == csv_data
        assert "dadosabertos.ibama.gov.br" in url

    @pytest.mark.asyncio
    async def test_zip_pequeno_raises(self):
        with (
            patch(
                "agrobr.ibama.client.retry_on_status",
                new_callable=AsyncMock,
                return_value=_mock_response(b"x" * 10),
            ),
            pytest.raises(SourceUnavailableError, match="muito pequeno"),
        ):
            await client.fetch_embargos_zip()

    @pytest.mark.asyncio
    async def test_resposta_nao_zip_raises(self):
        with (
            patch(
                "agrobr.ibama.client.retry_on_status",
                new_callable=AsyncMock,
                return_value=_mock_response(b"<html>erro</html>" * 100),
            ),
            pytest.raises(SourceUnavailableError, match="Assinatura inválida para zip"),
        ):
            await client.fetch_embargos_zip()

    @pytest.mark.asyncio
    async def test_zip_sem_csv_raises(self):
        zip_bytes = _make_zip({"leiame.txt": b"sem csv aqui" * 100})

        with (
            patch(
                "agrobr.ibama.client.retry_on_status",
                new_callable=AsyncMock,
                return_value=_mock_response(zip_bytes),
            ),
            pytest.raises(SourceUnavailableError, match="não contém arquivo CSV"),
        ):
            await client.fetch_embargos_zip()

    @pytest.mark.asyncio
    async def test_csv_truncado_raises(self):
        zip_bytes = _make_zip({"termo_embargo.csv": b"SEQ_TAD;UF\n" + b"1;PA\n" * 200})

        with (
            patch(
                "agrobr.ibama.client.retry_on_status",
                new_callable=AsyncMock,
                return_value=_mock_response(zip_bytes),
            ),
            pytest.raises(SourceUnavailableError, match="truncamento"),
        ):
            await client.fetch_embargos_zip()
