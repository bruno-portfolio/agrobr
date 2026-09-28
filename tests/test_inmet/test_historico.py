from __future__ import annotations

import io
import zipfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

from agrobr.exceptions import ParseError
from agrobr.inmet import api, client, parser
from tests.helpers import levanta_exatamente

GOLDEN = Path(__file__).parent.parent / "golden_data" / "inmet" / "historico_a701_sample.csv"
ZIP_URL = "https://portal.inmet.gov.br/uploads/dadoshistoricos/2025.zip"


def _golden_bytes() -> bytes:
    return GOLDEN.read_bytes()


def _make_zip(members: dict[str, bytes], pad_to_min: bool = True) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_STORED) as zf:
        for name, data in members.items():
            zf.writestr(name, data)
        if pad_to_min:
            zf.writestr("2025/_pad.bin", b"\x00" * client.MIN_HISTORICO_ZIP)
    return buf.getvalue()


def _mock_response(content: bytes, status_code: int = 200) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.content = content
    resp.raise_for_status = MagicMock()
    return resp


@pytest.fixture(autouse=True)
def _reset_historico_cache():
    client._historico_zip_cache = None
    yield
    client._historico_zip_cache = None


class TestParseHistoricoCsv:
    def test_golden_valido(self):
        df = parser.parse_historico_csv(_golden_bytes(), "A701")

        assert len(df) == 48
        assert df["estacao"].eq("A701").all()
        assert df["uf"].eq("SP").all()
        assert pd.api.types.is_datetime64_any_dtype(df["data"])
        assert pd.api.types.is_float_dtype(df["temperatura"])

    def test_truncado_raises(self):
        with pytest.raises(ParseError, match="truncado"):
            parser.parse_historico_csv(b"REGIAO:;SE\nUF:;SP", "A701")

    def test_header_irreconhecivel_raises(self):
        csv_quebrado = b"\n".join([b"META:;x"] * 8 + [b"COL_A;COL_B", b"1;2", b"3;4"])

        with levanta_exatamente(ParseError, "Header"):
            parser.parse_historico_csv(csv_quebrado, "A701")


class TestHistoricoApi:
    @pytest.mark.asyncio
    async def test_return_meta(self):
        with patch(
            "agrobr.inmet.client.retry_on_status",
            new_callable=AsyncMock,
            return_value=_mock_response(_make_zip({"INMET_SE_SP_A701_X.CSV": _golden_bytes()})),
        ):
            df, meta = await api.historico("A701", 2025, return_meta=True)

        assert meta.source == "inmet"
        assert meta.source_method == "httpx+zip+csv"
        assert meta.source_url == ZIP_URL
        assert meta.records_count == len(df)
