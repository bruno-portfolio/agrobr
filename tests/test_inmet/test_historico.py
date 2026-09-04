from __future__ import annotations

import io
import zipfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

from agrobr.exceptions import InvalidParameterError, ParseError, SourceUnavailableError
from agrobr.inmet import api, client, parser

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

    def test_schema_igual_ao_da_api(self):
        df = parser.parse_historico_csv(_golden_bytes(), "A701")

        assert set(df.columns).issubset(set(parser.COLUNAS_HORARIAS.values()))
        for col in ("data", "hora_utc", "estacao", "uf", "precipitacao_mm", "temperatura"):
            assert col in df.columns

    def test_hora_normalizada_sem_sufixo_utc(self):
        df = parser.parse_historico_csv(_golden_bytes(), "A701")

        assert df["hora_utc"].str.fullmatch(r"\d{4}").all()

    def test_decimal_brasileiro_convertido(self):
        df = parser.parse_historico_csv(_golden_bytes(), "A701")

        temps = df["temperatura"].dropna()
        assert len(temps) > 0
        assert temps.between(-10, 50).all()

    def test_celulas_vazias_viram_na(self):
        df = parser.parse_historico_csv(_golden_bytes(), "A701")

        assert df["radiacao_kj_m2"].isna().any()

    def test_agregar_diario_compatible(self):
        df = parser.parse_historico_csv(_golden_bytes(), "A701")
        diario = parser.agregar_diario(df)

        assert len(diario) == 2
        assert "temp_media" in diario.columns
        assert "precipitacao_mm" in diario.columns

    def test_header_irreconhecivel_raises(self):
        csv_quebrado = b"\n".join([b"META:;x"] * 8 + [b"COL_A;COL_B", b"1;2", b"3;4"])

        with pytest.raises(ParseError, match="Header"):
            parser.parse_historico_csv(csv_quebrado, "A701")

    def test_header_sem_hora_utc_raises(self):
        raw = _golden_bytes().decode("latin-1").splitlines()
        raw[8] = raw[8].replace("Hora UTC", "MOMENTO")
        csv_sem_hora = "\n".join(raw).encode("latin-1")

        with pytest.raises(ParseError, match="Header"):
            parser.parse_historico_csv(csv_sem_hora, "A701")

    def test_truncado_raises(self):
        with pytest.raises(ParseError, match="truncado"):
            parser.parse_historico_csv(b"REGIAO:;SE\nUF:;SP", "A701")


class TestFetchHistoricoEstacao:
    @pytest.mark.asyncio
    async def test_extrai_csv_da_estacao(self):
        zip_bytes = _make_zip(
            {"2025/INMET_SE_SP_A701_SAO PAULO - MIRANTE_01-01-2025_A_31-12-2025.CSV": b"conteudo"}
        )

        with patch(
            "agrobr.inmet.client.retry_on_status",
            new_callable=AsyncMock,
            return_value=_mock_response(zip_bytes),
        ):
            raw, url = await client.fetch_historico_estacao("a701", 2025)

        assert raw == b"conteudo"
        assert url.endswith("/2025.zip")

    @pytest.mark.asyncio
    async def test_estacao_inexistente_raises(self):
        zip_bytes = _make_zip({"2025/INMET_SE_SP_A701_X_01-01-2025_A_31-12-2025.CSV": b"x"})

        with (
            patch(
                "agrobr.inmet.client.retry_on_status",
                new_callable=AsyncMock,
                return_value=_mock_response(zip_bytes),
            ),
            pytest.raises(SourceUnavailableError, match="Z999 sem dados no ano 2025"),
        ):
            await client.fetch_historico_estacao("Z999", 2025)

    @pytest.mark.asyncio
    async def test_ano_404_raises_amigavel(self):
        with (
            patch(
                "agrobr.inmet.client.retry_on_status",
                new_callable=AsyncMock,
                return_value=_mock_response(b"", status_code=404),
            ),
            pytest.raises(SourceUnavailableError, match="indisponível no dadoshistoricos"),
        ):
            await client.fetch_historico_estacao("A701", 1999 + 1000)

    @pytest.mark.asyncio
    async def test_ano_antigo_demais_raises(self):
        with pytest.raises(ValueError, match="fora do dadoshistoricos"):
            await client.fetch_historico_estacao("A701", 1995)

    @pytest.mark.asyncio
    async def test_cache_evita_segundo_download_do_mesmo_ano(self):
        zip_bytes = _make_zip(
            {
                "2025/INMET_SE_SP_A701_X_01-01-2025_A_31-12-2025.CSV": b"a701",
                "2025/INMET_S_RS_A801_Y_01-01-2025_A_31-12-2025.CSV": b"a801",
            }
        )

        with patch(
            "agrobr.inmet.client.retry_on_status",
            new_callable=AsyncMock,
            return_value=_mock_response(zip_bytes),
        ) as mock_retry:
            raw1, _ = await client.fetch_historico_estacao("A701", 2025)
            raw2, _ = await client.fetch_historico_estacao("A801", 2025)

        assert raw1 == b"a701"
        assert raw2 == b"a801"
        assert mock_retry.call_count == 1


class TestHistoricoApi:
    @pytest.mark.asyncio
    async def test_year_before_2000_raises_before_download(self):
        with (
            patch.object(api.client, "fetch_historico_estacao", new_callable=AsyncMock) as fetch,
            pytest.raises(InvalidParameterError, match="2000"),
        ):
            await api.historico("A701", 1999)

        fetch.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_retorna_dataframe_horario(self):
        with patch(
            "agrobr.inmet.api.client.fetch_historico_estacao",
            new_callable=AsyncMock,
            return_value=(_golden_bytes(), ZIP_URL),
        ):
            df = await api.historico("A701", 2025)

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 48

    @pytest.mark.asyncio
    async def test_agregacao_diaria(self):
        with patch(
            "agrobr.inmet.api.client.fetch_historico_estacao",
            new_callable=AsyncMock,
            return_value=(_golden_bytes(), ZIP_URL),
        ):
            df = await api.historico("A701", 2025, agregacao="diario")

        assert len(df) == 2
        assert "temp_media" in df.columns

    @pytest.mark.asyncio
    async def test_return_meta(self):
        with patch(
            "agrobr.inmet.api.client.fetch_historico_estacao",
            new_callable=AsyncMock,
            return_value=(_golden_bytes(), ZIP_URL),
        ):
            df, meta = await api.historico("A701", 2025, return_meta=True)

        assert meta.source == "inmet"
        assert meta.source_method == "httpx+zip+csv"
        assert meta.source_url == ZIP_URL
        assert meta.records_count == len(df)
