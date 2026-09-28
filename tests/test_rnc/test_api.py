from __future__ import annotations

import csv
import io
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from agrobr.rnc import api, snapshot
from tests.helpers import rnc_csv_acquisition

GOLDEN_DIR = Path(__file__).resolve().parent.parent / "golden_data" / "rnc"


@pytest.fixture(autouse=True)
def _isolated_cache(tmp_path, monkeypatch):
    monkeypatch.setattr(snapshot, "cache_dir", lambda: tmp_path)


def _registradas_bytes():
    return (GOLDEN_DIR / "registradas_sample.csv").read_bytes()


def _protegidas_bytes():
    return (GOLDEN_DIR / "protegidas_sample.csv").read_bytes()


@pytest.mark.asyncio
async def test_registradas_as_polars():
    polars = pytest.importorskip("polars")
    with patch("agrobr.rnc.client.fetch_registradas_bundle", new_callable=AsyncMock) as mock:
        mock.return_value = rnc_csv_acquisition(_registradas_bytes(), "registradas")

        from agrobr.rnc import registradas

        df = await registradas(as_polars=True)
        assert isinstance(df, polars.DataFrame)


@pytest.mark.asyncio
async def test_registradas_filter_grupo():
    with (
        patch("agrobr.rnc.client.fetch_registradas_bundle", new_callable=AsyncMock) as mock,
    ):
        mock.return_value = rnc_csv_acquisition(_registradas_bytes(), "registradas")
        from agrobr.rnc import registradas

        df = await registradas(grupo="FLORESTAIS")
        assert len(df) > 0
        assert all("FLORESTAIS" in v for v in df["grupo"].values)


@pytest.mark.asyncio
async def test_registradas_filter_mantenedor_literal():
    rows = list(csv.reader(io.StringIO(_registradas_bytes().decode("utf-8-sig"))))[:3]
    rows[1][-1] = "BASF S/A"
    rows[2][-1] = "BASF S.A"
    stream = io.StringIO()
    csv.writer(stream).writerows(rows)
    captured = rnc_csv_acquisition(stream.getvalue().encode("utf-8"), "registradas")

    with patch("agrobr.rnc.client.fetch_registradas_bundle", AsyncMock(return_value=captured)):
        df = await api.registradas(mantenedor="BASF S.A")

    assert df["mantenedor"].tolist() == ["BASF S.A"]


@pytest.mark.asyncio
async def test_protegidas_filter_cultivar():
    with (
        patch("agrobr.rnc.client.fetch_protegidas_bundle", new_callable=AsyncMock) as mock,
    ):
        mock.return_value = rnc_csv_acquisition(_protegidas_bytes(), "protegidas")
        from agrobr.rnc import protegidas

        df = await protegidas(cultivar="BRS")
        assert len(df) > 0
        assert all("BRS" in v for v in df["cultivar"].values)


@pytest.mark.asyncio
async def test_protegidas_filter_titular():
    with (
        patch("agrobr.rnc.client.fetch_protegidas_bundle", new_callable=AsyncMock) as mock,
    ):
        mock.return_value = rnc_csv_acquisition(_protegidas_bytes(), "protegidas")
        from agrobr.rnc import protegidas

        df = await protegidas(titular="SAKATA")
        assert len(df) > 0
        assert all("SAKATA" in v for v in df["titular"].values)


@pytest.mark.asyncio
async def test_registradas_combined_filters():
    with (
        patch("agrobr.rnc.client.fetch_registradas_bundle", new_callable=AsyncMock) as mock,
    ):
        mock.return_value = rnc_csv_acquisition(_registradas_bytes(), "registradas")
        from agrobr.rnc import registradas

        df = await registradas(especie="Abacate", cultivar="Bonella")
        assert len(df) > 0
        assert all("Abacate" in v for v in df["nome_comum"].values)
        assert all("Bonella" in v for v in df["cultivar"].values)
