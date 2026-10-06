from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from unittest.mock import AsyncMock

import pandas as pd
import pytest

from agrobr.alt.anp_diesel import api

GOLDEN = Path(__file__).resolve().parents[1] / "golden_data/anp_diesel/vendas_sample/response.csv"


@pytest.mark.parametrize(
    "inicio",
    ["2013-01-01", date(2013, 1, 1), datetime(2013, 1, 1, 12)],
    ids=["str", "date", "datetime"],
)
@pytest.mark.parametrize(
    "fim",
    ["2013-01-31", date(2013, 1, 31), datetime(2013, 1, 31, 23, 59)],
    ids=["str", "date", "datetime"],
)
async def test_vendas_datas_civis_preservam_janeiro_publicado(inicio, fim, monkeypatch):
    monkeypatch.setattr(api.client, "fetch_vendas_m3", AsyncMock(return_value=GOLDEN.read_bytes()))

    frame = await api.vendas_diesel(uf="RO", inicio=inicio, fim=fim)

    assert len(frame) == 5
    assert frame["data"].tolist() == [pd.Timestamp("2013-01-01")] * 5
    assert frame["uf"].tolist() == ["RO"] * 5
    assert frame["regiao"].tolist() == ["Norte"] * 5
    diesel_s10 = frame.loc[frame["produto"] == "DIESEL S10", "volume_m3"]
    assert diesel_s10.tolist() == [3517.6]
