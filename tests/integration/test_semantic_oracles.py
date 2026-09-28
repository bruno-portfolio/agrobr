from __future__ import annotations

from datetime import date
from unittest.mock import AsyncMock, patch

import pytest

from agrobr import nasa_power
from agrobr.ibge import client as ibge_client
from agrobr.nasa_power import client as nasa_client

pytestmark = [pytest.mark.integration, pytest.mark.timeout(300)]


async def test_sidra_municipal_zero_observations_preserved():
    raw = await ibge_client.fetch_sidra(
        "5457",
        territorial_level="6",
        ibge_territorial_code="in n3 11",
        variable="214",
        period="2023",
        classifications={"782": "40124"},
    )
    assert len(raw) == 52
    assert (raw["V"] == "-").sum() == 8
    normalized = ibge_client.parse_sidra_response(raw)
    assert len(normalized) == 52
    assert (normalized["valor"] == 0).sum() == 8


async def test_nasa_monthly_precipitation_matches_daily_source():
    raw = await nasa_client.fetch_daily(-15.8, -47.9, date(2024, 2, 1), date(2024, 3, 2))
    with patch.object(nasa_client, "fetch_daily", AsyncMock(return_value=raw)):
        df = await nasa_power.clima_ponto(
            -15.8,
            -47.9,
            "2024-02-01",
            "2024-03-02",
            agregacao="mensal",
        )
    assert len(df) == 2
    for row in df.to_dict(orient="records"):
        month = row["mes"].strftime("%Y%m")
        observations = [
            value
            for key, value in raw["properties"]["parameter"]["PRECTOTCORR"].items()
            if key.startswith(month) and value != -999
        ]
        assert len(observations) == (29 if month == "202402" else 2)
        assert row["precip_acum_mm"] == pytest.approx(sum(observations), rel=1e-12)
