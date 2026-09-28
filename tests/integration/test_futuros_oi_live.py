from __future__ import annotations

import pandas as pd
import pytest

from agrobr import contracts, datasets

pytestmark = [pytest.mark.integration, pytest.mark.timeout(300)]


async def test_futuros_oi_historico_intervalo_recente():
    yesterday = pd.Timestamp.now(tz="UTC").normalize() - pd.Timedelta(days=1)
    days = pd.bdate_range(end=yesterday, periods=5)
    frame, meta = await datasets.futuros_agricolas(
        "boi",
        tipo="oi_historico",
        inicio=days[0].date().isoformat(),
        fim=days[-1].date().isoformat(),
        return_meta=True,
    )
    assert not frame.empty
    assert set(frame["ticker"]) == {"BGI"}
    assert "futuro" in set(frame["tipo"])
    returned = set(frame["data"].dt.date)
    assert returned.issubset(set(days.date))
    assert not frame.duplicated(["data", "ticker_completo"]).any()
    assert meta.selected_source == "b3"
    assert meta.attempted_sources == ["b3"]
    assert meta.records_count == len(frame)
    assert not meta.from_cache
    for day in set(days.date) - returned:
        assert any(day.isoformat() in warning for warning in meta.validation_warnings)
    contracts.validate_dataset(frame, "posicoes_abertas")
