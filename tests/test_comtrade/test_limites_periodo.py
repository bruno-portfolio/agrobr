from __future__ import annotations

from datetime import UTC, datetime

import pytest

from agrobr import comtrade, datasets
from agrobr.comtrade import api
from agrobr.exceptions import InvalidParameterError
from agrobr.utils import time as time_utils


@pytest.mark.parametrize(
    "consulta", [comtrade.comercio, comtrade.trade_mirror, datasets.comercio_internacional]
)
@pytest.mark.parametrize("periodo", [1900, 9999, "1961,2024", "2024-9999", "190001", "999912"])
async def test_periodo_fora_do_historico_falha_antes_da_rede(consulta, periodo):
    opcoes = {"periodo": periodo}
    if consulta is comtrade.trade_mirror:
        opcoes["partner"] = "CN"
    if isinstance(periodo, str) and len(periodo) == 6:
        opcoes["frequencia" if consulta is datasets.comercio_internacional else "freq"] = "M"
    with pytest.raises(InvalidParameterError, match="1962"):
        await consulta("1201", **opcoes)


def test_periodo_padrao_e_limite_usam_brasilia_na_virada_do_ano(monkeypatch):
    monkeypatch.setattr(time_utils, "utcnow_aware", lambda: datetime(2026, 1, 1, 1, tzinfo=UTC))
    assert api.prepare_query("1201").periods == ["2024"]
    with pytest.raises(InvalidParameterError, match="2025"):
        api.prepare_query("1201", periodo=2026)
