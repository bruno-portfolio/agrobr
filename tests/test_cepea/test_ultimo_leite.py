from __future__ import annotations

from pathlib import Path
from unittest.mock import Mock

import pytest

from agrobr.cepea import api
from agrobr.cepea.parsers import v1


@pytest.mark.parametrize("praca,expected", [(None, "brasil"), ("BA", "ba")])
async def test_ultimo_leite_prioriza_media_brasil_ou_praca_explicita(monkeypatch, praca, expected):
    path = Path(__file__).parents[1] / "golden_data/cepea/pages_20260905/leite.html"
    records = v1.CepeaParserV1().parse(path.read_text(encoding="utf-8"), "leite")
    store = Mock()
    store.indicadores_query.return_value = api._indicadores_to_dicts(records)
    monkeypatch.setattr(api, "get_store", lambda: store)
    result = await api.ultimo("leite", praca=praca, offline=True)
    assert result.praca.casefold() == expected
    assert store.indicadores_query.call_args.kwargs["praca"] == expected
