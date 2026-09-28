from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from agrobr.cepea import api, client
from agrobr.cepea.parsers import v1
from agrobr.exceptions import ParseError, SourceUnavailableError
from agrobr.utils import time as time_utils

PAGES = Path(__file__).resolve().parents[1] / "golden_data" / "cepea" / "pages_20260905"


@pytest.mark.parametrize("page", ["leite.html", "leite_20260904.html"])
def test_leite_mensal_exclui_spot(page):
    records = v1.CepeaParserV1().parse((PAGES / page).read_text(encoding="utf-8"), "leite")
    assert max(record.data for record in records) == date(2026, 7, 1)
    assert all(record.data.day == 1 and record.unidade == "BRL/L" for record in records)
    assert next(
        record.valor
        for record in records
        if record.data == date(2026, 7, 1) and record.praca == "RS"
    ) == Decimal("2.6560")


@pytest.mark.parametrize("entrypoint", ["indicador", "ultimo"])
async def test_fallback_por_conteudo(entrypoint, monkeypatch):
    store = MagicMock()
    store.indicadores_query.return_value = []
    monkeypatch.setattr(api, "get_store", lambda: store)
    monkeypatch.setattr(client, "_use_alternative_source", True)
    fetch = AsyncMock(
        side_effect=[
            client.FetchResult("<html>Sem dados</html>", "cepea"),
            client.FetchResult("<html>NA</html>", "noticias_agricolas"),
        ]
    )
    monkeypatch.setattr(client, "fetch_indicador_page", fetch)
    record = api.Indicador(
        fonte=api.constants.Fonte.NOTICIAS_AGRICOLAS,
        produto="soja",
        praca="Paranaguá/PR",
        data=time_utils.hoje(),
        valor=Decimal("10"),
        unidade="BRL/sc60kg",
    )
    monkeypatch.setattr(api.na_parser, "parse_indicador", lambda _html, _produto: [record])
    if entrypoint == "indicador":
        df, meta = await api.indicador("soja", force_refresh=True, return_meta=True)
        assert len(df) == 1
        assert meta.attempted_sources == ["cepea", "noticias_agricolas"]
    else:
        assert await api.ultimo("soja") == record
    assert fetch.await_args_list[1].kwargs == {"force_alternative": True}


async def test_ultimo_leite_usa_janela_mensal_no_cache(monkeypatch):
    reference = (time_utils.hoje().replace(day=1) - timedelta(days=32)).replace(day=1)
    record = api.Indicador(
        fonte=api.constants.Fonte.CEPEA,
        produto="leite",
        praca="RS",
        data=reference,
        valor=Decimal("2.5"),
        unidade="BRL/L",
    )
    store = MagicMock()
    store.indicadores_query.return_value = api._indicadores_to_dicts([record])
    store.indicadores_ultima_coleta.return_value = None
    monkeypatch.setattr(api, "get_store", lambda: store)
    fetch = AsyncMock()
    monkeypatch.setattr(client, "fetch_indicador_page", fetch)
    assert (await api.ultimo("leite", praca="RS")).data == reference
    fetch.assert_not_awaited()
    assert store.indicadores_query.call_args.kwargs["inicio"].date() < reference


async def test_pagina_sem_dados_e_fallback_indisponivel_levantam_parse_error(monkeypatch):
    fetch = AsyncMock(
        side_effect=[
            client.FetchResult("<html>Sem dados</html>", "cepea"),
            SourceUnavailableError("noticias_agricolas", last_error="HTTP 503"),
        ]
    )
    monkeypatch.setattr(client, "fetch_indicador_page", fetch)
    monkeypatch.setattr(client, "_use_alternative_source", True)
    with pytest.raises(ParseError, match="fallback indisponível.*HTTP 503"):
        await api._fetch_and_parse("soja")
    assert fetch.await_count == 2
