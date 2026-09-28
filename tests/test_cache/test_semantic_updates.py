from __future__ import annotations

from datetime import date
from unittest.mock import AsyncMock

from agrobr import constants
from agrobr.cache import duckdb_store
from agrobr.cepea import api
from agrobr.models import Indicador


async def test_public_refresh_warm_and_offline_keep_updated_units(monkeypatch, tmp_path):
    store = duckdb_store.DuckDBStore(constants.CacheSettings(cache_dir=tmp_path))
    reference = date.today()
    indicator = Indicador(
        produto="acucar_refinado",
        praca="São Paulo",
        data=reference,
        valor=2.7,
        unidade="BRL/kg",
        fonte=constants.Fonte.CEPEA,
        parser_version=2,
    )
    fetched = api._FetchResult(
        [indicator], "cepea", "https://example.org/indicador", 2, "test", 1, 0
    )
    fetcher = AsyncMock(return_value=fetched)
    monkeypatch.setattr(api, "get_store", lambda: store)
    monkeypatch.setattr(api, "_fetch_and_parse", fetcher)
    try:
        cold = await api.indicador("acucar_refinado", inicio=reference, fim=reference, offline=True)
        assert cold.empty
        fetcher.assert_not_awaited()
        store.indicadores_upsert(
            [
                {
                    "produto": "acucar_refinado",
                    "praca": "São Paulo",
                    "data": reference,
                    "valor": 135.0,
                    "unidade": "BRL/sc50kg",
                    "fonte": "cepea",
                    "parser_version": 1,
                }
            ]
        )
        for options in ({"force_refresh": True}, {}, {"offline": True}):
            frame = await api.indicador(
                "acucar_refinado", inicio=reference, fim=reference, **options
            )
            assert len(frame) == 1
            assert frame.iloc[0].valor == 2.7
            assert frame.iloc[0].unidade == "BRL/kg"
        fetcher.assert_awaited_once()
    finally:
        store.close()
