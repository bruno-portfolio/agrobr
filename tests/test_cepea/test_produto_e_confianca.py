from __future__ import annotations

import inspect
import warnings
from datetime import date
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from agrobr import constants
from agrobr.cache import duckdb_store
from agrobr.cepea import api, client
from agrobr.cepea.parsers import detector, v1
from agrobr.exceptions import InvalidParameterError

GOLDEN = Path(__file__).resolve().parents[1] / "golden_data"


@pytest.fixture
def cache(tmp_path, monkeypatch):
    store = duckdb_store.DuckDBStore(constants.CacheSettings(cache_dir=tmp_path / "cache"))
    monkeypatch.setattr(api, "get_store", lambda: store)
    monkeypatch.setattr(api, "_today", lambda: date(2026, 9, 23))
    monkeypatch.setattr(api, "_warn_license", lambda: None)
    yield store
    store.close()


@pytest.mark.usefixtures("cache")
async def test_produto_acentuado_preserva_preco_e_unidade_da_fonte(monkeypatch):
    html = (GOLDEN / "reconciliacao_r6_20260918/na_cafe.html").read_text(encoding="utf-8")
    download = AsyncMock(return_value=client.FetchResult(html, "noticias_agricolas"))
    monkeypatch.setattr(client, "fetch_indicador_page", download)
    frame = await api.indicador(" CAFÉ ", inicio="2026-09-17", fim="2026-09-17", force_refresh=True)
    download.assert_awaited_once_with("cafe")
    assert frame["produto"].tolist() == ["cafe"]
    assert frame["valor"].tolist() == [1541.97]
    assert frame["unidade"].tolist() == ["BRL/sc60kg"]


@pytest.mark.usefixtures("cache")
@pytest.mark.parametrize("produto", ["frango", "chicken", "etanol", "ethanol"])
async def test_nome_generico_nao_escolhe_variedade_e_e_recusado_antes_da_rede(monkeypatch, produto):
    download = AsyncMock()
    monkeypatch.setattr(client, "fetch_indicador_page", download)
    with pytest.raises(InvalidParameterError, match="Produto inválido"):
        await api.indicador(produto, force_refresh=True)
    download.assert_not_awaited()


@pytest.mark.usefixtures("cache")
async def test_moeda_sem_efeito_sai_da_assinatura_publica():
    assert "_moeda" not in inspect.signature(api.indicador).parameters
    with pytest.raises(TypeError, match="_moeda"):
        await api.indicador("soja", _moeda="USD", offline=True)


@pytest.mark.usefixtures("cache")
async def test_confianca_baixa_avisa_por_aquisicao_e_nao_inventa_aviso_no_cache(monkeypatch):
    html = (GOLDEN / "cepea/cache_ttl_20260923/soja_20260923.html").read_text(encoding="utf-8")
    download = AsyncMock(return_value=client.FetchResult(html, "cepea"))
    monkeypatch.setattr(client, "fetch_indicador_page", download)
    monkeypatch.setattr(v1.CepeaParserV1, "can_parse", lambda _self, _html: (True, 0.8))
    monkeypatch.setattr(detector, "PARSERS", [v1.CepeaParserV1])
    for _ in range(2):
        with pytest.warns(UserWarning, match="confiança.*80") as avisos:
            frame, meta = await api.indicador(
                "soja",
                inicio="2026-09-23",
                fim="2026-09-23",
                force_refresh=True,
                return_meta=True,
            )
        assert meta.validation_warnings == [
            str(aviso.message) for aviso in avisos if issubclass(aviso.category, UserWarning)
        ]
        assert frame["valor"].tolist() == [161.65]
        assert frame["unidade"].tolist() == ["BRL/sc60kg"]
    assert download.await_count == 2

    with warnings.catch_warnings(record=True) as avisos_cache:
        warnings.simplefilter("always")
        frame_cache, meta_cache = await api.indicador(
            "soja", inicio="2026-09-23", fim="2026-09-23", offline=True, return_meta=True
        )
    assert meta_cache.from_cache
    assert meta_cache.validation_warnings == []
    assert not avisos_cache
    assert frame_cache["valor"].tolist() == [161.65]
    assert download.await_count == 2


async def test_detector_nao_atribui_ao_selecionado_a_confianca_do_parser_que_falhou(monkeypatch):
    class Vazio(v1.CepeaParserV1):
        def can_parse(self, _html):
            return True, 0.8

        def parse(self, _html, _produto):
            return []

    monkeypatch.setattr(v1.CepeaParserV1, "can_parse", lambda _self, _html: (True, 0.95))
    monkeypatch.setattr(detector, "PARSERS", [v1.CepeaParserV1, Vazio])
    html = (GOLDEN / "cepea/cache_ttl_20260923/soja_20260923.html").read_text(encoding="utf-8")
    avisos = []
    selecionado, indicadores = await detector.get_parser_with_fallback(html, "soja", avisos=avisos)
    assert type(selecionado) is v1.CepeaParserV1
    assert avisos == []
    assert float(indicadores[0].valor) == 161.65
