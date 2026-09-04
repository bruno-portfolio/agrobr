from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from agrobr.defensivos import api, cache
from agrobr.defensivos.parser import parse_formulados_csv, parse_tecnicos_csv
from agrobr.models import MetaInfo

GOLDEN_DIR = Path(__file__).parent.parent / "golden_data" / "defensivos"


def _golden_formulados_bytes() -> bytes:
    return (GOLDEN_DIR / "formulados_sample" / "response.csv").read_bytes()


def _golden_tecnicos_bytes() -> bytes:
    return (GOLDEN_DIR / "tecnicos_sample" / "response.csv").read_bytes()


@pytest.fixture()
def _patch_cache(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(cache, "_cache_dir", lambda: tmp_path)


@pytest.fixture()
def _seed_formulados_cache(_patch_cache):
    raw = _golden_formulados_bytes()
    form_df, auth_df = parse_formulados_csv(raw)
    cache.write_formulados_pair(form_df, auth_df)


@pytest.fixture()
def _seed_tecnicos_cache(_patch_cache):
    raw = _golden_tecnicos_bytes()
    df = parse_tecnicos_csv(raw)
    cache.write_cache("tecnicos", df)


@pytest.mark.usefixtures("_seed_formulados_cache")
class TestFormulados:
    @pytest.mark.asyncio
    async def test_returns_dataframe(self):
        df = await api.formulados()
        assert isinstance(df, pd.DataFrame)
        assert "nr_registro" in df.columns
        assert "marca_comercial" in df.columns

    @pytest.mark.asyncio
    async def test_return_meta(self):
        result = await api.formulados(return_meta=True)
        assert isinstance(result, tuple)
        df, meta = result
        assert isinstance(df, pd.DataFrame)
        assert isinstance(meta, MetaInfo)
        assert meta.source == "defensivos"
        assert meta.parser_version == 2

    @pytest.mark.asyncio
    async def test_filter_ingrediente_ativo(self):
        df = await api.formulados(ingrediente_ativo="GLIFOSATO")
        assert len(df) >= 1
        assert all("GLIFOSATO" in str(v).upper() for v in df["ingrediente_ativo"])

    @pytest.mark.asyncio
    async def test_filter_ingrediente_ativo_literal(self):
        form_df = pd.DataFrame(
            {
                "ingrediente_ativo": ["2,4-D (sal dimetilamina", "2,4-D sal dimetilamina"],
            }
        )
        cached = AsyncMock(
            return_value=(0, "https://example.com/formulados.csv", form_df, pd.DataFrame())
        )
        with patch.object(api, "_ensure_formulados_cached", cached):
            df = await api.formulados(ingrediente_ativo="2,4-D (sal")

        assert df["ingrediente_ativo"].tolist() == ["2,4-D (sal dimetilamina"]

    @pytest.mark.asyncio
    async def test_filter_organicos(self):
        df = await api.formulados(organicos="SIM")
        assert len(df) >= 1
        assert all(v == "SIM" for v in df["organicos"])

    @pytest.mark.asyncio
    async def test_filter_titular(self):
        df = await api.formulados(titular="SYNGENTA")
        assert len(df) >= 1

    @pytest.mark.asyncio
    async def test_filter_classe(self):
        df = await api.formulados(classe="Herbicida")
        assert len(df) >= 1

    @pytest.mark.asyncio
    async def test_cache_hit_no_download(self):
        with patch.object(api.client, "download_formulados", new_callable=AsyncMock) as mock_dl:
            await api.formulados()
            mock_dl.assert_not_called()

    @pytest.mark.asyncio
    async def test_as_polars(self):
        pytest.importorskip("polars")
        result = await api.formulados(as_polars=True)
        assert type(result).__name__ == "DataFrame"
        assert type(result).__module__.startswith("polars")


@pytest.mark.usefixtures("_patch_cache")
class TestFormuladosCacheMiss:
    @pytest.mark.asyncio
    async def test_cache_miss_triggers_download(self):
        raw = _golden_formulados_bytes()
        with patch.object(
            api.client, "download_formulados", new_callable=AsyncMock, return_value=raw
        ) as mock_dl:
            df = await api.formulados()
            mock_dl.assert_called_once()
            assert isinstance(df, pd.DataFrame)
            assert len(df) > 0


@pytest.mark.usefixtures("_seed_formulados_cache")
class TestAutorizacoes:
    @pytest.mark.asyncio
    async def test_returns_dataframe(self):
        df = await api.autorizacoes()
        assert isinstance(df, pd.DataFrame)
        assert "cultura" in df.columns

    @pytest.mark.asyncio
    async def test_filter_cultura(self):
        df = await api.autorizacoes(cultura="SOJA")
        assert len(df) >= 1
        assert all("SOJA" in str(v).upper() for v in df["cultura"])

    @pytest.mark.asyncio
    async def test_filter_nr_registro(self):
        df = await api.autorizacoes(nr_registro="000189")
        assert len(df) >= 1
        assert all(v == "000189" for v in df["nr_registro"])

    @pytest.mark.asyncio
    async def test_as_polars(self):
        pytest.importorskip("polars")
        result = await api.autorizacoes(as_polars=True)
        assert type(result).__name__ == "DataFrame"
        assert type(result).__module__.startswith("polars")


@pytest.mark.usefixtures("_seed_tecnicos_cache")
class TestTecnicos:
    @pytest.mark.asyncio
    async def test_returns_dataframe(self):
        df = await api.tecnicos()
        assert isinstance(df, pd.DataFrame)
        assert "grupo_quimico" in df.columns

    @pytest.mark.asyncio
    async def test_filter_ingrediente_ativo(self):
        df = await api.tecnicos(ingrediente_ativo="GLIFOSATO")
        assert len(df) >= 1

    @pytest.mark.asyncio
    async def test_filter_marca(self):
        df = await api.tecnicos(marca="NORTOX")
        assert len(df) >= 1

    @pytest.mark.asyncio
    async def test_as_polars(self):
        pytest.importorskip("polars")
        result = await api.tecnicos(as_polars=True)
        assert type(result).__name__ == "DataFrame"
        assert type(result).__module__.startswith("polars")

    @pytest.mark.asyncio
    async def test_cache_miss_triggers_download(self, _patch_cache):
        raw = _golden_tecnicos_bytes()
        with patch.object(
            api.client, "download_tecnicos", new_callable=AsyncMock, return_value=raw
        ) as mock_dl:
            cache.invalidate()
            df = await api.tecnicos()
            mock_dl.assert_called_once()
            assert len(df) > 0
