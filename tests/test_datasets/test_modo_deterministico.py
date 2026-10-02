from __future__ import annotations

import warnings
from datetime import date
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock

import pandas as pd
import pytest

from agrobr import conab, constants, datasets, snapshots
from agrobr.cache import duckdb_store
from agrobr.cepea import api as cepea_api
from agrobr.datasets.deterministic import deterministic
from agrobr.exceptions import SourceUnavailableError
from agrobr.models import Indicador
from tests.helpers import levanta_exatamente, sem_excecao

from .conftest import mock_source_meta
from .test_balanco import _mock_df as balanco_df
from .test_estimativa_safra import _mock_df as safra_df

SNAPSHOT = "2024-12-31"


def _aviso(dataset: str) -> str:
    return (
        f"{dataset}: o modo determinístico não se aplica a este dataset; o dado é o corrente, "
        f"e não o de {SNAPSHOT}"
    )


@pytest.fixture
def cache(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    store = duckdb_store.DuckDBStore(constants.CacheSettings(cache_dir=tmp_path / "cache"))
    monkeypatch.setattr(cepea_api, "get_store", lambda: store)
    monkeypatch.setattr(duckdb_store, "get_store", lambda: store)
    yield store
    store.close()


@pytest.mark.parametrize(
    ("dataset", "alvo", "frame", "consulta"),
    [
        ("balanco", "balanco", balanco_df, lambda: datasets.balanco("soja", return_meta=True)),
        (
            "estimativa_safra",
            "safras",
            safra_df,
            lambda: datasets.estimativa_safra("soja", safra="2024/25", return_meta=True),
        ),
    ],
)
async def test_dataset_que_vai_a_rede_avisa_que_o_modo_nao_se_aplica(
    monkeypatch, dataset, alvo, frame, consulta
):
    espiao = AsyncMock(return_value=(frame(), mock_source_meta()))
    monkeypatch.setattr(conab, alvo, espiao)

    with warnings.catch_warnings(record=True) as emitidos, sem_excecao():
        warnings.simplefilter("always")
        async with deterministic(SNAPSHOT):
            _, meta = await consulta()

    espiao.assert_awaited()
    assert meta.snapshot == SNAPSHOT
    assert _aviso(dataset) in meta.validation_warnings
    assert [
        str(aviso.message) for aviso in emitidos if "modo determinístico" in str(aviso.message)
    ] == [_aviso(dataset)]


async def test_preco_diario_honra_o_modo_e_nao_avisa(cache):
    cache.indicadores_upsert(
        cepea_api._indicadores_to_dicts(
            [
                Indicador(
                    fonte=constants.Fonte.CEPEA,
                    produto="soja",
                    praca="Paranaguá/PR",
                    data=date(2024, 12, 20),
                    valor=Decimal("140"),
                    unidade="BRL/sc60kg",
                    parser_version=2,
                )
            ]
        )
    )

    with warnings.catch_warnings(record=True) as emitidos, sem_excecao():
        warnings.simplefilter("always")
        async with deterministic(SNAPSHOT):
            frame, meta = await datasets.preco_diario("soja", inicio="2024-12-01", return_meta=True)

    assert frame["valor"].tolist() == [140.0]
    assert not [aviso for aviso in meta.validation_warnings if "modo determinístico" in aviso]
    assert not [aviso for aviso in emitidos if "modo determinístico" in str(aviso.message)]


@pytest.mark.usefixtures("cache")
async def test_preco_diario_sem_o_produto_no_cache_levanta():
    with levanta_exatamente(
        SourceUnavailableError, "modo determinístico: sem dado no cache local para soja"
    ):
        async with deterministic(SNAPSHOT):
            await datasets.preco_diario("soja")


async def test_preco_diario_com_o_produto_no_cache_e_periodo_vazio_nao_levanta(cache):
    cache.indicadores_upsert(
        cepea_api._indicadores_to_dicts(
            [
                Indicador(
                    fonte=constants.Fonte.CEPEA,
                    produto="soja",
                    praca="Paranaguá/PR",
                    data=date(2025, 3, 10),
                    valor=Decimal("140"),
                    unidade="BRL/sc60kg",
                    parser_version=2,
                )
            ]
        )
    )

    with sem_excecao():
        async with deterministic(SNAPSHOT):
            frame = await datasets.preco_diario("soja")

    assert frame.empty


async def _fonte_com_parquet(path: Path, manifest: snapshots.SnapshotManifest) -> None:
    pd.DataFrame({"valor": [1.0]}).to_parquet(path / "sample.parquet")
    manifest.files[f"{path.name}/sample.parquet"] = {"rows": 1, "columns": ["valor"]}


@pytest.fixture
def snapshot_root(tmp_path, monkeypatch):
    monkeypatch.setattr(snapshots, "get_snapshots_dir", lambda: tmp_path)
    monkeypatch.setattr(snapshots.importlib.util, "find_spec", lambda _name: object())
    return tmp_path


async def test_snapshot_com_o_nome_em_outra_caixa_nao_acusa_adulteracao(snapshot_root, monkeypatch):
    pytest.importorskip("pyarrow")
    monkeypatch.setattr(snapshots, "_snapshot_cepea", _fonte_com_parquet)
    await snapshots.create_snapshot("caixa", ["cepea"])
    original = snapshots.load_from_snapshot("cepea", "sample", "caixa")
    sem_distincao = (snapshot_root / "caixa" / "CEPEA" / "Sample.parquet").exists()

    with sem_excecao():
        outra_caixa = snapshots.load_from_snapshot("CEPEA", "Sample", "caixa")
        inexistente = snapshots.load_from_snapshot("cepea", "inexistente", "caixa")

    if sem_distincao:
        pd.testing.assert_frame_equal(outra_caixa, original)
    else:
        assert outra_caixa is None
    assert inexistente is None
