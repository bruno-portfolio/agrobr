from __future__ import annotations

import re
import tomllib
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from agrobr import SnapshotError, snapshots

PYPROJECT = Path(__file__).resolve().parents[2] / "pyproject.toml"


async def test_snapshot_sem_engine_nao_cria_diretorio(monkeypatch, tmp_path):
    monkeypatch.setattr(snapshots.importlib.util, "find_spec", lambda _name: None)
    directory = tmp_path / "snapshots"
    monkeypatch.setattr(snapshots, "get_snapshots_dir", lambda: directory)
    extra = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))["project"]["optional-dependencies"]
    piso = next(dep for dep in extra["polars"] if dep.startswith("pyarrow"))
    with pytest.raises(ImportError, match=f'pyarrow é necessário.*pip install "{re.escape(piso)}"'):
        await snapshots.create_snapshot("no-engine")
    assert not directory.exists()


@pytest.mark.parametrize("sources", [["../../outside"], ["CEPEA"], ["cepea", "banana"]])
async def test_snapshot_fontes_invalidas_antes_de_criar_diretorio(sources, monkeypatch, tmp_path):
    monkeypatch.setattr(snapshots.importlib.util, "find_spec", lambda _name: object())
    directory = tmp_path / "snapshots"
    monkeypatch.setattr(snapshots, "get_snapshots_dir", lambda: directory)
    with pytest.raises(ValueError, match="Válidas: cepea, conab, ibge"):
        await snapshots.create_snapshot("bad-source", sources=sources)
    assert not directory.exists()
    assert list(tmp_path.iterdir()) == []


async def test_snapshot_zero_arquivos_remove_diretorio_e_preserva_erros(monkeypatch, tmp_path):
    monkeypatch.setattr(snapshots.importlib.util, "find_spec", lambda _name: object())
    monkeypatch.setattr(snapshots, "get_snapshots_dir", lambda: tmp_path)
    monkeypatch.setattr(
        snapshots, "_snapshot_cepea", AsyncMock(side_effect=OSError("cache inacessível"))
    )
    monkeypatch.setattr(snapshots, "_snapshot_conab", AsyncMock())
    with pytest.raises(SnapshotError, match="cache inacessível") as caught:
        await snapshots.create_snapshot("empty", sources=["cepea", "conab"])
    assert set(caught.value.errors) == {"cepea", "conab"}
    assert not (tmp_path / "empty").exists()
