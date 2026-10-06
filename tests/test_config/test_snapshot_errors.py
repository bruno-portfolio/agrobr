from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from agrobr import snapshots
from tests import helpers


@pytest.mark.parametrize("consulta", ["list_snapshots", "get_snapshot"])
async def test_snapshot_parcial_preserva_erros_por_fonte_na_consulta(
    consulta, monkeypatch, tmp_path
):
    monkeypatch.setattr(snapshots.importlib.util, "find_spec", lambda _name: object())
    monkeypatch.setattr(snapshots, "get_snapshots_dir", lambda: tmp_path)
    monkeypatch.setattr(snapshots, "_snapshot_cepea", helpers.make_snapshot_source)
    monkeypatch.setattr(
        snapshots, "_snapshot_conab", AsyncMock(side_effect=OSError("CONAB indisponível"))
    )
    monkeypatch.setattr(
        snapshots, "_snapshot_ibge", AsyncMock(side_effect=OSError("IBGE indisponível"))
    )

    criado = await snapshots.create_snapshot("parcial", sources=["cepea", "conab", "ibge"])
    esperado = {
        "conab": ["CONAB indisponível", "Nenhum conjunto de dados disponível"],
        "ibge": ["IBGE indisponível", "Nenhum conjunto de dados disponível"],
    }
    assert criado.errors == esperado

    if consulta == "list_snapshots":
        listados = snapshots.list_snapshots()
        assert len(listados) == 1
        consultado = listados[0]
    else:
        consultado = snapshots.get_snapshot("parcial")

    assert consultado is not None
    assert consultado.errors == esperado
    assert consultado.name == criado.name
    assert consultado.file_count == criado.file_count == 1
