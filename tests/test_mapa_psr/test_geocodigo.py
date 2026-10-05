from __future__ import annotations

from pathlib import Path

import pytest

from agrobr import contracts
from agrobr.alt.mapa_psr import api, parser
from tests.helpers import binary_stream, sem_excecao

SEM_GEOCODIGO = (
    Path(__file__).resolve().parents[1]
    / "golden_data/mapa_psr/sem_geocodigo_20260918/apolices.csv"
)


def test_geocodigo_publicado_como_traco_sai_nulo():
    frame = parser.parse_apolices(SEM_GEOCODIGO.read_bytes())
    assert len(frame) == 68
    assert frame["cd_ibge"].isna().all()
    assert frame["municipio"].notna().all()
    assert sorted(frame["ano_apolice"].unique().tolist()) == [2023, 2024]
    with sem_excecao():
        contracts.validate_dataset(frame, "mapa_psr_apolices")


async def test_as_polars_mantem_geocodigo_todo_nulo_como_string(monkeypatch):
    pl = pytest.importorskip("polars")
    monkeypatch.setattr(api, "_resolve_periodos", lambda *_: ["2016-2024"])
    monkeypatch.setattr(
        api.client, "open_periodo", lambda _: binary_stream(SEM_GEOCODIGO.read_bytes())
    )

    frame = await api.apolices(as_polars=True)

    assert frame["cd_ibge"].null_count() == len(frame) == 68
    assert frame.schema["cd_ibge"] == pl.String
