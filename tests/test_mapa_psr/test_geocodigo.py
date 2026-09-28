from __future__ import annotations

from pathlib import Path

from agrobr import contracts
from agrobr.alt.mapa_psr import parser
from tests.helpers import sem_excecao

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
