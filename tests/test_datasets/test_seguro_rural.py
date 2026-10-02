from pathlib import Path

import pytest

from agrobr.alt.mapa_psr import client
from agrobr.datasets.seguro_rural import SeguroRuralDataset, seguro_rural
from tests.helpers import binary_stream, isolated_dataset_case, levanta_exatamente

APOLICES = (
    Path(__file__).parents[1] / "golden_data" / "mapa_psr" / "apolices_sample" / "response.csv"
)


class TestSeguroRuralFetch:
    @pytest.mark.asyncio
    async def test_invalid_tipo(self):
        dataset = SeguroRuralDataset()

        with levanta_exatamente(ValueError, match="tipo deve ser"):
            await dataset.fetch(tipo="outro")


async def test_seguro_rural_uf_com_espaco_filtra_pela_sigla():
    with isolated_dataset_case("uf_com_espaco") as monkeypatch:
        monkeypatch.setattr(client, "open_periodo", lambda _: binary_stream(APOLICES.read_bytes()))
        sigla = await seguro_rural(uf="SP", ano=2007)
        com_espaco = await seguro_rural(uf=" sp ", ano=2007)
    assert len(sigla) > 0
    assert com_espaco.equals(sigla)
