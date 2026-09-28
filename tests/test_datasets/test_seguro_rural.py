import pytest

from agrobr.datasets.seguro_rural import SeguroRuralDataset
from tests.helpers import levanta_exatamente


class TestSeguroRuralFetch:
    @pytest.mark.asyncio
    async def test_invalid_tipo(self):
        dataset = SeguroRuralDataset()

        with levanta_exatamente(ValueError, match="tipo deve ser"):
            await dataset.fetch(tipo="outro")
