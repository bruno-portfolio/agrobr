from unittest.mock import AsyncMock, patch

import pandas as pd

from agrobr.datasets.deterministic import deterministic
from agrobr.datasets.preco_diario import PrecoDiarioDataset, _fetch_cepea

from .conftest import make_source, mock_source_meta


def _indicadores() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "data": [pd.Timestamp("2025-01-14")],
            "valor": [144.80],
            "unidade": ["R$/saca 60kg"],
            "produto": ["soja"],
            "fonte": ["cepea"],
            "praca": ["Paranaguá"],
        }
    )


async def test_fetch_cepea_limita_fim_dd_mm_aaaa_ao_snapshot():
    with patch("agrobr.cepea.indicador", new_callable=AsyncMock) as indicador:
        indicador.return_value = (_indicadores(), mock_source_meta())

        async with deterministic("2025-01-15"):
            await _fetch_cepea("soja", fim="16/01/2025")
            await _fetch_cepea("soja", fim="10/01/2025")

    assert [chamada.kwargs["fim"] for chamada in indicador.await_args_list] == [
        "2025-01-15",
        "10/01/2025",
    ]


async def test_dataset_limita_fim_dd_mm_aaaa_ao_snapshot():
    dataset = PrecoDiarioDataset()
    fonte = make_source(_indicadores())

    with patch.object(dataset.info.sources[0], "fetch_fn", fonte):
        async with deterministic("2025-01-15"):
            await dataset.fetch("soja", fim="16/01/2025")

    assert fonte.call_args.kwargs["fim"] == "2025-01-15"
