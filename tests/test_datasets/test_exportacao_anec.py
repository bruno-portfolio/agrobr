from __future__ import annotations

import pandas as pd

from agrobr.datasets import registry


def _mock_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "porto": "SANTOS",
                "produto": "soybean",
                "periodo": "last_week",
                "valor_ton": 275041.0,
            },
            {
                "porto": "PARANAGUÁ",
                "produto": "soybean",
                "periodo": "last_week",
                "valor_ton": 332376.0,
            },
        ]
    )


class TestRegistry:
    def test_describe_returns_anec_info(self):
        text = registry.describe("embarques_anec")
        assert "ANEC" in text
        assert "weekly" in text
        assert "zona_cinza" in text
