from __future__ import annotations

from agrobr.ibge import client


class TestParseSidraResponse:
    """Testes do parser de resposta SIDRA."""

    def test_parse_handles_invalid_values(self):
        """Testa que valores invalidos viram NaN."""
        import pandas as pd

        df = pd.DataFrame(
            {
                "V": ["100", "-", "..."],
            }
        )

        result = client.parse_sidra_response(df)

        assert result["valor"].iloc[0] == 100.0
        assert result["valor"].iloc[1] == 0
        assert pd.isna(result["valor"].iloc[2])
