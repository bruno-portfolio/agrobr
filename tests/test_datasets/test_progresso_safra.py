import pandas as pd


def _make_df(**overrides):
    row = {
        "cultura": "Soja",
        "safra": "2024/25",
        "operacao": "Semeadura",
        "estado": "MT",
        "semana_atual": "2024-11-15",
        "pct_ano_anterior": 0.85,
        "pct_semana_anterior": 0.90,
        "pct_semana_atual": 0.92,
        "pct_media_5_anos": 0.88,
    }
    row.update(overrides)
    return pd.DataFrame([row])
