import inspect

import pandas as pd

from agrobr import datasets


def _make_df(**overrides):
    row = {
        "cultura": "Soja",
        "safra": "2024/25",
        "operacao": "Semeadura",
        "uf": "MT",
        "semana_atual": "2024-11-15",
        "pct_ano_anterior": 0.85,
        "pct_semana_anterior": 0.90,
        "pct_semana_atual": 0.92,
        "pct_media_5_anos": 0.88,
    }
    row.update(overrides)
    return pd.DataFrame([row])


def test_assinatura_usa_uf_e_flags_nomeadas():
    parametros = inspect.signature(datasets.progresso_safra).parameters
    assert list(parametros)[:3] == ["produto", "uf", "operacao"]
    assert all(
        parametros[nome].kind is inspect.Parameter.KEYWORD_ONLY
        for nome in ("return_meta", "as_polars", "semana_url")
    )
