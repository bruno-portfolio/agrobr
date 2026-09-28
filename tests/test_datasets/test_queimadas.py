from pathlib import Path

import pandas as pd
import pytest

from agrobr.contracts import get_contract
from agrobr.datasets.queimadas import QueimadasDataset
from agrobr.queimadas.parser import parse_focos_csv

from .conftest import make_source

GOLDEN_DIR = Path(__file__).parent.parent / "golden_data" / "queimadas" / "focos_sample"


def _make_df(**overrides):
    row = {
        "data": pd.Timestamp("2024-08-15"),
        "hora_gmt": "1530",
        "lat": -12.5,
        "lon": -49.3,
        "satelite": "NOAA-20",
        "municipio": "Palmas",
        "municipio_id": 1721000,
        "estado": "Tocantins",
        "uf": "TO",
        "bioma": "Cerrado",
        "numero_dias_sem_chuva": 45.0,
        "precipitacao": 0.0,
        "risco_fogo": 0.85,
        "frp": 42.5,
    }
    row.update(overrides)
    return pd.DataFrame([row])


class TestQueimadasFetch:
    @pytest.mark.asyncio
    async def test_fetch_real_golden_satisfies_contract(self, monkeypatch):
        source_df = parse_focos_csv(GOLDEN_DIR.joinpath("response.csv").read_bytes())
        dataset = QueimadasDataset()
        monkeypatch.setattr(
            dataset.info.sources[0],
            "fetch_fn",
            make_source(source_df),
        )

        df = await dataset.fetch(ano=2025, mes=1)

        assert isinstance(df, pd.DataFrame), type(df)
        assert (df["risco_fogo"].dropna() >= 0).all()
        assert df["risco_fogo"].isna().any()

    @pytest.mark.parametrize("as_polars", [False, True], ids=["pandas", "polars"])
    async def test_golden_sai_na_ordem_do_contrato_e_com_data_em_ns(self, monkeypatch, as_polars):
        if as_polars:
            pl = pytest.importorskip("polars")
        source_df = parse_focos_csv(GOLDEN_DIR.joinpath("response.csv").read_bytes())
        dataset = QueimadasDataset()
        monkeypatch.setattr(dataset.info.sources[0], "fetch_fn", make_source(source_df))

        df = await dataset.fetch(ano=2025, mes=1, as_polars=as_polars)

        contrato = [coluna.name for coluna in get_contract("queimadas").columns]
        assert list(df.columns)[: len(contrato)] == contrato
        if as_polars:
            assert df.schema["data"] == pl.Datetime("ns")
        else:
            assert str(df["data"].dtype) == "datetime64[ns]"
