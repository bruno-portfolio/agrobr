from __future__ import annotations

import pandas as pd
import pytest

from agrobr.datasets.clima import ClimaDataset
from agrobr.datasets.preco_diario import PrecoDiarioDataset
from agrobr.utils.result import finalize_result
from tests.helpers import isolated_dataset_case

from .conftest import make_source


def _datas(unidade: str, *valores: str) -> pd.Series:
    return pd.Series(pd.to_datetime(list(valores))).dt.as_unit(unidade)


def _precos() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "data": _datas("s", "2024-01-15", "2024-01-16", "2024-02-15"),
            "valor": [145.0, 146.0, 150.0],
            "unidade": ["R$/sc60kg"] * 3,
            "praca": ["Paranaguá/PR"] * 3,
        }
    )


def _clima() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "mes": _datas("us", "2024-01-01", "2024-02-01"),
            "uf": ["SP", "SP"],
            "precip_acum_mm": [150.0, 120.0],
            "temp_media": [25.0, 26.0],
            "temp_max_media": [30.0, 31.0],
            "temp_min_media": [20.0, 21.0],
            "num_estacoes": pd.array([15, 15], dtype="Int64"),
            "fonte": ["inmet", "inmet"],
            "umidade_media": pd.array([pd.NA, pd.NA], dtype="Float64"),
            "radiacao_media_mj": pd.array([pd.NA, pd.NA], dtype="Float64"),
            "vento_medio_ms": pd.array([pd.NA, pd.NA], dtype="Float64"),
        }
    )


async def _datasets(as_polars: bool):
    with isolated_dataset_case("datas_em_ns"):
        precos = PrecoDiarioDataset()
        precos.info.sources[0].fetch_fn = make_source(_precos())
        clima = ClimaDataset()
        clima.info.sources[0].fetch_fn = make_source(_clima())
        return (
            await precos.fetch("soja", as_polars=as_polars),
            await clima.fetch("sp", ano=2024, as_polars=as_polars),
        )


def test_finalize_result_passa_toda_data_a_ns():
    frame = pd.DataFrame(
        {
            "s": _datas("s", "2024-01-02"),
            "ms": _datas("ms", "2024-01-02"),
            "us": _datas("us", "2024-01-02 10:00:00"),
            "utc": pd.Series(pd.to_datetime(["2024-01-02T10:00:00Z"])).dt.as_unit("us"),
            "ns": _datas("ns", "2024-01-02"),
            "valor": [1.0],
        }
    )

    saida = finalize_result(frame)

    assert {coluna: str(tipo) for coluna, tipo in saida.dtypes.items()} == {
        "s": "datetime64[ns]",
        "ms": "datetime64[ns]",
        "us": "datetime64[ns]",
        "utc": "datetime64[ns, UTC]",
        "ns": "datetime64[ns]",
        "valor": "float64",
    }
    assert saida["us"].tolist() == [pd.Timestamp("2024-01-02 10:00:00")]


async def test_datasets_saem_em_ns_e_se_juntam_pelo_mes():
    precos, clima = await _datasets(as_polars=False)

    assert str(precos["data"].dtype) == "datetime64[ns]"
    assert str(clima["mes"].dtype) == "datetime64[ns]"
    mensal = precos.groupby(precos["data"].dt.to_period("M").dt.start_time.dt.as_unit("ns"))[
        "valor"
    ].mean()
    juntos = clima.merge(mensal.rename("preco").reset_index(), left_on="mes", right_on="data")
    assert juntos[["precip_acum_mm", "preco"]].values.tolist() == [[150.0, 145.5], [120.0, 150.0]]


async def test_datasets_em_polars_se_juntam_pelo_mes():
    pl = pytest.importorskip("polars")
    precos, clima = await _datasets(as_polars=True)

    assert precos.schema["data"] == pl.Datetime("ns")
    assert clima.schema["mes"] == pl.Datetime("ns")
    mensal = precos.group_by(pl.col("data").dt.truncate("1mo").alias("mes")).agg(
        pl.col("valor").mean().alias("preco")
    )
    juntos = clima.join(mensal, on="mes").sort("mes")
    assert juntos.select("precip_acum_mm", "preco").rows() == [(150.0, 145.5), (120.0, 150.0)]
