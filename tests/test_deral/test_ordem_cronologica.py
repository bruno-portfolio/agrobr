from __future__ import annotations

import warnings
from pathlib import Path
from unittest.mock import AsyncMock

import pandas as pd
import pytest

from agrobr import datasets
from agrobr.deral import api, parser
from agrobr.utils.result import ATRIBUTO_AVISOS

CAPTURA = Path(__file__).parents[1] / "golden_data" / "deral" / "pc_20260915" / "response.xls"
ABAS_DIVERGENTES = [
    "deral: a aba '19-09-2021' publica a data 20/09/2021 na planilha; "
    "o quadro usa a data da planilha, e não o nome da aba.",
    "deral: a aba '18-12-2017' publica a data 08/01/2018 na planilha; "
    "o quadro usa a data da planilha, e não o nome da aba.",
]


def _avisos_da_fonte(emitidos: list[warnings.WarningMessage]) -> list[str]:
    return [str(aviso.message) for aviso in emitidos if str(aviso.message).startswith("deral:")]


def test_quadro_sai_em_ordem_cronologica_por_produto():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        df = parser.parse_pc_xls(CAPTURA.read_bytes())

    datas = pd.to_datetime(df["data"], format="%d/%m/%Y")
    assert pd.api.types.is_string_dtype(df["data"])
    assert datas.groupby(df["produto"]).apply(lambda serie: serie.is_monotonic_increasing).all()
    assert df.groupby("produto")["data"].last().to_dict() == (
        datas.groupby(df["produto"]).max().dt.strftime("%d/%m/%Y").to_dict()
    )
    assert df.iloc[-1][["produto", "data"]].tolist() == ["trigo", "14/09/2026"]


def test_aba_com_nome_diferente_da_data_avisa():
    with warnings.catch_warnings(record=True) as emitidos:
        warnings.simplefilter("always")
        df = parser.parse_pc_xls(CAPTURA.read_bytes())

    assert df.attrs.get(ATRIBUTO_AVISOS) == ABAS_DIVERGENTES
    assert _avisos_da_fonte(emitidos) == ABAS_DIVERGENTES
    assert df["data"].isin(["20/09/2021", "08/01/2018"]).any()


@pytest.mark.parametrize(
    "consulta",
    [
        lambda: api.condicao_lavouras("soja", return_meta=True),
        lambda: datasets.condicao_lavouras("soja", return_meta=True),
    ],
    ids=["fonte", "dataset"],
)
async def test_aviso_de_aba_chega_ao_meta_com_filtro(monkeypatch, consulta):
    monkeypatch.setattr(api.client, "fetch_pc_xls", AsyncMock(return_value=CAPTURA.read_bytes()))

    with warnings.catch_warnings(record=True) as emitidos:
        warnings.simplefilter("always")
        df, meta = await consulta()

    assert set(df["produto"]) == {"soja"}
    assert [aviso for aviso in meta.validation_warnings if aviso.startswith("deral:")] == (
        ABAS_DIVERGENTES
    )
    assert _avisos_da_fonte(emitidos) == ABAS_DIVERGENTES
