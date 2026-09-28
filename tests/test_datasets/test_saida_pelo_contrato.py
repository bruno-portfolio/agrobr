from __future__ import annotations

import datetime as dt
from dataclasses import replace
from typing import Any

import pandas as pd
import pytest

from agrobr.contracts import ColumnType, get_contract
from agrobr.datasets.base import BaseDataset, DatasetInfo

from .conftest import mock_source_meta

FORA_DE_ORDEM = (
    "balanco",
    "censo_agropecuario_legado",
    "comparacao_anual_anec",
    "pib_agro",
    "producao_anual",
    "queimadas",
    "serie_historica_safra",
    "clima",
)
DATAS = (ColumnType.DATE, ColumnType.DATETIME)
EXTRA = "extra_da_fonte"


def _saida_da_fonte(nome: str) -> pd.DataFrame:
    linha = {
        coluna.name: dt.date(2024, 3, 1) if coluna.type in DATAS else None
        for coluna in reversed(get_contract(nome).columns)
    }
    return pd.DataFrame([{**linha, EXTRA: "x"}])


def _dataset(nome: str) -> BaseDataset:
    class Falso(BaseDataset):
        info = DatasetInfo(name=nome, description="saída na ordem da fonte")

        async def fetch(self, _produto: str, return_meta: bool = False, **_kwargs: Any) -> Any:
            frame = _saida_da_fonte(nome)
            if return_meta:
                return frame, replace(mock_source_meta(), columns=list(frame.columns))
            return frame

    return Falso()


def _dataset_polars(nome: str) -> BaseDataset:
    class Nativo(BaseDataset):
        info = DatasetInfo(name=nome, description="polars direto da fonte")

        async def fetch(
            self, _produto: str, return_meta: bool = False, as_polars: bool = False, **_kwargs: Any
        ) -> Any:
            frame: Any = _saida_da_fonte(nome)
            if as_polars:
                frame = pytest.importorskip("polars").from_pandas(frame)
            return (frame, mock_source_meta()) if return_meta else frame

    return Nativo()


def _ordem_do_contrato(nome: str) -> list[str]:
    return [*get_contract(nome).empty_frame().columns, EXTRA]


def _datas(nome: str) -> list[str]:
    return [coluna.name for coluna in get_contract(nome).columns if coluna.type in DATAS]


@pytest.mark.parametrize("nome", FORA_DE_ORDEM)
async def test_saida_com_dado_segue_a_ordem_do_contrato(nome: str):
    frame, meta = await _dataset(nome).fetch("x", return_meta=True)

    assert list(frame.columns) == _ordem_do_contrato(nome)
    assert meta.columns == _ordem_do_contrato(nome)


async def test_data_do_contrato_em_objeto_sai_em_datetime64_ns():
    frame = await _dataset("queimadas").fetch("x")

    assert {coluna: str(frame[coluna].dtype) for coluna in _datas("queimadas")} == dict.fromkeys(
        _datas("queimadas"), "datetime64[ns]"
    )
    assert frame["data"].tolist() == [pd.Timestamp(2024, 3, 1)]


async def test_data_do_contrato_sai_em_datetime_ns_no_polars():
    pl = pytest.importorskip("polars")
    frame = await _dataset("queimadas").fetch("x", as_polars=True)

    assert frame.columns == _ordem_do_contrato("queimadas")
    assert {coluna: frame.schema[coluna] for coluna in _datas("queimadas")} == dict.fromkeys(
        _datas("queimadas"), pl.Datetime("ns")
    )


async def test_polars_da_fonte_com_date_sai_em_datetime_ns_e_na_ordem():
    pl = pytest.importorskip("polars")
    frame = await _dataset_polars("queimadas").fetch("x", as_polars=True)

    assert frame.columns == _ordem_do_contrato("queimadas")
    assert {coluna: frame.schema[coluna] for coluna in _datas("queimadas")} == dict.fromkeys(
        _datas("queimadas"), pl.Datetime("ns")
    )
    assert frame["data"].to_list() == [dt.datetime(2024, 3, 1)]
