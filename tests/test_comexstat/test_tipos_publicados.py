from __future__ import annotations

import csv
import io
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pytest

from agrobr import comexstat, datasets
from agrobr.comexstat import query
from agrobr.exceptions import InvalidParameterError
from agrobr.utils import time as time_utils
from tests.helpers import install_comexstat_http

GOLDEN = Path(__file__).parents[1] / "golden_data/comexstat/integridade20260908"


def test_ano_padrao_e_limite_usam_brasilia_na_virada_do_ano(monkeypatch):
    monkeypatch.setattr(time_utils, "utcnow_aware", lambda: datetime(2026, 1, 1, 1, tzinfo=UTC))
    assert query.build_query(fluxo="exportacao", produto="soja").ano == 2024
    with pytest.raises(InvalidParameterError, match="2025"):
        query.build_query(fluxo="exportacao", produto="soja", ano=2026)


@pytest.mark.parametrize("fluxo", ["exportacao", "importacao"])
@pytest.mark.parametrize("camada", ["fonte", "dataset"])
@pytest.mark.parametrize("polars", [False, True])
async def test_comercio_real_cheio_vazio_tipos_e_valores(monkeypatch, fluxo, camada, polars):
    if polars:
        pytest.importorskip("polars")
    arquivo = "EXP_2025.csv" if fluxo == "exportacao" else "IMP_2025.csv"
    corpo = (GOLDEN / arquivo).read_bytes()
    publicados = list(csv.DictReader(io.StringIO(corpo.decode("utf-8-sig")), delimiter=";"))
    codigos = ("1507",)
    produto = "oleo_soja"
    esperados = [linha for linha in publicados if linha["CO_NCM"].startswith(codigos)]
    assert esperados
    uf = esperados[0]["SG_UF_NCM"]
    esperados = [linha for linha in esperados if linha["SG_UF_NCM"] == uf]
    install_comexstat_http(monkeypatch, corpo)
    consulta = getattr(datasets if camada == "dataset" else comexstat, fluxo)
    cheio = await consulta(produto, ano=2025, uf=uf, as_polars=polars)
    vazio_uf = next(
        estado
        for estado in ("AC", "AP", "RR", "DF")
        if not any(
            linha["SG_UF_NCM"] == estado and linha["CO_NCM"].startswith(codigos)
            for linha in publicados
        )
    )
    vazio = await consulta(produto, ano=2025, uf=vazio_uf, as_polars=polars)
    esperado = sum(int(linha["KG_LIQUIDO"]) for linha in esperados)
    assert cheio["kg_liquido"].sum() == float(esperado)
    if polars:
        assert vazio.is_empty()
        assert cheio.schema == vazio.schema
    else:
        pd.testing.assert_frame_equal(cheio.iloc[:0], vazio)
        assert str(cheio["kg_liquido"].dtype) == "float64"
        for coluna in ("produto", "uf") if camada == "dataset" else ("ncm", "uf"):
            assert cheio[coluna].dtype == (
                pd.Series([""]).dtype if camada == "dataset" else pd.StringDtype(storage="python")
            )
