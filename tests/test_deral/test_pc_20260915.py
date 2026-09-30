from __future__ import annotations

import json
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import pandas as pd
import pytest

from agrobr import datasets
from agrobr.deral import api, parser
from agrobr.exceptions import InvalidParameterError, ParseError
from agrobr.utils import io as excel_io

GOLDEN = Path(__file__).parents[1] / "golden_data" / "deral" / "pc_20260915"


@pytest.fixture
def capture() -> bytes:
    return (GOLDEN / "response.xls").read_bytes()


@pytest.fixture
def expected() -> dict[str, Any]:
    return json.loads((GOLDEN / "expected.json").read_text(encoding="utf-8"))


@pytest.mark.parametrize(
    "published,normalized",
    [
        (datetime(2026, 9, 14), "14/09/2026"),
        ("Referência: 14/09/2026", "14/09/2026"),
        ("14-09-2026", "14/09/2026"),
        ("15-09-25", "15/09/2025"),
        ("01-01-99", "01/01/2099"),
    ],
)
def test_data_fora_primeiras_dez_linhas_e_colunas(published, normalized):
    df = pd.DataFrame([[None] * 12 for _ in range(21)], dtype=object)
    df.iat[20, 11] = published
    assert parser._find_data_referencia(df, "Atual") == normalized


@pytest.mark.parametrize("sheet_name", ["Atual", "15-09-25"])
def test_aba_sem_data_nao_usa_nome(capture, sheet_name):
    with excel_io.open_excel_safe(capture, source="deral") as xls:
        df = pd.read_excel(xls, sheet_name="Atual", header=None)
    df.iat[20, 11] = None

    with pytest.raises(ParseError, match=f"Data de referência não encontrada na aba {sheet_name}"):
        parser._extract_multi_produto_sheet(df, sheet_name)


def test_data_calendario_invalida_falha():
    df = pd.DataFrame([["Referência: 31/02/2026"]])
    with pytest.raises(ParseError, match="Data de referência não encontrada na aba Atual"):
        parser._find_data_referencia(df, "Atual")


async def test_source_method_leitor_xlsx_real(monkeypatch):
    sheet = pd.DataFrame(
        [
            ["Data:", datetime(2026, 9, 14), None, None, None, None, None],
            ["Cultura", "Plantada", "Colhida", "Ruim", "Média", "Boa", "Condição"],
            ["Soja", 90.0, 0.0, "-", 4.0, 96.0, None],
            ["Fonte: DERAL", None, None, None, None, None, None],
            ["Nota", None, None, None, None, None, None],
            ["Elaboração", None, None, None, None, None, None],
        ]
    )
    stream = BytesIO()
    with pd.ExcelWriter(stream, engine="openpyxl") as writer:
        sheet.to_excel(writer, sheet_name="Atual", header=False, index=False)
    monkeypatch.setattr(api.client, "fetch_pc_xls", AsyncMock(return_value=stream.getvalue()))
    df, meta = await api.condicao_lavouras(return_meta=True)

    assert meta.source_method == "httpx+openpyxl"
    assert df["data"].eq(pd.Timestamp("2026-09-14")).all()
    assert df.loc[df["condicao"] == "ruim", "pct"].tolist() == [0.0]
    assert df[["plantio_pct", "colheita_pct"]].drop_duplicates().values.tolist() == [[90.0, 0.0]]


@pytest.mark.parametrize("consulta", [api.condicao_lavouras, datasets.condicao_lavouras])
@pytest.mark.parametrize("produto", ["xx", "mandioca", 5, "", 0, []])
async def test_produto_fora_da_planilha_recusado_antes_da_rede(monkeypatch, consulta, produto):
    baixar = AsyncMock()
    monkeypatch.setattr(api.client, "fetch_pc_xls", baixar)

    with pytest.raises(InvalidParameterError, match="Valores válidos: \\['cafe', 'cevada'"):
        await consulta(produto)

    baixar.assert_not_awaited()


@pytest.mark.parametrize("consulta", [api.condicao_lavouras, datasets.condicao_lavouras])
@pytest.mark.parametrize(
    ("produto", "publicados"),
    [
        ("Soja", {"soja"}),
        ("soybean", {"soja"}),
        ("feijão", {"feijao_1", "feijao_2"}),
        ("milho", {"milho_1", "milho_2"}),
        ("Milho 2ª safra", {"milho_2"}),
    ],
)
async def test_fonte_e_dataset_aceitam_os_mesmos_nomes(
    monkeypatch, capture, consulta, produto, publicados
):
    monkeypatch.setattr(api.client, "fetch_pc_xls", AsyncMock(return_value=capture))

    with pytest.warns(UserWarning):
        df = await consulta(produto)

    assert set(df["produto"]) == publicados


async def test_produto_sem_linhas_na_planilha_mantem_os_dtypes_do_cheio(monkeypatch):
    amostra = (GOLDEN.parent / "pc_sample" / "response.xlsx").read_bytes()
    monkeypatch.setattr(api.client, "fetch_pc_xls", AsyncMock(return_value=amostra))

    cheio = await api.condicao_lavouras()
    vazio = await api.condicao_lavouras("trigo")

    assert len(cheio) and vazio.empty
    assert cheio["data"].dtype == "datetime64[ns]"
    assert vazio.dtypes.to_dict() == cheio.dtypes.to_dict()
