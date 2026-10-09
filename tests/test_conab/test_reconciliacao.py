from __future__ import annotations

import copy
import json
from datetime import date
from io import BytesIO
from typing import Any

import pandas as pd
import pytest

from agrobr import conab, datasets
from agrobr.conab.parsers.v1 import ConabParserV1
from agrobr.conab.progresso import parser as progress_parser
from agrobr.contracts import conab as conab_contracts
from agrobr.contracts import estimativa_safra as safra_contracts
from agrobr.exceptions import SourceUnavailableError
from agrobr.ibge import client as ibge_client
from agrobr.ibge import lspa_parser
from tests.helpers import (
    RECONCILIACAO_CONAB_GOLDEN,
    assert_balance_dtypes,
    assert_reconciliation_case,
    install_reconciliacao_conab_http,
    load_reconciliacao_conab_manifest,
)

MANIFEST = load_reconciliacao_conab_manifest()
CASES = MANIFEST["cases"]
EXCEL_CASES = [case for case in CASES if not case["id"].startswith("lspa_")]
SOURCE_CASES = [case for case in CASES if case["id"].startswith("safra_")]
LSPA_CASES = [case for case in CASES if case["id"].startswith("lspa_")]
UNIT_CASES = list(
    {(case["file"], case["sheet"]): case for case in CASES if case["dataset"] == "balanco"}.values()
)
COLUNAS_INVERNO = {
    "B": ("area_plantada", "2024/25"),
    "C": ("area_plantada", "2025/26"),
    "E": ("produtividade", "2024/25"),
    "F": ("produtividade", "2025/26"),
    "H": ("producao", "2024/25"),
    "I": ("producao", "2025/26"),
}
CEREAIS_DE_INVERNO = {
    "aveia": (
        "RS",
        {"B39": 401.8, "C39": 401.8, "E39": 2452, "F39": 2336, "H39": 985.2, "I39": 938.6},
    ),
    "canola": (
        "RS",
        {"B39": 209.9, "C39": 366.3, "E39": 1621, "F39": 1619, "H39": 340.2, "I39": 593},
    ),
    "centeio": ("PR", {"B37": 2.1, "C37": 2.3, "E37": 2401, "F37": 2251, "H37": 5, "I37": 5.2}),
    "cevada": (
        "RS",
        {"B39": 31.4, "C39": 20.3, "E39": 3509, "F39": 2947, "H39": 110.2, "I39": 59.8},
    ),
    "trigo": (
        "RS",
        {"B39": 1156.9, "C39": 774, "E39": 3097, "F39": 2924, "H39": 3582.9, "I39": 2263.2},
    ),
    "triticale": ("RS", {"B39": 3.6, "C39": 3.6, "E39": 3076, "F39": 2764, "H39": 11.1, "I39": 10}),
}


@pytest.fixture(autouse=True)
def sem_periodos_ibge(monkeypatch: pytest.MonkeyPatch) -> None:
    """Os replays deste módulo não trazem o `/periodos`."""

    async def sem_metadado(_table_code: str, _df: object) -> dict[str, object]:
        return {}

    monkeypatch.setattr(ibge_client, "_periodos_modificacao", sem_metadado)


@pytest.mark.parametrize("case", EXCEL_CASES, ids=lambda item: item["id"])
def test_reconciliation_parser_matches_independent_cells(case: dict[str, Any]):
    raw = (RECONCILIACAO_CONAB_GOLDEN / case["file"]).read_bytes()
    expected = copy.deepcopy(case)
    product = case["selection"]["produto"]
    if case["dataset"] == "estimativa_safra":
        records = ConabParserV1().parse_safra_produto(
            BytesIO(raw),
            product,
            safra_ref=case["selection"]["safra"],
            levantamento=case["selection"]["levantamento"],
            data_publicacao=date.fromisoformat(case["publication_date"]),
        )
        frame = pd.DataFrame([record.model_dump() for record in records if record.uf])
        expected["samples"] = [
            item for item in expected["samples"] if item["column"] != "area_colhida"
        ]
        expected["null_columns"] = []
    elif case["dataset"] == "balanco":
        records = ConabParserV1().parse_suprimento(BytesIO(raw), product)
        frame = pd.DataFrame(records).rename(columns={"suprimento_total": "suprimento"})
    else:
        frame = progress_parser.parse_progresso_xlsx(raw).rename(columns={"uf": "estado"})
        if case["expected_keys"]:
            culture = case["expected_keys"][0]["cultura"]
            frame = frame.loc[frame["cultura"] == culture]
        else:
            frame = frame.loc[frame["cultura"] == "Soja"]
    assert_reconciliation_case(frame, expected)


@pytest.mark.asyncio
@pytest.mark.parametrize("case", SOURCE_CASES, ids=lambda item: item["id"])
async def test_reconciliation_conab_source_has_null_harvested_area_without_source_column(
    case: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
):
    requests = install_reconciliacao_conab_http(monkeypatch, case)
    selection = {name: value for name, value in case["selection"].items() if name != "fonte"}
    frame, meta = await conab.safras(**selection, return_meta=True)
    expected = {**case, "null_columns": ["area_colhida"] if not frame.empty else []}
    assert_reconciliation_case(frame, expected)
    if not frame.empty:
        valid, errors = conab_contracts.CONAB_SAFRA_V2.validate(frame)
        assert valid, errors
        assert frame["area_colhida"].dtype == "float64"
    assert meta.source_url == case["source_url"]
    assert meta.parser_version == 3
    assert meta.schema_version == "2.0"
    assert meta.selected_source == "conab"
    assert meta.attempted_sources == ["conab"]
    assert meta.records_count == len(frame)
    assert requests[-1] == case["source_url"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case", [case for case in CASES if case.get("public_replay", True)], ids=lambda item: item["id"]
)
async def test_reconciliation_public_dataset_matches_independent_oracle(
    case: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
):
    requests = install_reconciliacao_conab_http(monkeypatch, case)
    fetch = getattr(datasets, case["dataset"])
    selecao = case["selection"]
    if case["dataset"] == "balanco":
        selecao = {**selecao, "levantamento": case["edition"]["levantamento"]}
    if case["id"] == "safra_2025_26_04_trigo":
        with pytest.warns(
            UserWarning, match="conab responderam sem observações de trigo"
        ) as avisos:
            frame, meta = await fetch(**selecao, return_meta=True)
        pd.testing.assert_frame_equal(frame, safra_contracts.ESTIMATIVA_SAFRA_V3_1.empty_frame())
        assert meta.records_count == 0
        assert meta.contract_version == "3.1"
        assert meta.attempted_sources == ["conab"]
        assert meta.validation_warnings == [
            str(aviso.message)
            for aviso in avisos
            if str(aviso.message).startswith("estimativa_safra:")
        ]
        assert requests[-1] == case["source_url"]
        return
    if case.get("dataset_error"):
        with pytest.raises(SourceUnavailableError):
            await fetch(**selecao, return_meta=True)
        assert requests
        return
    frame, meta = await fetch(**selecao, return_meta=True)
    oraculo = (
        frame.rename(columns={"uf": "estado"}) if case["dataset"] == "progresso_safra" else frame
    )
    assert_reconciliation_case(oraculo, case)
    if case["dataset"] == "balanco":
        assert_balance_dtypes(frame, dataset=True)
    assert meta.dataset == case["dataset"]
    assert meta.validation_passed is True
    source = "ibge_lspa" if case["id"].startswith("lspa_") else "conab"
    assert meta.attempted_sources == [source]
    assert meta.selected_source == source
    assert meta.parser_version == (3 if source == "conab" else 2)
    assert (
        meta.contract_version
        == {"estimativa_safra": "3.1", "balanco": "1.1", "progresso_safra": "2.0"}[case["dataset"]]
    )
    assert meta.schema_version == meta.contract_version
    assert meta.records_count == len(frame)
    if "source_url" in case:
        assert meta.source_url == case["source_url"]
    assert requests


@pytest.mark.parametrize("case", LSPA_CASES, ids=lambda item: item["id"])
def test_reconciliation_lspa_parser_preserves_json_period_unit_and_value(case: dict[str, Any]):
    rows = [
        row
        for file in case["files"]
        for row in json.loads((RECONCILIACAO_CONAB_GOLDEN / file).read_text(encoding="utf-8"))
        if row["D3C"] in case["components"]
    ]
    for component in case["components"]:
        selected = [row for row in rows if row["D3C"] == component]
        frame = lspa_parser.parse_lspa(pd.DataFrame(selected), component)
        assert len(frame) == len(selected)
        for row in selected:
            found = frame.loc[
                (frame["ano"] == int(row["D2C"][:4]))
                & (frame["mes"] == int(row["D2C"][-2:]))
                & (frame["variavel_cod"] == int(row["D4C"]))
            ]
            assert len(found) == 1
            actual = found.iloc[0]
            assert actual["unidade"] == row["MN"]
            assert actual["localidade_cod"] == int(row["D1C"])
            if row["V"] in ("..", "...", "X"):
                assert pd.isna(actual["valor"])
            else:
                assert actual["valor"] == (0 if row["V"] == "-" else float(row["V"]))


@pytest.mark.parametrize("produto", sorted(CEREAIS_DE_INVERNO))
async def test_cereais_de_inverno_ano_civil_e_ano_de_encerramento_da_safra(
    produto: str, monkeypatch: pytest.MonkeyPatch
):
    uf, cells = CEREAIS_DE_INVERNO[produto]
    expected: dict[str, dict[str, float]] = {}
    for cell, value in cells.items():
        field, safra = COLUNAS_INVERNO[cell[0]]
        expected.setdefault(safra, {})[field] = value
    raw = (RECONCILIACAO_CONAB_GOLDEN / "7cd4df7946e5c57f.xlsx").read_bytes()
    records = ConabParserV1().parse_safra_produto(BytesIO(raw), produto, levantamento=12)
    parsed = {
        record.safra: {
            field: float(getattr(record, field))
            for field in ("area_plantada", "produtividade", "producao")
        }
        for record in records
        if record.uf == uf
    }
    assert parsed == expected
    install_reconciliacao_conab_http(
        monkeypatch, next(case for case in CASES if case["id"] == "safra_2025_26_12_trigo")
    )
    for selection in ({"safra": "2025/26", "levantamento": 12}, {}):
        frame = await conab.safras(produto, uf=uf, **selection)
        assert frame[["safra", *expected["2025/26"]]].to_dict("records") == [
            {"safra": "2025/26", **expected["2025/26"]}
        ]
