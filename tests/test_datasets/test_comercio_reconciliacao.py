from __future__ import annotations

import copy
import json
import math
import warnings
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs

import httpx
import pandas as pd
import pytest

from agrobr import constants, datasets
from agrobr.contracts import get_contract
from tests.helpers import collect_failures, isolated_dataset_case

GOLDEN = Path(__file__).parents[1] / "golden_data" / "reconciliacao_comercio_exterior_20260918"
MANIFEST = json.loads((GOLDEN / "manifest.json").read_text(encoding="utf-8"))
CASES = {case["id"]: case for case in MANIFEST["cases"]}
COMEX_HOST = httpx.URL(constants.URLS[constants.Fonte.COMEXSTAT]["bulk_csv"]).host
ABIOVE_HOST = httpx.URL(constants.URLS[constants.Fonte.ABIOVE]["exportacao"]).host
COMTRADE_HOST = httpx.URL(constants.URLS[constants.Fonte.COMTRADE]["base"]).host
COLUNAS_EXPORTACAO = [column.name for column in get_contract("exportacao").columns]
_REAL_ASYNC_CLIENT = httpx.AsyncClient


def _bytes(rel: str) -> bytes:
    return (GOLDEN / rel).resolve().read_bytes()


def _matches(got: Any, expected: Any, tolerance: float) -> bool:
    if expected is None:
        return got is None or (isinstance(got, float) and math.isnan(got))
    if isinstance(expected, (int, float)) and not isinstance(expected, bool):
        return isinstance(got, (int, float)) and abs(float(got) - float(expected)) <= tolerance
    return got == expected


def _assert_samples(frame: pd.DataFrame, case: dict[str, Any]) -> None:
    tolerance = case["tolerancia"]["absoluta"]
    for sample in case["samples"]:
        ausentes = [column for column in sample["key"] if column not in frame.columns]
        assert not ausentes, f"{case['id']}: colunas de chave ausentes na saída: {ausentes}"
        assert sample["column"] in frame.columns, (
            f"{case['id']}: coluna de saída {sample['column']!r} ausente"
        )
        mask = pd.Series(True, index=frame.index)
        for column, value in sample["key"].items():
            mask &= frame[column].astype(str) == str(value)
        rows = frame[mask]
        assert len(rows) == 1, f"{case['id']}: chave {sample['key']} -> {len(rows)} linhas"
        got = rows.iloc[0][sample["column"]]
        assert _matches(got, sample["value"], tolerance), (
            f"{case['id']} {sample['key']} {sample['column']}: saída={got!r} esperado={sample['value']!r}"
        )


def _sintetico() -> tuple[pd.DataFrame, dict[str, Any]]:
    frame = pd.DataFrame(
        [
            {
                "ano": 2025,
                "mes": 3,
                "produto": "soja",
                "uf": "MT",
                "kg_liquido": 184510918.0,
                "valor_fob_usd": 71866118.0,
            },
            {
                "ano": 2025,
                "mes": 4,
                "produto": "soja",
                "uf": "MT",
                "kg_liquido": 2543679973.0,
                "valor_fob_usd": 38917721136.0,
            },
        ]
    )
    samples = []
    for record in frame.to_dict("records"):
        key = {column: record[column] for column in ("ano", "mes", "produto", "uf")}
        for column in ("kg_liquido", "valor_fob_usd"):
            samples.append(
                {
                    "key": key,
                    "column": column,
                    "value": record[column],
                    "raw_cells": [],
                    "conversion": "linha sintética do guard",
                }
            )
    return frame, {"id": "sintetico", "tolerancia": {"absoluta": 0.0}, "samples": samples}


def _mock_transport(
    monkeypatch: pytest.MonkeyPatch,
    *,
    comexstat: dict[str, bytes] | None = None,
    abiove: dict[str, bytes] | None = None,
    comtrade: dict[str, bytes] | None = None,
) -> list[str]:
    seen: list[str] = []

    class Body(httpx.AsyncByteStream):
        def __init__(self, data: bytes) -> None:
            self._data = data

        async def __aiter__(self):
            yield self._data

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(str(request.url))
        host = request.url.host
        if host == COMEX_HOST:
            body = (comexstat or {}).get(request.url.path.rsplit("/", 1)[-1])
            if body is None:
                return httpx.Response(503, stream=Body(b""), headers={"content-type": "text/plain"})
            return httpx.Response(
                200,
                stream=Body(body),
                headers={"content-type": "text/csv", "content-length": str(len(body))},
            )
        if host == ABIOVE_HOST:
            body = (abiove or {}).get(request.url.path.rsplit("/", 1)[-1])
            if body is None:
                return httpx.Response(404, content=b"", headers={"content-type": "text/html"})
            return httpx.Response(
                200,
                content=body,
                headers={
                    "content-type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                },
            )
        if host == COMTRADE_HOST:
            params = parse_qs(request.url.query.decode())
            role = "count" if params.get("countOnly") else "data"
            body = (comtrade or {}).get(role)
            if body is None:
                return httpx.Response(
                    503, content=b"{}", headers={"content-type": "application/json"}
                )
            return httpx.Response(200, content=body, headers={"content-type": "application/json"})
        return httpx.Response(404, content=b"")

    class MockedClient(_REAL_ASYNC_CLIENT):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            kwargs["transport"] = httpx.MockTransport(handler)
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", MockedClient)
    return seen


async def test_comercio_reconciliacao_r8_casos_1():
    with collect_failures() as check:
        for case_id in [c for c in CASES if c.startswith("comexstat_exp_")]:
            case = f"test_exportacao_from_comexstat_bulk_slice[{(case_id,)!r}]"
            with check(case), isolated_dataset_case(case) as monkeypatch:
                case = CASES[case_id]
                seen = _mock_transport(
                    monkeypatch, comexstat={"EXP_2025.csv": _bytes(case["file"])}
                )
                frame, meta = await datasets.exportacao(case["product"], ano=2025, return_meta=True)
                assert any(httpx.URL(url).host == COMEX_HOST for url in seen)
                assert len(frame) == case["period"]["rows_out"]
                for column in ("kg_liquido", "valor_fob_usd"):
                    assert str(frame[column].dtype) == "float64", column
                _assert_samples(frame, case)
                assert meta.selected_source == "comexstat"
                assert meta.records_count == len(frame)
                assert list(frame.columns) == COLUNAS_EXPORTACAO
                assert "volume_ton" in frame.columns
                pd.testing.assert_series_equal(
                    frame["volume_ton"], frame["kg_liquido"] / 1000, check_names=False
                )
        for case_id in [c for c in CASES if c.startswith("abiove_")]:
            case = f"test_exportacao_falls_back_to_abiove[{(case_id,)!r}]"
            with check(case), isolated_dataset_case(case) as monkeypatch:
                case = CASES[case_id]
                seen = _mock_transport(
                    monkeypatch, abiove={"exp_202512.xlsx": _bytes(case["file"])}
                )
                with pytest.warns(UserWarning):
                    frame, meta = await datasets.exportacao(
                        case["product"], ano=2025, return_meta=True
                    )
                assert any(httpx.URL(url).host == COMEX_HOST for url in seen)
                assert any(url.endswith("exp_202512.xlsx") for url in seen)
                assert len(frame) == case["period"]["months"]
                assert frame["uf"].isna().all()
                assert set(frame["produto"]) == {case["product"]}
                _assert_samples(frame, case)
                assert meta.selected_source == "abiove"
                assert meta.attempted_sources == ["comexstat", "abiove"]
                assert list(frame.columns) == COLUNAS_EXPORTACAO
                assert "volume_ton" in frame.columns
                pd.testing.assert_series_equal(
                    frame["volume_ton"], frame["kg_liquido"] / 1000, check_names=False
                )


@pytest.mark.parametrize("case_id", [c for c in CASES if c.startswith("comexstat_imp_")])
async def test_importacao_from_comexstat_bulk_slice(case_id: str, monkeypatch: pytest.MonkeyPatch):
    case = CASES[case_id]
    seen = _mock_transport(monkeypatch, comexstat={"IMP_2025.csv": _bytes(case["file"])})
    frame, meta = await datasets.importacao(case["product"], ano=2025, return_meta=True)
    assert any(httpx.URL(url).host == COMEX_HOST for url in seen)
    assert len(frame) == case["period"]["rows_out"]
    for column in ("kg_liquido", "valor_fob_usd", "valor_frete_usd", "valor_seguro_usd"):
        assert str(frame[column].dtype) == "float64", column
    _assert_samples(frame, case)
    assert meta.selected_source == "comexstat"
    assert meta.fetch_timestamp == meta.fetched_at
    assert list(frame.columns) == [column.name for column in get_contract("importacao").columns]
    assert "volume_ton" in frame.columns
    pd.testing.assert_series_equal(
        frame["volume_ton"], frame["kg_liquido"] / 1000, check_names=False
    )


async def test_comercio_internacional_from_comtrade_preview(monkeypatch: pytest.MonkeyPatch):
    case = CASES["comtrade_soja_br_x_2023"]
    monkeypatch.delenv("AGROBR_COMTRADE_API_KEY", raising=False)
    seen = _mock_transport(
        monkeypatch,
        comtrade={"count": _bytes(case["files"]["count"]), "data": _bytes(case["files"]["data"])},
    )
    frame, meta = await datasets.comercio_internacional(
        "1201", periodo=2023, fluxo="X", parceiro="all", return_meta=True
    )
    assert any("countOnly=true" in url for url in seen)
    assert len(frame) == case["period"]["rows"]
    assert frame["codigo_parceiro"].nunique() == case["period"]["partners"]
    for column in ("peso_liquido_kg", "valor_fob_usd", "valor_primario_usd"):
        assert str(frame[column].dtype) == "float64", column
    _assert_samples(
        frame.rename(
            columns={
                "codigo_declarante": "reporter_code",
                "iso_declarante": "reporter_iso",
                "declarante": "reporter",
                "codigo_parceiro": "partner_code",
                "iso_parceiro": "partner_iso",
                "parceiro": "partner",
                "codigo_fluxo": "fluxo_code",
                "codigo_hs": "hs_code",
                "descricao_produto": "produto_desc",
            }
        ),
        case,
    )
    assert meta.selected_source == "comtrade_guest"
    assert meta.attempted_sources[-1] == "comtrade_guest"
    assert meta.fetch_timestamp == meta.fetched_at


@pytest.mark.parametrize(
    "mutacao", ["chave_ausente", "saida_ausente", "periodo_removido", "valor", "uf"]
)
def test_mutated_manifest_fails(mutacao: str):
    frame, case = _sintetico()
    _assert_samples(frame, case)
    if mutacao == "chave_ausente":
        frame = frame.drop(columns=["uf"])
    elif mutacao == "saida_ausente":
        frame = frame.drop(columns=["kg_liquido"])
    elif mutacao == "periodo_removido":
        frame = frame[frame["mes"] != 4]
    elif mutacao == "valor":
        case = copy.deepcopy(case)
        case["samples"][2]["value"] += 1
    else:
        case = copy.deepcopy(case)
        case["samples"][0]["key"]["uf"] = "PR"
    with pytest.raises(AssertionError):
        _assert_samples(frame, case)


async def test_exportacao_as_polars_tem_o_schema_do_contrato_nas_duas_fontes():
    pl = pytest.importorskip("polars")
    frames = []
    for prefixo, fonte, arquivo in (
        ("comexstat_exp_", "comexstat", "EXP_2025.csv"),
        ("abiove_", "abiove", "exp_202512.xlsx"),
    ):
        case_id = next(c for c in CASES if c.startswith(prefixo))
        case = CASES[case_id]
        with isolated_dataset_case(case_id) as isolado:
            _mock_transport(isolado, **{fonte: {arquivo: _bytes(case["file"])}})
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                frame = await datasets.exportacao(case["product"], ano=2025, as_polars=True)
        frames.append(frame)
    comexstat, abiove = frames
    assert comexstat.columns == abiove.columns == COLUNAS_EXPORTACAO
    assert comexstat["uf"].null_count() == 0
    assert abiove["uf"].null_count() == abiove.height > 0
    assert comexstat.schema == abiove.schema
    assert pl.concat(frames).height == comexstat.height + abiove.height


async def test_comercio_internacional_as_polars_texto_todo_nulo_sai_string(
    monkeypatch: pytest.MonkeyPatch,
):
    pl = pytest.importorskip("polars")
    case = CASES["comtrade_soja_br_x_2023"]
    monkeypatch.delenv("AGROBR_COMTRADE_API_KEY", raising=False)
    corpo = json.loads(_bytes(case["files"]["data"]))
    for registro in corpo["data"]:
        registro["cmdDesc"] = None
    _mock_transport(
        monkeypatch,
        comtrade={
            "count": _bytes(case["files"]["count"]),
            "data": json.dumps(corpo).encode(),
        },
    )
    frame = await datasets.comercio_internacional(
        "1201", periodo=2023, fluxo="X", parceiro="all", as_polars=True
    )
    assert frame.height == case["period"]["rows"]
    assert frame["descricao_produto"].null_count() == frame.height
    assert frame.schema["descricao_produto"] == pl.String
