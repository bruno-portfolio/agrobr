from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx
import pandas as pd
import pytest

from agrobr import contracts, datasets, deral, exceptions
from agrobr.deral import parser
from tests.helpers import conferir_corpo

GOLDEN = Path(__file__).parents[1] / "golden_data/reconciliacao_boletins_anec_anda_deral_20260918"
MANIFEST = json.loads((GOLDEN / "deral_manifest.json").read_text(encoding="utf-8"))
CASES = MANIFEST["cases"]
REAL_ASYNC_CLIENT = httpx.AsyncClient
SOURCE_URL = "https://www.agricultura.pr.gov.br/system/files/publico/Safras/PC.xls"


def _expected(case: dict[str, Any], *, current_only: bool = False) -> pd.DataFrame:
    records = []
    for sheet in case["sheets"]:
        if current_only and sheet["name"] not in {"Atual", "Anterior"}:
            continue
        for row in sheet["rows"]:
            for condicao in ("ruim", "media", "boa"):
                records.append(
                    {
                        **row["key"],
                        "condicao": condicao,
                        "pct": row["values"][condicao],
                        "plantio_pct": row["values"]["plantio_pct"],
                        "colheita_pct": row["values"]["colheita_pct"],
                    }
                )
    frame = pd.DataFrame(records)
    frame["data"] = pd.to_datetime(frame["data"], format="%d/%m/%Y")
    return frame.sort_values(["produto", "data", "condicao"]).reset_index(drop=True)


@pytest.fixture(params=CASES, ids=lambda case: case["id"])
def case(request: pytest.FixtureRequest) -> dict[str, Any]:
    return request.param


@pytest.fixture
def transport(monkeypatch: pytest.MonkeyPatch) -> Callable[[bytes], list[str]]:
    def install(body: bytes) -> list[str]:
        seen: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(str(request.url))
            assert str(request.url) == SOURCE_URL
            return httpx.Response(
                200, content=body, headers={"content-type": "application/vnd.ms-excel"}
            )

        monkeypatch.setattr(
            httpx,
            "AsyncClient",
            lambda **kwargs: REAL_ASYNC_CLIENT(**kwargs, transport=httpx.MockTransport(handler)),
        )
        return seen

    return install


async def _fetch(target: str, body: bytes, **kwargs: Any) -> pd.DataFrame:
    if target == "parser":
        return parser.parse_pc_xls(body)
    api = deral.condicao_lavouras if target == "source" else datasets.condicao_lavouras
    frame, meta = await api(return_meta=True, **kwargs)
    conferir_corpo(meta, body)
    assert meta.records_count == len(frame)
    assert meta.selected_source == "deral"
    assert meta.attempted_sources == ["deral"]
    assert meta.schema_version == contracts.get_contract("condicao_lavouras").version == "2.0"
    if target == "source":
        assert meta.source_method == "httpx+xlrd"
    return frame


@pytest.mark.parametrize("target", ["parser", "source", "dataset"])
async def test_planilha_original_todas_abas_e_celulas(case, target, transport):
    body = (GOLDEN / case["file"]).read_bytes()
    seen = transport(body)
    frame = await _fetch(target, body)
    pd.testing.assert_frame_equal(frame, _expected(case), check_dtype=False, check_exact=True)
    assert len(frame) == case["output_rows"]
    assert not frame.duplicated(["produto", "data", "condicao"]).any()
    assert seen == ([] if target == "parser" else [SOURCE_URL])


@pytest.mark.parametrize("target", ["source", "dataset"])
@pytest.mark.parametrize("product", ["feijao_1", "feijao_2", "milho_1", "milho_2", "soja"])
async def test_seletor_publico_preserva_primeira_segunda_safra(case, target, product, transport):
    body = (GOLDEN / case["file"]).read_bytes()
    transport(body)
    frame = await _fetch(target, body, produto=product)
    expected = _expected(case)
    expected = expected[expected["produto"] == product].reset_index(drop=True)
    pd.testing.assert_frame_equal(frame, expected, check_dtype=False, check_exact=True)


@pytest.mark.parametrize("target", ["parser", "source", "dataset"])
@pytest.mark.parametrize("header", ["boa", "media", "ruim", "plantada", "colhida"])
async def test_cabecalho_obrigatorio_ausente_recusa_sucesso_parcial(target, header, transport):
    body = (GOLDEN / f"deral/mutations/missing_{header}.xls").read_bytes()
    transport(body)
    with pytest.raises(exceptions.ParseError) as caught:
        await _fetch(target, body)
    if target == "dataset":
        assert caught.value.errors and all(kind == "parse" for _, kind, _ in caught.value.errors)
        assert isinstance(caught.value.__cause__, exceptions.ParseError)
