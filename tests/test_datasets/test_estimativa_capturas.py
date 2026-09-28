from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import httpx
import pandas as pd
import pytest

from agrobr import contracts, datasets
from agrobr.ibge import client as ibge_client
from tests.helpers import sem_excecao

CAPTURE_DIR = Path(__file__).parent.parent / "golden_data" / "ibge" / "lspa_selecao_20260906"
CASES = [
    ("soja", "MT", 1, 45586.022, "soja_mt_2025_01"),
    ("soja", "MT", 12, 50175.032, "soja_mt_2025_12"),
    ("milho", None, 1, 124132.653, "milho_br_2025_01"),
    ("milho", None, 12, 141734.445, "milho_br_2025_12"),
]
AREA_TOTALS_HA = {
    "soja_mt_2025_01": (12664905, 12664905),
    "soja_mt_2025_12": (12788611, 12788611),
    "milho_br_2025_01": (17212909 + 4659475, 17180302 + 4610245),
    "milho_br_2025_12": (4651323 + 17895899, 4407427 + 17863886),
}


@pytest.fixture(autouse=True)
def sem_periodos_ibge(monkeypatch: pytest.MonkeyPatch) -> None:
    """As capturas deste módulo não trazem o `/periodos`."""

    async def sem_metadado(_table_code: str, _df: object) -> dict[str, object]:
        return {}

    monkeypatch.setattr(ibge_client, "_periodos_modificacao", sem_metadado)


@pytest.fixture(scope="module")
def capture_manifest() -> list[dict[str, Any]]:
    return json.loads((CAPTURE_DIR / "manifest.json").read_text(encoding="utf-8"))["requests"]


@pytest.fixture
def captured_http(monkeypatch, capture_manifest: list[dict[str, Any]]) -> list[str]:
    responses = {item["requested_url"]: item for item in capture_manifest}
    requested: list[str] = []
    original_client = httpx.AsyncClient

    def respond(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        url = str(request.url)
        assert url in responses, f"Consulta não coberta pelas capturas oficiais: {url}"
        requested.append(url)
        item = responses[url]
        return httpx.Response(
            item["status"],
            headers={"content-type": item["content_type"]},
            content=(CAPTURE_DIR / item["body_file"]).read_bytes(),
            request=request,
        )

    def client_with_captured_transport(*args: Any, **kwargs: Any) -> httpx.AsyncClient:
        return original_client(*args, transport=httpx.MockTransport(respond), **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", client_with_captured_transport)
    return requested


@pytest.mark.parametrize("produto,uf,mes,producao,case", CASES)
async def test_official_capture_through_public_dataset(
    produto: str,
    uf: str | None,
    mes: int,
    producao: float,
    case: str,
    captured_http: list[str],
    capture_manifest: list[dict[str, Any]],
):
    with sem_excecao():
        frame, meta = await datasets.estimativa_safra(
            produto,
            safra="2024/25",
            uf=uf,
            fonte="ibge_lspa",
            mes=mes,
            return_meta=True,
        )

    assert len(frame) == 1
    row = frame.iloc[0]
    assert row["producao"] == pytest.approx(producao, rel=0, abs=1e-8)
    area_plantada_ha, area_colhida_ha = AREA_TOTALS_HA[case]
    assert row["area_plantada"] == pytest.approx(area_plantada_ha / 1000, rel=0, abs=1e-8)
    assert row["area_colhida"] == pytest.approx(area_colhida_ha / 1000, rel=0, abs=1e-8)
    assert row["produtividade"] == pytest.approx(
        producao * 1_000_000 / area_colhida_ha, rel=0, abs=1e-8
    )
    assert row["produto"] == produto
    assert row["safra"] == "2024/25"
    assert row["ano_lspa"] == 2025
    assert row["mes_lspa"] == mes
    if uf is None:
        assert pd.isna(row["uf"])
    else:
        assert row["uf"] == uf
    assert row["fonte"] == "ibge_lspa"
    assert {"unidade_producao", "unidade_area"} <= set(frame.columns)
    assert row["unidade_producao"] == "mil_ton"
    assert row["unidade_area"] == "mil_ha"
    assert pd.isna(row["levantamento"])
    assert meta.source == "datasets.estimativa_safra/ibge_lspa"
    assert meta.selected_source == "ibge_lspa"
    assert meta.attempted_sources == ["ibge_lspa"]
    assert meta.dataset == "estimativa_safra"
    assert meta.schema_version == "3.1"
    assert meta.contract_version == "3.1"
    assert meta.records_count == len(frame)
    assert meta.columns == frame.columns.tolist()
    expected_urls = {item["requested_url"] for item in capture_manifest if item["case"] == case}
    assert set(captured_http) == expected_urls
    assert len(captured_http) == len(expected_urls)
    contracts.validate_dataset(frame, "estimativa_safra")
