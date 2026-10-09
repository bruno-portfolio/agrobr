from __future__ import annotations

import json
import re
from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx
import pandas as pd
import pytest

from agrobr import anda, contracts, datasets, exceptions
from agrobr.anda import client, parser
from tests.helpers import conferir_corpo, sem_excecao

GOLDEN = Path(__file__).parents[1] / "golden_data/reconciliacao_boletins_anec_anda_deral_20260918"
MANIFEST = json.loads((GOLDEN / "anda_manifest.json").read_text(encoding="utf-8"))
CASES = MANIFEST["cases"]
REAL_ASYNC_CLIENT = httpx.AsyncClient


@pytest.fixture(params=CASES, ids=lambda case: case["id"])
def case(request: pytest.FixtureRequest) -> dict[str, Any]:
    return request.param


@pytest.fixture
def transport(
    monkeypatch: pytest.MonkeyPatch,
) -> Callable[[dict[str, Any], bytes], list[str]]:
    def install(case: dict[str, Any], body: bytes) -> list[str]:
        seen: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            url = str(request.url)
            seen.append(url)
            if url == client.ESTATISTICAS_URL:
                return httpx.Response(
                    200,
                    content=(GOLDEN / "anda/anda_catalog.html").read_bytes(),
                    headers={"content-type": "text/html; charset=utf-8"},
                )
            assert url == case["url"], url
            return httpx.Response(200, content=body, headers={"content-type": "application/pdf"})

        monkeypatch.setattr(
            httpx,
            "AsyncClient",
            lambda **kwargs: REAL_ASYNC_CLIENT(**kwargs, transport=httpx.MockTransport(handler)),
        )
        return seen

    return install


@pytest.mark.parametrize("target", ["source", "dataset"])
async def test_api_publica_pdf_transporte_real(case, target, transport):
    seen = transport(case, (GOLDEN / case["file"]).read_bytes())
    if target == "source":
        frame, meta = await anda.entregas(case["year"], return_meta=True)
    else:
        frame, meta = await datasets.fertilizante(ano=case["year"], return_meta=True)
    pd.testing.assert_frame_equal(
        frame, pd.DataFrame(case["observations"]), check_dtype=False, check_exact=True
    )
    pd.testing.assert_frame_equal(
        frame.iloc[:0], contracts.get_contract("fertilizante").empty_frame()
    )
    assert meta.selected_source == "anda"
    assert meta.attempted_sources == ["anda"]
    assert meta.records_count == len(frame)
    assert meta.schema_version == contracts.get_contract("fertilizante").version == "2.0"
    assert seen == [client.ESTATISTICAS_URL, case["url"]]
    corpo = (GOLDEN / case["file"]).read_bytes()
    recibo = next(item for item in MANIFEST["files"] if item["file"] == case["file"])
    assert meta.source_url == case["url"]
    assert meta.raw_content_hash == recibo["sha256"]
    conferir_corpo(meta, corpo)
    edicao = re.match(r"(\D+?)\s+\d", case["published_totals"][0]["row_text"]).group(1)
    assert meta.source_details["pdf"] == {
        "url": case["url"],
        "rotulo_catalogo": recibo["catalog_label"],
        "edicao_impressa": edicao,
        "sha256": recibo["sha256"],
        "bytes": len(corpo),
        "pagina_de_recursos": client.ESTATISTICAS_URL,
    }


async def test_agregacao_mensal_confere_o_pdf_oficial(case, transport):
    transport(case, (GOLDEN / case["file"]).read_bytes())
    with sem_excecao():
        frame, meta = await anda.entregas(case["year"], agregacao="mensal", return_meta=True)
    esperado = (
        pd.DataFrame(case["observations"])
        .groupby(["ano", "mes", "produto_fertilizante"], as_index=False)["volume_ton"]
        .sum()
    )
    pd.testing.assert_frame_equal(frame, esperado, check_dtype=False, check_exact=True)
    assert "uf" not in frame.columns
    assert meta.schema_version != contracts.get_contract("fertilizante").version


@pytest.mark.parametrize("mutation", ["year", "section"])
@pytest.mark.parametrize("target", ["parser", "source", "dataset"])
async def test_pdf_metrica_ou_periodo_ausente_recusado(mutation, target, transport):
    selected = next(c for c in CASES if c["year"] == 2026)
    body = (GOLDEN / f"anda/anda_2026_{mutation}.pdf").read_bytes()
    transport(selected, body)
    with pytest.raises(exceptions.ParseError) as caught:
        if target == "parser":
            parser.parse_entregas_pdf(body, ano=2026)
        elif target == "source":
            await anda.entregas(2026)
        else:
            await datasets.fertilizante(ano=2026)
    if target == "dataset":
        assert caught.value.errors and all(kind == "parse" for _, kind, _ in caught.value.errors)
        assert isinstance(caught.value.__cause__, exceptions.ParseError)
