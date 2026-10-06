from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pandas as pd
import pytest

from agrobr import datasets, ibge
from agrobr.exceptions import SourceUnavailableError
from tests import helpers

GOLDEN = (
    Path(__file__).resolve().parents[1]
    / "golden_data/reconciliacao_censos_producao_ibge_conab_20260918"
)
MANIFEST = json.loads((GOLDEN / "lot1_manifest.json").read_text(encoding="utf-8"))
CONAB_MANIFEST = json.loads((GOLDEN / "lot1_conab_manifest.json").read_text(encoding="utf-8"))


@pytest.mark.parametrize("case", MANIFEST["cases"], ids=lambda case: case["id"])
async def test_sidra_valores_preservados_na_api_e_dataset(monkeypatch, case):
    started = datetime.now(UTC)
    calls = helpers.install_reconciliacao_r5_http(monkeypatch, case, MANIFEST)
    source, source_meta = await getattr(ibge, case["source_api"])(
        **case["selection"], return_meta=True
    )
    frame, meta = await getattr(datasets, case["dataset"])(**case["selection"], return_meta=True)
    source_case = {**case, "null_columns": []}
    helpers.assert_reconciliation_case(source, source_case)
    helpers.assert_reconciliation_case(frame, case)
    if case["dataset"] in {"extrativismo_vegetal", "silvicultura"}:
        assert str(frame["valor"].dtype) == "float64"
    assert len(calls) == 2
    assert meta.selected_source == source_meta.selected_source
    assert meta.attempted_sources == source_meta.attempted_sources
    assert meta.records_count == source_meta.records_count == len(frame)
    assert meta.fetch_timestamp is not None
    assert started <= meta.fetch_timestamp <= datetime.now(UTC)
    assert meta.schema_version == ("2.2" if case["dataset"] == "producao_anual" else "1.1")
    pd.testing.assert_frame_equal(source, frame[source.columns])


@pytest.mark.parametrize(
    "product,level,reason",
    [
        ("soja", "municipio", "CONAB Safras nao oferece granularidade municipal"),
        ("cana", "uf", "CONAB Safras nao oferece o produto 'cana'"),
    ],
    ids=["soja-municipio", "cana-uf"],
)
async def test_fallback_conab_nao_inventa_cobertura_de_produto_ou_municipio(
    monkeypatch, product, level, reason
):
    calls = helpers.install_reconciliacao_r5_conab_http(
        monkeypatch, CONAB_MANIFEST["cases"][0]["r3_case_id"]
    )
    original_send = httpx.AsyncClient.send

    async def send(client, request, **kwargs):
        if request.url.host == "servicodados.ibge.gov.br":
            calls.append(str(request.url))
            return httpx.Response(403, text="Forbidden", request=request)
        return await original_send(client, request, **kwargs)

    monkeypatch.setattr(httpx.AsyncClient, "send", send)
    with pytest.raises(SourceUnavailableError) as error:
        await datasets.producao_anual(product, ano=2026, nivel=level)
    assert error.value.errors[0][:2] == ("ibge_pam", "unavailable")
    assert error.value.errors[-1] == ("conab", "unavailable", f"conab unavailable: {reason}")
    assert "apisidra.ibge.gov.br" in calls[0]
    assert any("servicodados.ibge.gov.br" in url for url in calls)
    assert not any("conab" in url for url in calls)


@pytest.mark.parametrize("nome", ["lot1_conab_manifest.json", "lot1_conab_legacy_manifest.json"])
def test_manifesto_conab_fixa_o_sha_atual_do_manifesto_r3(nome):
    fixado = json.loads((GOLDEN / nome).read_text(encoding="utf-8"))["r3_manifest_sha256"]
    r3 = GOLDEN.parent / "reconciliacao_conab_20260918/manifest.json"
    atual = hashlib.sha256(r3.read_bytes()).hexdigest()
    assert fixado == atual
