from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from agrobr import datasets
from agrobr.conab import client

FIXTURE = Path(__file__).parents[1] / "golden_data/conab/selecao_20260906"


@pytest.fixture
def captured_http(monkeypatch: pytest.MonkeyPatch) -> tuple[dict[str, Any], list[str]]:
    manifest = json.loads((FIXTURE / "manifest.json").read_text(encoding="utf-8"))
    responses = {}
    for resource in manifest["files"]:
        content = (FIXTURE / resource["file"]).read_bytes()
        assert hashlib.sha256(content).hexdigest() == resource["sha256"]
        responses[resource["url"]] = content
    requests: list[str] = []

    async def fetch(url: str) -> bytes:
        requests.append(url)
        return responses[url]

    monkeypatch.setattr(client, "_fetch_http", fetch)
    return manifest, requests


@pytest.mark.asyncio
@pytest.mark.parametrize("index", [0, 1], ids=["xlsx_sem_extensao", "xls_pagina_dois"])
async def test_estimativa_seleciona_captura_oficial(
    captured_http: tuple[dict[str, Any], list[str]], index: int
):
    manifest, requests = captured_http
    oracle = manifest["oracles"][index]

    frame, meta = await datasets.estimativa_safra(
        "soja",
        safra=oracle["safra"],
        levantamento=oracle["levantamento"],
        uf="mt",
        return_meta=True,
    )

    assert requests == [*manifest["catalog_urls"], oracle["url"]]
    assert len(frame) == 1
    row = frame.iloc[0]
    assert row["produto"] == "soja"
    assert row["uf"] == "MT"
    assert row["safra"] == oracle["safra"]
    assert row["levantamento"] == oracle["levantamento"]
    assert row["data_publicacao"] == pd.Timestamp(oracle["publication_date"])
    for field, expected in oracle["selected_values"].items():
        assert row[field] == expected["value_raw"]
    assert row["fonte"] == "conab"
    assert {"unidade_producao", "unidade_area"} <= set(frame.columns)
    assert row["unidade_producao"] == "mil_ton"
    assert row["unidade_area"] == "mil_ha"
    assert frame[["ano_lspa", "mes_lspa"]].isna().all().all()
    assert str(frame["ano_lspa"].dtype) == "Int64"
    assert str(frame["mes_lspa"].dtype) == "Int64"
    assert meta.source == "datasets.estimativa_safra/conab"
    assert meta.source_url == oracle["url"]
    assert meta.dataset == "estimativa_safra"
    assert meta.schema_version == "3.1"
    assert meta.contract_version == "3.1"
    assert meta.validation_passed is True
    assert meta.attempted_sources == ["conab"]
    assert meta.selected_source == "conab"
    assert meta.from_cache is False
