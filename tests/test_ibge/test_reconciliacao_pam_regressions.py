from __future__ import annotations

import json
from pathlib import Path

import httpx
import pandas as pd
import pytest

from agrobr import ibge
from agrobr.exceptions import ParseError
from agrobr.ibge import pam_parser
from tests import helpers

GOLDEN = (
    Path(__file__).resolve().parents[1]
    / "golden_data/reconciliacao_censos_producao_ibge_conab_20260918"
)
MANIFEST = json.loads((GOLDEN / "lot1_manifest.json").read_text(encoding="utf-8"))
CASE = MANIFEST["cases"][0]


async def test_pam_celulas_duplicadas_nao_escolhem_primeira_medida(monkeypatch):
    helpers.install_reconciliacao_r5_http(monkeypatch, CASE, MANIFEST)
    original = httpx.AsyncClient.send

    async def send(client, request, **kwargs):
        response = await original(client, request, **kwargs)
        records = response.json()
        records.append({**records[0], "V": "1"})
        return httpx.Response(200, json=records, request=request)

    monkeypatch.setattr(httpx.AsyncClient, "send", send)
    with pytest.raises(ParseError, match="duplicad|amb.g"):
        await ibge.pam(**CASE["selection"])


def test_pam_coffee_condition_changes_in_2002():
    result = pam_parser.add_unit_columns(pd.DataFrame({"ano": [2001, 2002]}), "cafe")
    assert result.condicao_produto.tolist() == ["em_coco", "beneficiado"]
