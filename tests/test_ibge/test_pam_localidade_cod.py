from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import unquote

import pandas as pd
import pytest

from agrobr import datasets, ibge
from agrobr.ibge._helpers import SIDRA_BASE, registrar_canal
from agrobr.models import MetaInfo
from tests.helpers import assert_replay_served, conferir_corpo, install_replay_http, sem_excecao

GOLDEN = Path(__file__).resolve().parents[1] / "golden_data/ibge/pam_localidade_cod_20260925"
MANIFESTO = json.loads((GOLDEN / "manifest.json").read_text(encoding="utf-8"))
SIDRA = (
    "https://apisidra.ibge.gov.br/values/t/5457/n6/in N3 {uf}/h/n/p/2024/v/{variaveis}/c782/40124"
)
CODIGO_UF = {"DF": "53", "RR": "14"}
QUATRO = "8331,216,214,112"
CINCO = "8331,216,214,112,215"


def _servir(monkeypatch, tmp_path, uf: str, variaveis: str) -> dict:
    registros = json.loads((GOLDEN / f"{uf}.json").read_text(encoding="utf-8"))[1:]
    if variaveis == QUATRO:
        registros = [registro for registro in registros if registro["D2C"] != "215"]
    no_formato_do_cliente = [
        {
            **registro,
            "D2C": registro["D3C"],
            "D2N": registro["D3N"],
            "D3C": registro["D2C"],
            "D3N": registro["D2N"],
        }
        for registro in registros
    ]
    (tmp_path / f"{uf}.json").write_text(json.dumps(no_formato_do_cliente), encoding="utf-8")
    pedido = {
        "match": {
            "path": SIDRA.format(uf=CODIGO_UF[uf], variaveis=variaveis),
            "params": {},
            "skip": 0,
        },
        "file": f"{uf}.json",
        "content_type": "application/json",
    }
    return install_replay_http(monkeypatch, {"requests": [pedido]}, tmp_path)


def _esperado(uf: str) -> dict[str, int]:
    return {nome: int(codigo) for nome, codigo in MANIFESTO["corpos"][uf]["municipios"].items()}


@pytest.mark.parametrize("uf", ["DF", "RR"])
async def test_pam_municipal_traz_o_codigo_ibge_publicado(monkeypatch, tmp_path, uf):
    visto = _servir(monkeypatch, tmp_path, uf, CINCO)
    variaveis = ["area_plantada", "area_colhida", "producao", "rendimento", "valor_producao"]
    with sem_excecao():
        frame, meta = await ibge.pam(
            "soja", ano=2024, uf=uf, nivel="municipio", variaveis=variaveis, return_meta=True
        )
    assert_replay_served(visto)
    assert "localidade_cod" in frame.columns
    assert str(frame["localidade_cod"].dtype) == "Int64"
    assert dict(zip(frame["localidade"], frame["localidade_cod"], strict=True)) == _esperado(uf)
    assert meta.schema_version == "2.1"
    conferir_corpo(meta, (tmp_path / f"{uf}.json").read_bytes())
    assert unquote(meta.source_url) == SIDRA.format(uf=CODIGO_UF[uf], variaveis=CINCO)
    assert meta.source_details["pagina"] == SIDRA_BASE
    assert [c["url"] for c in meta.source_details["consultas"]] == [meta.source_url]


def test_varias_consultas_deixam_o_topo_na_pagina_e_cada_corpo_no_detalhe():
    meta = MetaInfo(
        source="ibge_lspa",
        source_url=SIDRA_BASE,
        source_method="httpx",
        fetched_at=datetime(2026, 9, 26, tzinfo=UTC),
    )
    consultas = []
    for numero in (1, 2):
        frame = pd.DataFrame()
        frame.attrs.update(
            canal="sidra",
            url=f"https://apisidra.ibge.gov.br/values/t/{numero}",
            sha256=str(numero) * 64,
            bytes=numero * 100,
        )
        registrar_canal(meta, frame)
        consultas.append((meta.source_url, meta.raw_content_hash, meta.raw_content_size))

    assert consultas == [
        ("https://apisidra.ibge.gov.br/values/t/1", "1" * 64, 100),
        (SIDRA_BASE, None, 0),
    ]
    assert [(c["sha256"], c["bytes"]) for c in meta.source_details["consultas"]] == [
        ("1" * 64, 100),
        ("2" * 64, 200),
    ]
    assert meta.source_details["pagina"] == SIDRA_BASE
    assert meta.fetch_timestamp is not None and meta.fetch_timestamp.tzinfo is UTC


async def test_producao_anual_municipal_traz_o_codigo_ibge(monkeypatch, tmp_path):
    visto = _servir(monkeypatch, tmp_path, "RR", QUATRO)
    with sem_excecao():
        frame, meta = await datasets.producao_anual(
            "soja", ano=2024, nivel="municipio", uf="RR", return_meta=True
        )
    assert_replay_served(visto)
    assert "localidade_cod" in frame.columns
    assert dict(zip(frame["localidade"], frame["localidade_cod"], strict=True)) == _esperado("RR")
    assert meta.contract_version == "2.2"
