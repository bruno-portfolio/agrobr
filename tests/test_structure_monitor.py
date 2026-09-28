from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from agrobr.cepea import client
from agrobr.health import checker
from scripts import compare_structures, fetch_structures
from tests.helpers import sem_excecao

GOLDEN = Path(__file__).parent / "golden_data"
BASELINE = checker.CEPEA_BASELINES / "cepea_baseline.json"


async def _monitorar(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, pagina: Path, fonte: str
) -> tuple[bool, dict]:
    """O que o workflow faz: coleta a estrutura da página servida e compara com a baseline do pacote."""
    resultado = client.FetchResult(pagina.read_bytes().decode("utf-8"), fonte)
    monkeypatch.setattr(client, "fetch_indicador_page", AsyncMock(return_value=resultado))
    monkeypatch.chdir(tmp_path)
    with sem_excecao():
        await fetch_structures.fetch_all_structures(str(tmp_path / "current.json"))
        drift = compare_structures.compare(
            str(BASELINE), str(tmp_path / "current.json"), 0.85, str(tmp_path / "diff.json")
        )
    return drift, json.loads((tmp_path / "diff.json").read_text(encoding="utf-8"))


async def test_monitor_com_a_pagina_da_na_nao_acusa_drift(monkeypatch, tmp_path):
    drift, relatorio = await _monitorar(
        monkeypatch,
        tmp_path,
        GOLDEN / "reconciliacao_r6_20260918" / "na_soja.html",
        "noticias_agricolas",
    )

    assert drift is False
    assert relatorio.get("comparisons") == [
        {
            "source": "cepea",
            "status": "skipped",
            "reason": "a página veio da Notícias Agrícolas, e não do CEPEA",
        }
    ]
    assert not (tmp_path / "drift_detected.flag").exists()


async def test_monitor_compara_a_pagina_do_cepea_com_a_baseline_do_pacote(monkeypatch, tmp_path):
    drift, relatorio = await _monitorar(
        monkeypatch,
        tmp_path,
        GOLDEN / "cepea" / "cache_ttl_20260923" / "soja_20260923.html",
        "cepea",
    )

    assert drift is False
    assert [(item.get("status"), item.get("similarity")) for item in relatorio["comparisons"]] == [
        ("ok", 1.0)
    ]


async def test_monitor_acusa_o_layout_antigo(monkeypatch, tmp_path):
    drift, relatorio = await _monitorar(
        monkeypatch, tmp_path, GOLDEN / "cepea" / "soja_sample" / "response.html", "cepea"
    )

    assert drift is True
    assert [item.get("status") for item in relatorio["comparisons"]] == ["drift"]
    assert (tmp_path / "drift_detected.flag").exists()
