from __future__ import annotations

from itertools import count
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from agrobr.cepea import client
from agrobr.health import checker
from agrobr.health.checker import CheckStatus
from agrobr.noticias_agricolas import parser as na_parser

RAIZ = Path(__file__).resolve().parents[2]
PAGINA = RAIZ / "tests" / "golden_data" / "cepea" / "cache_ttl_20260923" / "soja_20260923.html"
PAGINA_NA = RAIZ / "tests" / "golden_data" / "reconciliacao_r6_20260918" / "na_soja.html"
MOTIVO_NA = "Fingerprint não comparado: a página veio de noticias_agricolas, e não do CEPEA"


def _servir(monkeypatch: pytest.MonkeyPatch, fonte: str = "cepea", pagina: Path = PAGINA) -> None:
    resultado = client.FetchResult(pagina.read_bytes().decode("utf-8"), fonte)
    monkeypatch.setattr(client, "fetch_indicador_page", AsyncMock(return_value=resultado))


@pytest.mark.parametrize("pasta", ["raiz", "outra"])
async def test_deep_compara_a_baseline_do_pacote_de_qualquer_pasta(monkeypatch, tmp_path, pasta):
    _servir(monkeypatch)
    monkeypatch.chdir(RAIZ if pasta == "raiz" else tmp_path)

    resultado = await checker.check_cepea_deep()

    assert (resultado.status, resultado.message) == (CheckStatus.OK, "All checks passed")
    assert resultado.details.get("fingerprint_similarity") == 1.0
    assert resultado.details.get("records_parsed") == 15


async def test_deep_compara_a_pagina_do_cepea_vinda_do_navegador(monkeypatch):
    _servir(monkeypatch, "browser")

    resultado = await checker.check_cepea_deep()

    assert resultado.status == CheckStatus.OK
    assert resultado.details.get("fingerprint_similarity") == 1.0


async def test_deep_sem_baseline_avisa_o_motivo(monkeypatch, tmp_path):
    _servir(monkeypatch)
    monkeypatch.setattr(checker, "CEPEA_BASELINES", tmp_path)

    resultado = await checker.check_cepea_deep()

    assert resultado.status == CheckStatus.WARNING
    assert resultado.message == f"Fingerprint não comparado: baseline indisponível em {tmp_path}"
    assert "fingerprint_similarity" not in resultado.details
    assert resultado.details.get("records_parsed") == 15


async def test_deep_com_a_pagina_real_da_na_parseia_pela_na(monkeypatch):
    _servir(monkeypatch, "noticias_agricolas", PAGINA_NA)

    resultado = await checker.check_cepea_deep()

    assert (resultado.status, resultado.message) == (CheckStatus.WARNING, MOTIVO_NA)
    assert "fingerprint_similarity" not in resultado.details
    assert resultado.details.get("parser_version") == na_parser.PARSER_VERSION
    assert resultado.details.get("records_parsed") == 10


async def test_deep_com_a_na_lenta_diz_o_motivo_real(monkeypatch):
    _servir(monkeypatch, "noticias_agricolas", PAGINA_NA)
    relogio = count(0, 6)
    monkeypatch.setattr(checker, "time", SimpleNamespace(monotonic=lambda: next(relogio)))

    resultado = await checker.check_cepea_deep()

    assert (resultado.status, resultado.message) == (CheckStatus.WARNING, MOTIVO_NA)
    assert resultado.details.get("latency_ms") == 6000
