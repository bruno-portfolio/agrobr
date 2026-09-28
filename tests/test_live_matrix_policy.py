from __future__ import annotations

import json
from pathlib import Path

import pytest
import requests

from agrobr import datasets
from agrobr.antaq import client as antaq_client
from agrobr.exceptions import SourceUnavailableError
from tests.integration import live_policy

pytest_plugins = ["pytester"]
DATASET_COUNT = len(datasets.list_datasets())


@pytest.mark.parametrize("year,known", [(2024, True), (2023, False)])
async def test_redirect_oficial_antaq_reconhecido_pela_politica(monkeypatch, year, known):
    response = requests.Response()
    response.status_code = 200
    response._content = b"<html>Indisponivel</html>"
    response.url = (
        "https://www.gov.br/antaq/pt-br/central-de-conteudos/publicacoes-da-antaq/"
        "publicacoes-off/painel-estatistico-aquaviario-indisponivel"
    )
    monkeypatch.setattr(antaq_client.requests, "get", lambda *_args, **_kwargs: response)
    with pytest.raises(antaq_client.OfficialOutageError) as caught:
        await antaq_client.fetch_ano_zip(year)
    assert live_policy.known_antaq_2024_outage(caught.value, year) is known
    assert caught.value.notice_url == response.url


@pytest.mark.parametrize(
    ("dataset", "argument", "environment", "missing"),
    [
        ("oferta_demanda_global", None, None, True),
        ("oferta_demanda_global", "", "", True),
        ("oferta_demanda_global", "synthetic", None, False),
        ("oferta_demanda_global", None, "synthetic", False),
        ("oferta_demanda_global", "", "synthetic", False),
        ("producao_anual", None, None, False),
    ],
)
def test_usda_skip_requires_missing_key(monkeypatch, dataset, argument, environment, missing):
    monkeypatch.delenv("AGROBR_USDA_API_KEY", raising=False)
    if environment is not None:
        monkeypatch.setenv("AGROBR_USDA_API_KEY", environment)
    assert live_policy.missing_usda_key(dataset, {"api_key": argument}) is missing


@pytest.mark.parametrize(
    ("source", "url", "year", "cause", "known"),
    [
        ("antaq", live_policy.ANTAQ_2024_URL, 2024, "522", True),
        ("antaq", live_policy.ANTAQ_2024_URL, 2023, "522", False),
        ("antaq", live_policy.ANTAQ_2024_URL.replace("2024", "2023"), 2024, "522", False),
        ("antaq", live_policy.ANTAQ_2024_URL.replace("2024", "Mercadoria"), 2024, "522", False),
        ("antaq", live_policy.ANTAQ_2024_URL + "?other=1", 2024, "522", False),
        ("other", live_policy.ANTAQ_2024_URL, 2024, "522", False),
        ("antaq", live_policy.ANTAQ_2024_URL, 2024, "503", False),
        ("antaq", live_policy.ANTAQ_2024_URL, 2024, "5220", False),
        ("antaq", live_policy.ANTAQ_2024_URL, 2024, "text_only", False),
        ("antaq", live_policy.ANTAQ_2024_URL, 2024, "wrong_type", False),
        ("antaq", live_policy.ANTAQ_2024_URL, 2024, "other_response_url", False),
    ],
)
def test_antaq_skip_requires_exact_request_and_typed_status(source, url, year, cause, known):
    error = SourceUnavailableError(source, url=url, last_error="Retriable status: 522")
    if cause == "wrong_type":
        error.__cause__ = requests.exceptions.ConnectionError("Retriable status: 522")
    elif cause != "text_only":
        response = requests.Response()
        response.status_code = 522 if cause == "other_response_url" else int(cause)
        response.url = "https://example.invalid/outage" if cause == "other_response_url" else url
        error.__cause__ = requests.exceptions.HTTPError(response=response)
    assert live_policy.known_antaq_2024_outage(error, year) is known


@pytest.mark.slow
@pytest.mark.parametrize(
    (
        "scenario",
        "selection",
        "exit_code",
        "selected",
        "validated",
        "known",
        "not_verified",
        "failed",
        "errors",
    ),
    [
        ("all_outages", "", 1, DATASET_COUNT, 0, 0, 1, DATASET_COUNT - 1, 0),
        ("known_only", "oferta_demanda_global or movimentacao_portuaria", 1, 2, 0, 1, 1, 0, 0),
        (
            "mixed",
            "producao_anual or oferta_demanda_global or movimentacao_portuaria",
            0,
            3,
            1,
            1,
            1,
            0,
            0,
        ),
        ("unexpected", "producao_anual", 1, 1, 0, 0, 0, 1, 0),
        ("usda_with_key", "oferta_demanda_global", 1, 1, 0, 0, 0, 1, 0),
        ("antaq_other_url", "movimentacao_portuaria", 1, 1, 0, 0, 0, 1, 0),
        ("antaq_503", "movimentacao_portuaria", 1, 1, 0, 0, 0, 1, 0),
        ("antaq_parse", "movimentacao_portuaria", 1, 1, 0, 0, 0, 1, 0),
        ("antaq_aggregated_text", "movimentacao_portuaria", 1, 1, 0, 0, 0, 1, 0),
        ("fixture_error", "producao_anual", 1, 1, 0, 0, 0, 0, 1),
        ("fixture_skip", "producao_anual", 1, 1, 0, 0, 0, 0, 0),
        ("bad_metadata", "producao_anual", 1, 1, 0, 0, 0, 1, 0),
        ("teardown_error", "producao_anual", 1, 1, 0, 0, 0, 0, 1),
        ("known_only", "oferta_demanda_global or test_unrelated", 1, 1, 0, 0, 1, 0, 0),
        ("offline", "test_live_cases_cover_registry", 0, 0, 0, 0, 0, 0, 0),
    ],
)
def test_live_matrix_pytest_exit_and_coverage(
    pytester,
    monkeypatch,
    scenario,
    selection,
    exit_code,
    selected,
    validated,
    known,
    not_verified,
    failed,
    errors,
):
    root = Path(__file__).resolve().parents[1]
    monkeypatch.setenv("PYTHONPATH", str(root))
    monkeypatch.setenv("PYTHONDONTWRITEBYTECODE", "1")
    monkeypatch.setenv("PYTHONIOENCODING", "utf-8")
    monkeypatch.setenv("AGROBR_TEST_LIVE_SCENARIO", scenario)
    pytester.makeconftest(
        'pytest_plugins = ["tests.conftest", "tests.integration.conftest", '
        '"tests.live_matrix_scenarios"]'
    )
    pytester.makeini("[pytest]\nasyncio_mode = auto\nmarkers = integration: live matrix")
    pytester.makepyfile(
        test_datasets_live=(root / "tests/integration/test_datasets_live.py").read_text(
            encoding="utf-8"
        ),
        test_unrelated="def test_unrelated():\n    pass\n",
    )
    report_path = pytester.path / "live.json"
    args = [
        "-p",
        "no:cacheprovider",
        "-o",
        "addopts=",
        "--disable-socket",
        "--allow-unix-socket",
        "--live-matrix-report",
        str(report_path),
        "--junitxml",
        str(pytester.path / "results.xml"),
    ]
    args += ["-k", selection] if selection else ["-m", "integration"]
    result = pytester.runpytest_subprocess(*args, timeout=90)
    assert result.ret == exit_code
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["exit_code"] == exit_code
    assert report["summary"] == {
        "selected": selected,
        "deselected": DATASET_COUNT - selected,
        "validated": validated,
        "expected_refusal": 0,
        "known_unavailable": known,
        "not_verified": not_verified,
        "failed": failed,
        "error": errors,
        "unexpected_skip": int(scenario == "fixture_skip"),
        "unvalidated": 0,
        "pending": 0,
    }
    assert len(report["cases"]) == selected
    assert len(report["deselected"]) == DATASET_COUNT - selected
    for case in report["cases"]:
        if case["status"] == "known_unavailable":
            assert case["live_matrix_reason"] == live_policy.ANTAQ_2024_522
        if case["status"] == "not_verified":
            assert case["live_matrix_reason"] == live_policy.USDA_MISSING_KEY
        if case["status"] == "validated":
            assert case["records_count"] == 1
            assert case["selected_source"] == "synthetic"
            assert case["attempted_sources"] == "synthetic"
