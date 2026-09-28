from __future__ import annotations

import json
import os
from collections import Counter
from collections.abc import Callable, Generator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import pytest

from agrobr.antaq import client as antaq_client
from agrobr.exceptions import SourceUnavailableError
from tests.integration import live_policy


@dataclass
class LiveMatrixResults:
    cases: dict[str, dict[str, Any]] = field(default_factory=dict)
    deselected: list[str] = field(default_factory=list)
    started_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())

    def record(self, report: pytest.TestReport) -> None:
        case = self.cases[report.nodeid]
        case["phases"][report.when] = report.outcome
        properties = dict(report.user_properties)
        for name in ("selected_source", "attempted_sources", "records_count", "live_matrix_reason"):
            if name in properties:
                case[name] = properties[name]
        if report.failed:
            case["status"] = "failed" if report.when == "call" else "error"
        elif report.skipped:
            case["status"] = (
                properties["live_matrix_status"]
                if report.when == "call"
                and properties.get("live_matrix_status") in {"known_unavailable", "not_verified"}
                else "unexpected_skip"
            )
        elif report.when == "call":
            case["status"] = (
                properties["live_matrix_status"]
                if properties.get("live_matrix_status") in {"validated", "expected_refusal"}
                else "unvalidated"
            )

    def summary(self) -> dict[str, int]:
        counts = Counter(case["status"] for case in self.cases.values())
        return {
            "selected": len(self.cases),
            "deselected": len(self.deselected),
            "validated": counts["validated"],
            "expected_refusal": counts["expected_refusal"],
            "known_unavailable": counts["known_unavailable"],
            "not_verified": counts["not_verified"],
            "failed": counts["failed"],
            "error": counts["error"],
            "unexpected_skip": counts["unexpected_skip"],
            "unvalidated": counts["unvalidated"],
            "pending": counts["pending"],
        }

    def requires_failure(self) -> bool:
        return bool(self.cases) and (
            not (self.summary()["validated"] or self.summary()["expected_refusal"])
            or any(
                case["status"]
                not in {"validated", "expected_refusal", "known_unavailable", "not_verified"}
                for case in self.cases.values()
            )
        )


_RESULTS = pytest.StashKey[LiveMatrixResults]()


def pytest_configure(config: pytest.Config) -> None:
    config.stash[_RESULTS] = LiveMatrixResults()


def _is_matrix_case(item: pytest.Item) -> bool:
    return (
        item.path.name == "test_datasets_live.py"
        and getattr(item, "originalname", None) == "test_dataset_live_satisfies_contract"
    )


@pytest.hookimpl(trylast=True)
def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    results = config.stash[_RESULTS]
    for item in items:
        if _is_matrix_case(item):
            results.cases[item.nodeid] = {
                "nodeid": item.nodeid,
                "dataset": item.callspec.params["dataset_name"],
                "status": "pending",
                "phases": {},
            }


def pytest_deselected(items: list[pytest.Item]) -> None:
    for item in items:
        if _is_matrix_case(item):
            item.config.stash[_RESULTS].deselected.append(item.callspec.params["dataset_name"])


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item: pytest.Item) -> Generator[None, Any, None]:
    outcome = yield
    results = item.config.stash[_RESULTS]
    if item.nodeid in results.cases:
        results.record(outcome.get_result())


def pytest_sessionfinish(session: pytest.Session) -> None:
    results = session.config.stash[_RESULTS]
    if session.config.option.collectonly:
        return
    if results.requires_failure() and session.exitstatus == pytest.ExitCode.OK:
        session.exitstatus = pytest.ExitCode.TESTS_FAILED
    report_path = session.config.getoption("live_matrix_report")
    if report_path:
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "started_at": results.started_at,
                    "finished_at": datetime.now(UTC).isoformat(),
                    "exit_code": int(session.exitstatus),
                    "summary": results.summary(),
                    "cases": list(results.cases.values()),
                    "deselected": sorted(results.deselected),
                },
                ensure_ascii=False,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )


def pytest_terminal_summary(terminalreporter: Any) -> None:
    results = terminalreporter.config.stash[_RESULTS]
    if not results.cases or terminalreporter.config.option.collectonly:
        return
    summary = results.summary()
    terminalreporter.write_sep(
        "=",
        f"Live datasets: {summary['validated']}/{summary['selected']} validados; "
        f"{summary['expected_refusal']} recusas esperadas; "
        f"{summary['known_unavailable']} indisponibilidades conhecidas; "
        f"{summary['not_verified']} não verificados",
    )
    if not (summary["validated"] or summary["expected_refusal"]):
        terminalreporter.write_line("FALHA: nenhum dataset selecionado completou a validação live.")
    for case in results.cases.values():
        reason = case.get("live_matrix_reason", "")
        terminalreporter.write_line(f"{case['dataset']}: {case['status']} {reason}".rstrip())
        if case["status"] == "not_verified" and os.environ.get("GITHUB_ACTIONS") == "true":
            terminalreporter.write_line(
                f"::warning title=Não verificado::{case['dataset']}: {reason}"
            )


@pytest.fixture
def apply_live_policy(
    monkeypatch: pytest.MonkeyPatch,
    record_property: Callable[[str, Any], None],
) -> Callable[[str, dict[str, Any]], None]:
    def skip_known(reason: str, status: str = "known_unavailable") -> None:
        record_property("live_matrix_status", status)
        record_property("live_matrix_reason", reason)
        pytest.skip(reason)

    def apply(dataset_name: str, kwargs: dict[str, Any]) -> None:
        if live_policy.missing_usda_key(dataset_name, kwargs):
            skip_known(live_policy.USDA_MISSING_KEY, "not_verified")
        if dataset_name != "movimentacao_portuaria" or kwargs.get("ano") != 2024:
            return
        original = antaq_client.fetch_ano_zip

        async def observed_download(ano: int) -> bytes:
            try:
                return await original(ano)
            except SourceUnavailableError as exc:
                if live_policy.known_antaq_2024_outage(exc, ano):
                    skip_known(
                        f"ANTAQ: aviso oficial de indisponibilidade: {exc.notice_url}"
                        if isinstance(exc, antaq_client.OfficialOutageError)
                        else live_policy.ANTAQ_2024_522
                    )
                raise

        monkeypatch.setattr(antaq_client, "fetch_ano_zip", observed_download)

    return apply
