from __future__ import annotations

import hashlib
import io
import zipfile
from pathlib import Path

import httpx
import pandas as pd
import pytest

from agrobr.http import responses
from agrobr.ibge import client
from scripts import _mutacao_pytest, mutacao_inputs, mutacao_testes
from tests import helpers


def test_frame_mutation_noop_and_source_identity():
    raw = b"D1C,D3C,V\n35,2023,120\n41,2024,250\n"
    frame = mutacao_inputs.load_frame(raw, "csv", [], {})
    before = mutacao_inputs.frame_bytes(frame)
    noop = mutacao_inputs.mutate_frame(frame, {"1/V": "999"}, [], noop=True)
    assert noop == before
    altered = mutacao_inputs.mutate_frame(frame, {"1/V": "999"}, ["D3C"], noop=False)
    result = mutacao_inputs.replace_frame_values(frame, altered)
    assert result.to_dict("records") == [{"D1C": "35", "V": "120"}, {"D1C": "41", "V": "999"}]
    assert mutacao_inputs.frame_bytes(frame) == before


def test_frame_mutation_preserves_unrelated_types_index_and_attributes():
    frame = pd.DataFrame({"V": ["100", "200"], "count": pd.Series([1, None], dtype="Int64")})
    frame.index = [8, 13]
    frame.attrs["channel"] = "sidra"
    reference = frame.reset_index(drop=True)
    payload = mutacao_inputs.mutate_frame(reference, {"1/V": "201"}, [], noop=False)
    result = mutacao_inputs.replace_frame_values(frame, payload)
    pd.testing.assert_series_equal(result["count"], frame["count"])
    assert result.index.tolist() == [8, 13]
    assert result.attrs == {"channel": "sidra"}
    assert result["V"].tolist() == ["100", "201"]
    assert frame["V"].tolist() == ["100", "200"]


@pytest.mark.parametrize(
    "cells,columns",
    [({"9/V": "1"}, []), ({"0/absent": "1"}, []), ({}, ["absent"]), ({"0/V": "100"}, [])],
)
def test_frame_mutation_rejects_invalid_or_equivalent_edits(cells, columns):
    with pytest.raises(ValueError):
        mutacao_inputs.mutate_frame(pd.DataFrame({"V": ["100"]}), cells, columns, noop=False)


def test_json_input_path_and_record_filter():
    raw = b'{"records":[{"year":"2023","V":"100"},{"year":"2024","V":"200"}]}'
    frame = mutacao_inputs.load_frame(raw, "json", ["records"], {"year": "2024"})
    assert frame.to_dict("records") == [{"year": "2024", "V": "200"}]
    with pytest.raises(ValueError, match="empty"):
        mutacao_inputs.load_frame(raw, "json", ["records"], {"year": "2000"})


def test_binary_mutation_changes_only_declared_member_and_offset():
    original = io.BytesIO()
    with zipfile.ZipFile(original, "w") as archive:
        archive.writestr("table.xls", b"header\x01\x02\x03tail")
        archive.writestr("untouched.txt", b"unchanged")
    raw = original.getvalue()
    noop, _ = mutacao_inputs.mutate_bytes(raw, "010203", "040506", 6, "table.xls", noop=True)
    assert noop == raw
    changed, receipt = mutacao_inputs.mutate_bytes(
        raw, "010203", "040506", 6, "table.xls", noop=False
    )
    with zipfile.ZipFile(io.BytesIO(changed)) as archive:
        assert archive.read("table.xls") == b"header\x04\x05\x06tail"
        assert archive.read("untouched.txt") == b"unchanged"
    assert receipt["offset"] == 6
    with pytest.raises(ValueError, match="match"):
        mutacao_inputs.mutate_bytes(raw, "010203", "040506", 0, "table.xls", noop=False)


async def test_return_mutation_matches_body_and_arguments_and_preserves_noop(monkeypatch, tmp_path):
    original = pd.DataFrame({"V": ["100"]})
    bodies = {"1092": original}

    async def fetch(table_code: str) -> pd.DataFrame:
        return bodies.get(table_code, original)

    monkeypatch.setattr(client, "fetch_sidra", fetch)
    spec = _mutacao_pytest.RuntimeSpec(
        root=Path(__file__).resolve().parents[1],
        output=tmp_path / "unused.json",
        mode="mutant",
        tests=["adapter"],
        kind="frame_cells",
        target_file="tests/test_mutacao_inputs.py",
        target_module="agrobr.ibge.client",
        target_callable="fetch_sidra",
        input_argument="$return",
        input_sha256=hashlib.sha256(mutacao_inputs.frame_bytes(original)).hexdigest(),
        match_arguments={"table_code": "1092"},
    )
    observer = _mutacao_pytest.MutationObserver(spec)
    observer.replacement = mutacao_inputs.mutate_frame(original, {"0/V": "101"}, [], noop=False)
    observer.install_input_mutation()
    assert await client.fetch_sidra("other") is original
    assert observer.evidence == {}
    bodies["1092"] = pd.DataFrame({"V": ["200"]})
    assert (await client.fetch_sidra("1092"))["V"].tolist() == ["200"]
    assert observer.evidence == {}
    bodies["1092"] = original
    assert (await client.fetch_sidra("1092"))["V"].tolist() == ["101"]
    assert original["V"].tolist() == ["100"]
    observer.replacement = mutacao_inputs.frame_bytes(original)
    assert await client.fetch_sidra("1092") is original
    assert observer.evidence["collection"][-1]["changed"] is False
    assert observer.evidence["collection"][0]["kind"] == "source_return"


def test_code_reach_excludes_following_lines_after_shorter_replacements():
    before = "raise ValueError(\n            'invalid'\n        )"
    source = f"def first(flag):\n    if flag:\n        {before}\n    return 7\n\ndef second(flag):\n    if flag:\n        {before}\n    return 7\n".encode()
    mutation = mutacao_testes.Mutation(
        kind="code_replace",
        target_file="example.py",
        before=before,
        after="return 0",
        occurrences=2,
    )
    changed, reached = mutacao_testes.code_replacement(source, mutation, noop=False)
    assert reached == [3, 8]
    assert changed.splitlines()[3] == b"    return 7"
    assert changed.splitlines()[8] == b"    return 7"
    noop, original_lines = mutacao_testes.code_replacement(source, mutation, noop=True)
    assert noop == source
    assert original_lines == [3, 4, 5, 10, 11, 12]


def test_code_reach_excludes_unchanged_context_and_observes_insertions():
    source = b"def example(flag):\n    if flag:\n        raise ValueError()\n    return 7\n"
    mutation = mutacao_testes.Mutation(
        kind="code_replace",
        target_file="example.py",
        before="        raise ValueError()\n    return 7",
        after="        pass\n    return 7",
    )
    assert mutacao_testes.code_replacement(source, mutation, noop=False)[1] == [3]
    mutation.before = "    if flag:"
    mutation.after = "    return 9\n    if flag:"
    assert mutacao_testes.code_replacement(source, mutation, noop=False)[1] == [2]
    assert mutacao_testes.code_replacement(source, mutation, noop=True)[0] == source


def test_response_mutation_preserves_transport_and_noop(monkeypatch, tmp_path):
    raw = b'{"value":123}'
    original = httpx.Response(
        200,
        content=raw,
        headers={"x-source": "fixture"},
        request=httpx.Request("GET", "https://fixture.invalid/data"),
    )
    seen = []

    def parse_response(response: httpx.Response, _source: str) -> object:
        seen.append(response)
        return response.json()

    monkeypatch.setattr(responses, "parse_json_response", parse_response)
    spec = _mutacao_pytest.RuntimeSpec(
        root=Path(__file__).resolve().parents[1],
        output=tmp_path / "unused.json",
        mode="mutant",
        tests=["adapter"],
        kind="bytes_replace",
        target_file="tests/test_mutacao_inputs.py",
        target_module="agrobr.http.responses",
        target_callable="parse_json_response",
        input_argument="response",
        input_sha256=hashlib.sha256(raw).hexdigest(),
    )
    observer = _mutacao_pytest.MutationObserver(spec)
    observer.replacement = b'{"value":124}'
    observer.install_input_mutation()
    assert responses.parse_json_response(original, "ibge") == {"value": 124}
    assert seen[-1].request is original.request
    assert seen[-1].headers == original.headers
    assert original.content == raw
    observer.replacement = raw
    assert responses.parse_json_response(original, "ibge") == {"value": 123}
    assert seen[-1] is original


def test_collector_preserves_semantic_failure_evidence_and_runs_later_cases(tmp_path):
    visited = []
    with pytest.raises(pytest.fail.Exception) as caught, helpers.collect_failures() as check:
        for value in [1, 2]:
            with check(value):
                visited.append(value)
                assert value == 2
    assert visited == [1, 2]
    spec = _mutacao_pytest.RuntimeSpec(
        root=Path(__file__).resolve().parents[1],
        output=tmp_path / "unused.json",
        mode="mutant",
        tests=["collector"],
        kind="code_replace",
        target_file="tests/test_mutacao_inputs.py",
    )
    observer = _mutacao_pytest.MutationObserver(spec)
    details = observer.exception_details(caught.value)
    assert len(details["collected_failures"]) == 1
    assert details["collected_failures"][0]["exception"] == "AssertionError"
    assert (
        details["collected_failures"][0]["frames"][-1]["statement"].strip() == "assert value == 2"
    )
    experiment = mutacao_testes.Experiment(
        id="collector",
        family="k6",
        equivalence_class="collector",
        behavior="retains assertion",
        tests=["collector"],
        assertion_functions=[
            "test_collector_preserves_semantic_failure_evidence_and_runs_later_cases"
        ],
        mutation={
            "kind": "code_replace",
            "target_file": "tests/helpers.py",
            "before": "a",
            "after": "b",
        },
    )
    trial = {
        "receipt": {},
        "timeout": False,
        "pytest": {
            "foreign_agrobr_modules": {},
            "socket_disabled": True,
            "collection": [],
            "evidence": {"collector": [{"kind": "executed_line", "line": 1}]},
            "reports": [{"nodeid": "collector", "when": "call", "outcome": "failed", **details}],
        },
    }
    result = mutacao_testes.classify_result(experiment, trial, "collector")
    assert result["semantic_kill"]
    trial["pytest"]["reports"][0]["collected_failures"][0]["exception"] = "KeyError"
    assert not mutacao_testes.classify_result(experiment, trial, "collector")["semantic_kill"]


@pytest.mark.parametrize(
    "exception,message,semantic",
    [
        ("SourceUnavailableError", "Parse failed: missing dimension", True),
        ("SourceUnavailableError", "connection timed out", False),
        ("RuntimeError", "Parse failed: missing dimension", False),
    ],
)
def test_input_guard_recognizes_only_declared_layout_failure(exception, message, semantic):
    experiment = mutacao_testes.Experiment(
        id="layout_guard",
        family="k6",
        equivalence_class="typed_guard",
        behavior="layout guard",
        tests=["layout"],
        expected_guard="SourceUnavailableError",
        guard_message="Parse failed: missing dimension",
        mutation={"kind": "bytes_replace", "target_file": "fixture.json"},
    )
    trial = {
        "receipt": {},
        "timeout": False,
        "pytest": {
            "foreign_agrobr_modules": {},
            "socket_disabled": True,
            "collection": [],
            "evidence": {"layout": [{"kind": "parser_input"}]},
            "reports": [
                {
                    "nodeid": "layout",
                    "when": "call",
                    "outcome": "failed",
                    "exception": exception,
                    "message": message,
                    "frames": [{"function": "fetch", "statement": "raise error"}],
                }
            ],
        },
    }
    assert mutacao_testes.classify_result(experiment, trial, "layout")["semantic_kill"] is semantic
