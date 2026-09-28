from __future__ import annotations

import argparse
import csv
import difflib
import hashlib
import io
import json
import os
import re
import shutil
import subprocess
import sys
import time
import zipfile
from pathlib import Path, PurePosixPath
from typing import Any, Literal
from xml.etree import ElementTree

import pydantic

from scripts import mutacao_inputs


class Mutation(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid")

    kind: Literal["code_replace", "xlsx_cells", "frame_cells", "bytes_replace"]
    target_file: str
    before: str | None = None
    after: str | None = None
    occurrences: int = 1
    sheet: str | None = None
    cells: dict[str, str | int | float] = pydantic.Field(default_factory=dict)
    target_module: str | None = None
    target_callable: str | None = None
    input_argument: str | None = None
    match_arguments: dict[str, Any] = pydantic.Field(default_factory=dict)
    frame_format: Literal["csv", "json"] = "csv"
    json_path: list[str | int] = pydantic.Field(default_factory=list)
    frame_filters: dict[str, str | int | float] = pydantic.Field(default_factory=dict)
    drop_columns: list[str] = pydantic.Field(default_factory=list)
    before_hex: str = ""
    after_hex: str = ""
    byte_offset: int = 0
    archive_member: str | None = None


class Experiment(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid")

    id: str = pydantic.Field(pattern=r"^[a-z0-9_]+$")
    family: str
    equivalence_class: str
    behavior: str
    tests: list[str] = pydantic.Field(min_length=1)
    mutation: Mutation
    assertion_functions: list[str] = pydantic.Field(default_factory=list)
    expected_guard: str | None = None
    guard_message: str | None = None
    timeout_seconds: int = pydantic.Field(default=120, ge=1, le=3600)
    markers: str = "not integration and not benchmark"


class Plan(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid")

    experiments: list[Experiment]


def contained(root: Path, relative: str | Path) -> Path:
    result = (root / relative).resolve()
    if not result.is_relative_to(root.resolve()):
        raise ValueError(f"Path leaves the permitted root: {relative}")
    return result


def snapshot(root: Path) -> dict[str, dict[str, str | int]]:
    paths = [root / name for name in ("pyproject.toml", "README.md", "LICENSE")]
    for directory in ("agrobr", "tests", "scripts", "examples", "docs", ".github"):
        paths.extend(path for path in (root / directory).rglob("*") if path.is_file())
    result = {}
    for path in sorted(paths):
        if not path.exists() or "__pycache__" in path.parts or path.suffix == ".pyc":
            continue
        relative = path.relative_to(root).as_posix()
        contained(root, relative)
        raw = path.read_bytes()
        result[relative] = {"bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}
    return result


def snapshot_digest(value: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def create_workspace(root: Path, output: Path) -> Path:
    workspace = contained(output, "workspace")
    if workspace.exists():
        raise ValueError(
            f"Use a new output directory; isolated workspace already exists: {workspace}"
        )
    workspace.mkdir(parents=True)
    ignore = shutil.ignore_patterns("__pycache__", "*.pyc", ".pytest_cache")
    for directory in ("agrobr", "tests", "scripts", "examples", "docs", ".github"):
        if (root / directory).is_dir():
            shutil.copytree(root / directory, workspace / directory, ignore=ignore)
    shutil.copy2(root / "pyproject.toml", workspace / "pyproject.toml")
    for filename in ("README.md", "LICENSE"):
        if (root / filename).is_file():
            shutil.copy2(root / filename, workspace / filename)
    return workspace


def worksheet_member(archive: zipfile.ZipFile, sheet: str) -> str:
    namespace = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
    relation_namespace = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
    workbook = ElementTree.fromstring(archive.read("xl/workbook.xml"))
    found = [node for node in workbook.iter(f"{{{namespace}}}sheet") if node.get("name") == sheet]
    if len(found) != 1:
        raise ValueError(f"Expected exactly one sheet named {sheet}")
    relation = found[0].get(f"{{{relation_namespace}}}id")
    relationships = ElementTree.fromstring(archive.read("xl/_rels/workbook.xml.rels"))
    target = next(node.attrib["Target"] for node in relationships if node.get("Id") == relation)
    return target.lstrip("/") if target.startswith("/") else str(PurePosixPath("xl") / target)


def mutate_xlsx(raw: bytes, mutation: Mutation, *, noop: bool) -> tuple[bytes, dict[str, Any]]:
    if not mutation.cells:
        raise ValueError("XLSX mutation requires at least one cell")
    namespace = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        member = worksheet_member(archive, mutation.sheet or "")
        document = ElementTree.fromstring(archive.read(member))
        cells = {node.get("r"): node for node in document.iter(f"{{{namespace}}}c")}
        original = {}
        for address, value in mutation.cells.items():
            if address not in cells:
                raise ValueError(f"Cell does not exist: {mutation.sheet}!{address}")
            node = cells[address]
            original[address] = ElementTree.tostring(node, encoding="unicode")
            for child in list(node):
                node.remove(child)
            if isinstance(value, str):
                node.set("t", "inlineStr")
                inline = ElementTree.SubElement(node, f"{{{namespace}}}is")
                ElementTree.SubElement(inline, f"{{{namespace}}}t").text = value
            else:
                node.attrib.pop("t", None)
                ElementTree.SubElement(node, f"{{{namespace}}}v").text = str(value)
        receipt = {"member": member, "original_cells": original, "changed_cells": mutation.cells}
        if noop:
            return raw, receipt
        changed = ElementTree.tostring(document, encoding="utf-8", xml_declaration=True)
        output = io.BytesIO()
        with zipfile.ZipFile(output, "w") as altered:
            for entry in archive.infolist():
                altered.writestr(
                    entry, changed if entry.filename == member else archive.read(entry.filename)
                )
        return output.getvalue(), receipt


def code_replacement(raw: bytes, mutation: Mutation, *, noop: bool) -> tuple[bytes, list[int]]:
    if mutation.before is None or mutation.after is None:
        raise ValueError("Code replacement requires before and after")
    newline = "\r\n" if b"\r\n" in raw else "\n"
    before = mutation.before.replace("\r\n", "\n").replace("\n", newline).encode()
    after = mutation.after.replace("\r\n", "\n").replace("\n", newline).encode()
    if not before or raw.count(before) != mutation.occurrences:
        raise ValueError("Code replacement occurrence count does not match")
    points = [match.start() for match in re.finditer(re.escape(before), raw)]
    changed = raw if noop else raw.replace(before, after)
    replacement = before if noop else after
    offsets = [
        point + index * (len(replacement) - len(before)) for index, point in enumerate(points)
    ]
    starts = [changed[:point].count(b"\n") + 1 for point in offsets]
    changed_offsets = set()
    matcher = difflib.SequenceMatcher(a=before.splitlines(), b=after.splitlines(), autojunk=False)
    for tag, left_start, left_end, right_start, right_end in matcher.get_opcodes():
        if tag != "equal":
            begin, end = (left_start, left_end) if noop else (right_start, right_end)
            changed_offsets.update(range(begin, end))
    if not changed_offsets and noop:
        changed_offsets.update(range(len(before.splitlines())))
    if not changed_offsets:
        raise ValueError("Code replacement has no observable changed line in this mode")
    lines = sorted({start + offset for start in starts for offset in changed_offsets})
    compile(changed, mutation.target_file, "exec")
    return changed, lines


def prepare_trial(
    workspace: Path, experiment: Experiment, mode: str, original: bytes, directory: Path
) -> dict[str, Any]:
    mutation = experiment.mutation
    target = contained(workspace, mutation.target_file)
    target.write_bytes(original)
    spec: dict[str, Any] = {
        "root": str(workspace),
        "output": str(directory / f"{mode}.json"),
        "mode": mode,
        "tests": experiment.tests,
        "markers": experiment.markers,
        "kind": mutation.kind,
        "target_file": mutation.target_file,
    }
    receipt: dict[str, Any] = {"original_sha256": hashlib.sha256(original).hexdigest()}
    original_input = original
    if mutation.kind == "code_replace":
        changed, lines = code_replacement(original, mutation, noop=mode != "mutant")
        target.write_bytes(changed)
        spec["target_lines"] = lines
    else:
        if mutation.kind == "xlsx_cells":
            changed, details = mutate_xlsx(original, mutation, noop=mode != "mutant")
        elif mutation.kind == "frame_cells":
            frame = mutacao_inputs.load_frame(
                original, mutation.frame_format, mutation.json_path, mutation.frame_filters
            )
            original_input = mutacao_inputs.frame_bytes(frame)
            changed = mutacao_inputs.mutate_frame(
                frame, mutation.cells, mutation.drop_columns, noop=mode != "mutant"
            )
            details = {
                "frame_format": mutation.frame_format,
                "json_path": mutation.json_path,
                "frame_filters": mutation.frame_filters,
                "cells": mutation.cells,
                "drop_columns": mutation.drop_columns,
                "canonical_input_sha256": hashlib.sha256(original_input).hexdigest(),
            }
        else:
            changed, details = mutacao_inputs.mutate_bytes(
                original,
                mutation.before_hex,
                mutation.after_hex,
                mutation.byte_offset,
                mutation.archive_member,
                noop=mode != "mutant",
            )
        receipt.update(details)
        payload = contained(workspace, f".f7_inputs/{experiment.id}_{mode}.bin")
        payload.parent.mkdir(exist_ok=True)
        payload.write_bytes(changed)
        spec.update(
            {
                "target_module": mutation.target_module,
                "target_callable": mutation.target_callable,
                "input_argument": mutation.input_argument,
                "input_sha256": hashlib.sha256(original_input).hexdigest(),
                "input_replacement": payload.relative_to(workspace).as_posix(),
                "match_arguments": mutation.match_arguments,
            }
        )
    receipt["delivered_sha256"] = hashlib.sha256(changed).hexdigest()
    receipt["identity"] = changed == original_input
    if mode != "mutant" and not receipt["identity"]:
        raise ValueError("Baseline or no-op changed bytes")
    if mode == "mutant" and receipt["identity"]:
        raise ValueError("Mutant is identical to the original")
    spec_path = directory / f"{mode}_spec.json"
    spec_path.write_text(json.dumps(spec, indent=2) + "\n", encoding="utf-8")
    receipt["spec"] = str(spec_path)
    return receipt


def run_trial(
    root: Path,
    workspace: Path,
    directory: Path,
    experiment: Experiment,
    mode: str,
    original: bytes,
    original_digest: str,
) -> dict[str, Any]:
    before = snapshot(root)
    if snapshot_digest(before) != original_digest:
        raise RuntimeError("Original source/fixture tree changed since the initial snapshot")
    started = time.perf_counter()
    try:
        receipt = prepare_trial(workspace, experiment, mode, original, directory)
    except (
        ValueError,
        SyntaxError,
        KeyError,
        StopIteration,
        zipfile.BadZipFile,
        ElementTree.ParseError,
    ) as error:
        receipt = {"preparation_error": f"{type(error).__name__}: {error}"}
    command = [sys.executable, "-m", "scripts._mutacao_pytest", receipt.get("spec", "")]
    timed_out = False
    with (directory / f"{mode}.log").open("wb") as output:
        if "preparation_error" in receipt:
            output.write(receipt["preparation_error"].encode("utf-8"))
            returncode = 2
        else:
            try:
                process = subprocess.run(
                    command,
                    cwd=workspace,
                    stdout=output,
                    stderr=subprocess.STDOUT,
                    check=False,
                    timeout=experiment.timeout_seconds,
                    env={
                        **os.environ,
                        "PYTHONPATH": str(workspace),
                        "PYTHONDONTWRITEBYTECODE": "1",
                        "PYTHONIOENCODING": "utf-8",
                    },
                )
                returncode = process.returncode
            except subprocess.TimeoutExpired:
                timed_out = True
                returncode = -1
    after = snapshot(root)
    report = {
        "command": command,
        "returncode": returncode,
        "timeout": timed_out,
        "seconds": time.perf_counter() - started,
        "receipt": receipt,
        "original_before_sha256": snapshot_digest(before),
        "original_after_sha256": snapshot_digest(after),
        "original_unchanged": before == after,
        "original_initial_sha256": original_digest,
        "original_files": len(before),
        "original_bytes": sum(int(item["bytes"]) for item in before.values()),
    }
    (directory / f"{mode}_audit.json").write_text(
        json.dumps(report, indent=2) + "\n", encoding="utf-8"
    )
    if before != after:
        raise RuntimeError(
            "Original source/fixture tree changed during the trial; no restoration attempted"
        )
    result = directory / f"{mode}.json"
    report["pytest"] = json.loads(result.read_text(encoding="utf-8")) if result.exists() else None
    return report


def green(trial: dict[str, Any]) -> bool:
    observed = trial["pytest"]
    return bool(
        trial["returncode"] == 0
        and not trial["timeout"]
        and observed
        and observed["socket_disabled"]
        and not observed["foreign_agrobr_modules"]
        and observed["selected"]
        and all(report["outcome"] == "passed" for report in observed["reports"])
        and set(observed["selected"])
        == {report["nodeid"] for report in observed["reports"] if report["when"] == "call"}
    )


def classify_result(experiment: Experiment, trial: dict[str, Any], nodeid: str) -> dict[str, Any]:
    observed = trial["pytest"]
    result: dict[str, Any] = {
        "phase": "invalid_mutant",
        "reached": False,
        "semantic_kill": False,
        "failure": "",
        "assertion": "",
    }
    if trial["receipt"].get("preparation_error"):
        return {**result, "failure": trial["receipt"]["preparation_error"]}
    if trial["timeout"]:
        return {**result, "phase": "timeout"}
    if not observed or observed["foreign_agrobr_modules"] or not observed["socket_disabled"]:
        return result
    if any(report["outcome"] == "failed" for report in observed["collection"]):
        return {**result, "phase": "collection_setup"}
    evidence = observed["evidence"].get(nodeid, [])
    result["reached"] = bool(evidence)
    result["evidence"] = evidence
    reports = [report for report in observed["reports"] if report["nodeid"] == nodeid]
    failures = [report for report in reports if report["outcome"] == "failed"]
    if not failures:
        return {**result, "phase": "survived" if evidence else "not_reached"}
    failure = failures[0]
    result["failure"] = failure["exception"] + ": " + failure["message"]
    if failure["when"] != "call":
        return {**result, "phase": "collection_setup"}
    if failure.get("collected_failures"):
        nested = [
            classify_result(
                experiment,
                {
                    **trial,
                    "pytest": {**observed, "reports": [{**failure, **inner}]},
                },
                nodeid,
            )
            for inner in failure["collected_failures"]
        ]
        return next(
            (item for item in nested if item["phase"] == "hash"),
            next((item for item in nested if item["semantic_kill"]), nested[0]),
        )
    frames = failure["frames"]
    relevant = [
        frame for frame in frames[-1:] if frame["function"] in experiment.assertion_functions
    ]
    if any(re.search(r"\bsha256\s*\(|\.hexdigest\s*\(", frame["statement"]) for frame in frames):
        return {**result, "phase": "hash"}
    semantic = failure["exception"] == "AssertionError" and bool(relevant)
    if (
        experiment.mutation.kind != "code_replace"
        and experiment.expected_guard
        in {"ParseError", "ContractViolationError", "SourceUnavailableError"}
        and failure["exception"] == experiment.expected_guard
        and experiment.guard_message
        and re.search(experiment.guard_message, failure["message"])
    ):
        semantic = True
        relevant = [
            {
                "guard": experiment.expected_guard,
                "message_pattern": experiment.guard_message,
                "frame": frames[-1],
            }
        ]
    if (
        experiment.expected_guard
        and failure["exception"] == "Failed"
        and "DID NOT RAISE" in failure["message"]
    ):
        semantic = experiment.expected_guard in failure["message"]
        relevant = frames[-1:]
    if semantic and evidence:
        return {**result, "phase": "semantic", "semantic_kill": True, "assertion": relevant[-1]}
    return {**result, "phase": "call_nonsemantic" if evidence else "not_reached"}


def run_experiment(
    root: Path, workspace: Path, output: Path, experiment: Experiment, original_digest: str
) -> list[dict[str, Any]]:
    directory = contained(output, f"runs/{experiment.id}")
    directory.mkdir(parents=True)
    target = contained(workspace, experiment.mutation.target_file)
    original = contained(root, experiment.mutation.target_file).read_bytes()
    phases = {}
    try:
        for mode in ("baseline", "noop", "mutant"):
            phases[mode] = run_trial(
                root, workspace, directory, experiment, mode, original, original_digest
            )
            if mode != "mutant" and not green(phases[mode]):
                break
    finally:
        target.write_bytes(original)
    baseline = green(phases["baseline"])
    noop = "noop" in phases and green(phases["noop"])
    rows = []
    selected = phases["baseline"]["pytest"]["selected"] if baseline else experiment.tests
    for nodeid in selected:
        result = (
            classify_result(experiment, phases["mutant"], nodeid)
            if "mutant" in phases
            else {
                "phase": "baseline_failed" if not baseline else "noop_failed",
                "reached": False,
                "semantic_kill": False,
                "failure": "",
                "assertion": "",
            }
        )
        rows.append(
            {
                "family": experiment.family,
                "equivalence_class": experiment.equivalence_class,
                "mutant": experiment.id,
                "nodeid": nodeid,
                "applicable": True,
                "baseline": baseline,
                "noop": noop,
                "valid": baseline
                and noop
                and result["reached"]
                and result["phase"] in {"semantic", "survived"},
                "behavior": experiment.behavior,
                **result,
            }
        )
    (directory / "result.json").write_text(
        json.dumps(rows, ensure_ascii=True, indent=2) + "\n", encoding="utf-8"
    )
    print(
        json.dumps(
            {
                "experiment": experiment.id,
                "baseline": baseline,
                "noop": noop,
                "phases": [row["phase"] for row in rows],
            }
        ),
        flush=True,
    )
    return rows


def main() -> None:
    argument_parser = argparse.ArgumentParser()
    argument_parser.add_argument("--plan", required=True, type=Path)
    argument_parser.add_argument("--output", required=True, type=Path)
    argument_parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    argument_parser.add_argument("--only", action="append", default=[])
    args = argument_parser.parse_args()
    root = args.root.resolve()
    output = contained(root / "reports", args.output.resolve())
    if output.exists() and any(output.iterdir()):
        raise ValueError("Output directory must be new or empty to preserve previous evidence")
    output.mkdir(parents=True, exist_ok=True)
    plan = Plan.model_validate_json(args.plan.read_bytes())
    if args.only:
        requested = set(args.only)
        unknown = requested - {experiment.id for experiment in plan.experiments}
        if unknown:
            raise ValueError(f"Unknown experiment IDs: {sorted(unknown)}")
        plan.experiments = [
            experiment for experiment in plan.experiments if experiment.id in requested
        ]
    (output / "plan.json").write_text(plan.model_dump_json(indent=2) + "\n", encoding="utf-8")
    baseline = snapshot(root)
    (output / "original_manifest.json").write_text(json.dumps(baseline, indent=2), encoding="utf-8")
    workspace = create_workspace(root, output)
    copied = snapshot(workspace)
    if copied != baseline:
        raise RuntimeError("Isolated source/fixture copy does not match original")
    rows = []
    for experiment in plan.experiments:
        rows.extend(run_experiment(root, workspace, output, experiment, snapshot_digest(baseline)))
    columns = [
        "family",
        "equivalence_class",
        "mutant",
        "nodeid",
        "applicable",
        "baseline",
        "noop",
        "valid",
        "reached",
        "semantic_kill",
        "phase",
        "behavior",
        "failure",
        "assertion",
        "evidence",
    ]
    with (output / "mutacao.csv").open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    key: json.dumps(row.get(key), ensure_ascii=True)
                    if isinstance(row.get(key), (list, dict))
                    else row.get(key, "")
                    for key in columns
                }
            )
    if any(not row["baseline"] or not row["noop"] for row in rows):
        raise SystemExit(2)


if __name__ == "__main__":
    main()
