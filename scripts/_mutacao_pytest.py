from __future__ import annotations

import argparse
import functools
import hashlib
import importlib
import inspect
import io
import json
import sys
from collections.abc import Generator
from pathlib import Path
from types import FrameType
from typing import Any, Literal

import httpx
import pandas as pd
import pydantic
import pytest

import agrobr
from scripts import mutacao_inputs


class RuntimeSpec(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid")

    root: Path
    output: Path
    mode: Literal["baseline", "noop", "mutant"]
    tests: list[str]
    markers: str = "not integration and not benchmark"
    kind: Literal["code_replace", "xlsx_cells", "frame_cells", "bytes_replace"]
    target_file: str
    target_lines: list[int] = pydantic.Field(default_factory=list)
    target_module: str | None = None
    target_callable: str | None = None
    input_argument: str | None = None
    input_sha256: str | None = None
    input_replacement: str | None = None
    match_arguments: dict[str, Any] = pydantic.Field(default_factory=dict)


class MutationObserver:
    def __init__(self, spec: RuntimeSpec) -> None:
        self.spec = spec
        self.root = spec.root.resolve()
        self.target = (self.root / spec.target_file).resolve()
        if not self.target.is_relative_to(self.root):
            raise ValueError("Mutation target is outside the isolated root")
        self.current = "collection"
        self.reports: list[dict[str, Any]] = []
        self.collections: list[dict[str, Any]] = []
        self.evidence: dict[str, list[dict[str, Any]]] = {}
        self.selected: list[str] = []
        self.replacement = (
            (self.root / spec.input_replacement).read_bytes() if spec.input_replacement else None
        )
        self.restore: tuple[Any, str, Any] | None = None
        self.previous_trace: Any = None
        self.socket_disabled = False
        self.trace_paths: dict[str, bool] = {}

    def record(self, evidence: dict[str, Any]) -> None:
        observed = self.evidence.setdefault(self.current, [])
        if evidence not in observed:
            observed.append(evidence)

    def traces_file(self, filename: str) -> bool:
        if filename not in self.trace_paths:
            self.trace_paths[filename] = Path(filename) == self.target
        return self.trace_paths[filename]

    def trace_call(self, frame: FrameType, event: str, _argument: Any) -> Any:
        if event == "call" and self.traces_file(frame.f_code.co_filename):
            return self.trace_line
        return None

    def trace_line(self, frame: FrameType, event: str, _argument: Any) -> Any:
        if event == "line" and frame.f_lineno in self.spec.target_lines:
            self.record(
                {
                    "kind": "executed_line",
                    "file": self.spec.target_file,
                    "line": frame.f_lineno,
                    "function": frame.f_code.co_name,
                }
            )
        return self.trace_line

    def input_bytes(self, value: Any) -> bytes | None:
        if self.spec.kind == "frame_cells" and isinstance(value, pd.DataFrame):
            return mutacao_inputs.frame_bytes(value)
        if isinstance(value, bytes):
            return value
        if isinstance(value, io.BytesIO):
            return value.getvalue()
        if isinstance(value, httpx.Response):
            return value.content
        return None

    def install_input_mutation(self) -> None:
        if not self.spec.target_module or not self.spec.target_callable:
            raise ValueError("Input mutation requires a module and callable")
        module = importlib.import_module(self.spec.target_module)
        container: Any = module
        *owners, name = self.spec.target_callable.split(".")
        for owner in owners:
            container = getattr(container, owner)
        original = getattr(container, name)
        implementation = Path(inspect.getfile(original)).resolve()
        if not implementation.is_relative_to(self.root):
            raise ValueError("Mutated callable comes from outside the isolated root")
        signature = inspect.signature(original)
        if (
            self.spec.input_argument != "$return"
            and self.spec.input_argument not in signature.parameters
        ):
            raise ValueError("Input mutation requires an existing callable argument")

        def altered_value(value: Any) -> Any:
            content = self.input_bytes(value)
            if content is None or hashlib.sha256(content).hexdigest() != self.spec.input_sha256:
                return value
            delivered = self.replacement if self.replacement is not None else content
            self.record(
                {
                    "kind": "source_return"
                    if self.spec.input_argument == "$return"
                    else "parser_input",
                    "callable": self.spec.target_module + ":" + self.spec.target_callable,
                    "implementation": implementation.relative_to(self.root).as_posix(),
                    "argument": self.spec.input_argument,
                    "matched_arguments": self.spec.match_arguments,
                    "received_sha256": hashlib.sha256(content).hexdigest(),
                    "delivered_sha256": hashlib.sha256(delivered).hexdigest(),
                    "changed": delivered != content,
                }
            )
            if delivered != content:
                replacement: Any = delivered
                if isinstance(value, io.BytesIO):
                    replacement = io.BytesIO(delivered)
                    replacement.seek(value.tell())
                elif isinstance(value, pd.DataFrame):
                    replacement = mutacao_inputs.replace_frame_values(value, delivered)
                elif isinstance(value, httpx.Response):
                    replacement = httpx.Response(
                        value.status_code,
                        headers=value.headers,
                        content=delivered,
                        request=value.request,
                        extensions=value.extensions.copy(),
                    )
                return replacement
            return value

        def matches(bound: inspect.BoundArguments) -> bool:
            bound.apply_defaults()
            return all(
                bound.arguments.get(key) == value
                for key, value in self.spec.match_arguments.items()
            )

        @functools.wraps(original)
        def wrapped(*args: Any, **kwargs: Any) -> Any:
            bound = signature.bind(*args, **kwargs)
            if matches(bound):
                argument = self.spec.input_argument or ""
                bound.arguments[argument] = altered_value(bound.arguments.get(argument))
            return original(*bound.args, **bound.kwargs)

        @functools.wraps(original)
        async def wrapped_return(*args: Any, **kwargs: Any) -> Any:
            value = await original(*args, **kwargs)
            return altered_value(value) if matches(signature.bind(*args, **kwargs)) else value

        if self.spec.input_argument == "$return" and not inspect.iscoroutinefunction(original):
            raise ValueError("Return mutation requires an async acquisition callable")
        self.restore = container, name, original
        setattr(
            container, name, wrapped_return if self.spec.input_argument == "$return" else wrapped
        )

    def pytest_configure(self, config: pytest.Config) -> None:
        self.socket_disabled = bool(config.getoption("disable_socket", default=False))
        if not self.socket_disabled:
            raise ValueError("Socket blocking is required")
        if self.spec.kind != "code_replace":
            self.install_input_mutation()
        self.previous_trace = sys.gettrace()
        if self.spec.kind == "code_replace":
            sys.settrace(self.trace_call)

    def pytest_collection_finish(self, session: pytest.Session) -> None:
        self.selected = [item.nodeid for item in session.items]

    def pytest_collectreport(self, report: pytest.CollectReport) -> None:
        if report.failed or report.skipped:
            self.collections.append(
                {
                    "nodeid": report.nodeid,
                    "outcome": report.outcome,
                    "details": str(report.longrepr),
                }
            )

    @pytest.hookimpl(hookwrapper=True)
    def pytest_runtest_protocol(self, item: pytest.Item) -> Generator[None, Any, None]:
        self.current = item.nodeid
        yield
        self.current = "between_tests"

    @pytest.hookimpl(hookwrapper=True)
    def pytest_runtest_makereport(
        self, item: pytest.Item, call: pytest.CallInfo[Any]
    ) -> Generator[None, Any, None]:
        outcome = yield
        report = outcome.get_result()
        details = self.exception_details(call.excinfo.value) if call.excinfo is not None else {}
        self.reports.append(
            {
                "nodeid": item.nodeid,
                "when": report.when,
                "outcome": report.outcome,
                "duration": report.duration,
                "exception": details.get("exception"),
                "message": details.get("message", ""),
                "frames": details.get("frames", []),
                "collected_failures": details.get("collected_failures", []),
                "longrepr": report.longreprtext if report.failed or report.skipped else "",
            }
        )

    def exception_details(self, error: BaseException) -> dict[str, Any]:
        frames = []
        for entry in pytest.ExceptionInfo.from_exception(error).traceback:
            path = Path(str(entry.path))
            if path.is_absolute() and path.is_relative_to(self.root):
                frames.append(
                    {
                        "file": path.relative_to(self.root).as_posix(),
                        "line": entry.lineno + 1,
                        "function": entry.name,
                        "statement": str(entry.statement),
                    }
                )
        collected = getattr(error, "_agrobr_case_failures", ())
        return {
            "exception": type(error).__name__,
            "message": str(error),
            "frames": frames,
            "collected_failures": [
                self.exception_details(inner)
                for inner in collected
                if isinstance(inner, BaseException)
            ],
        }

    def finish(self, returncode: int) -> None:
        sys.settrace(self.previous_trace)
        if self.restore:
            container, name, original = self.restore
            setattr(container, name, original)
        modules = {
            name: str(Path(module.__file__).resolve())
            for name, module in sys.modules.copy().items()
            if (name == "agrobr" or name.startswith("agrobr."))
            and getattr(module, "__file__", None)
        }
        foreign = {
            name: path for name, path in modules.items() if not Path(path).is_relative_to(self.root)
        }
        self.spec.output.parent.mkdir(parents=True, exist_ok=True)
        self.spec.output.write_text(
            json.dumps(
                {
                    "returncode": returncode,
                    "agrobr_file": str(Path(agrobr.__file__).resolve()),
                    "isolated_root": str(self.root),
                    "foreign_agrobr_modules": foreign,
                    "socket_disabled": self.socket_disabled,
                    "selected": self.selected,
                    "collection": self.collections,
                    "reports": self.reports,
                    "evidence": self.evidence,
                    "python": sys.version,
                },
                ensure_ascii=True,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )


def main() -> None:
    argument_parser = argparse.ArgumentParser()
    argument_parser.add_argument("spec", type=Path)
    args = argument_parser.parse_args()
    spec = RuntimeSpec.model_validate_json(args.spec.read_bytes())
    root = spec.root.resolve()
    if not Path(agrobr.__file__).resolve().is_relative_to(root):
        raise ValueError("agrobr was not imported from the isolated copy")
    (root / ".mutacao_tmp").mkdir(exist_ok=True)
    observer = MutationObserver(spec)
    status = int(
        pytest.main(
            [
                *spec.tests,
                "-p",
                "no:cacheprovider",
                "--disable-socket",
                "--allow-unix-socket",
                "--basetemp",
                str(root / ".mutacao_tmp" / spec.mode),
                "--junitxml",
                str(spec.output.with_suffix(".xml")),
                "-m",
                spec.markers,
                "--tb=short",
                "-q",
            ],
            plugins=[observer],
        )
    )
    observer.finish(status)
    raise SystemExit(status)


if __name__ == "__main__":
    main()
