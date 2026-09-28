from __future__ import annotations

import hashlib
import json
from collections.abc import Awaitable, Callable
from typing import Any, Generic, TypeVar

from agrobr.exceptions import SourceUnavailableError

PageT = TypeVar("PageT")


class PageState(Generic[PageT]):
    def __init__(self, diagnostic_limit: Callable[[], int]) -> None:
        self._diagnostic_limit = diagnostic_limit
        self.pages: list[PageT] = []
        self.accepted = self.received = self.overlap = self.selected = self.parse_ms = 0
        self.previous_signature: str | None = None
        self.windows: dict[str, tuple[int, int]] = {}
        self.ambiguity_count = 0
        self.ambiguities: list[dict[str, Any]] = []
        self.diagnostics: dict[str, Any] = {}
        self.statistics: dict[str, dict[str, int]] = {}
        self.accepted_diagnostics: dict[str, Any] = {}
        self.accepted_statistics: dict[str, dict[str, int]] = {}
        self.warnings: list[str] = []
        self.warning_count = 0

    def _diagnostics(self, parsed: Any, offset: int) -> None:
        maximum = self._diagnostic_limit()
        overlap = int(self.accepted > 0)
        overlap_values = {}
        if overlap:
            properties = parsed.records[0].properties
            overlap_values = properties.model_dump()
            overlap_values.update(properties.model_dump(by_alias=True))
        for category, entry in parsed.diagnostics.items():
            aggregate = self.diagnostics.setdefault(
                category, {"count": 0, "examples": [], "examples_omitted": 0}
            )
            aggregate["count"] += entry["count"]
            for example in entry["examples"]:
                if len(aggregate["examples"]) < maximum:
                    aggregate["examples"].append(
                        {"page_index": len(self.pages), "requested_offset": offset, **example}
                    )
            aggregate["examples_omitted"] = aggregate["count"] - len(aggregate["examples"])
            accepted = self.accepted_diagnostics.setdefault(
                category, {"count": 0, "examples": [], "examples_omitted": 0}
            )
            repeated = overlap and any(example["row_index"] == 0 for example in entry["examples"])
            accepted["count"] += entry["count"] - int(repeated)
            for example in entry["examples"]:
                if example["row_index"] >= overlap and len(accepted["examples"]) < maximum:
                    accepted["examples"].append(
                        {
                            "page_index": len(self.pages),
                            "accepted_position": self.accepted + example["row_index"] - overlap,
                            "row_index": example["row_index"],
                        }
                    )
            accepted["examples_omitted"] = accepted["count"] - len(accepted["examples"])
        for name, values in parsed.statistics.items():
            current = self.statistics.setdefault(name, {})
            for key, count in values.items():
                current[key] = current.get(key, 0) + count
            if overlap and name not in overlap_values:
                raise ValueError(f"Campo estatístico ausente nas propriedades de overlap: {name}")
            previous = overlap_values.get(name)
            deductions = {
                "null_count": int(overlap and previous is None),
                "empty_count": int(overlap and isinstance(previous, str) and previous == ""),
                "whitespace_count": int(
                    overlap
                    and isinstance(previous, str)
                    and previous != ""
                    and previous.strip() == ""
                ),
            }
            accepted_stats = self.accepted_statistics.setdefault(name, {})
            for key, count in values.items():
                accepted_stats[key] = accepted_stats.get(key, 0) + count - deductions[key]
        for warning in parsed.warnings:
            self.warning_count += 1
            if len(self.warnings) < maximum:
                self.warnings.append(f"Página {len(self.pages)}, offset {offset}: {warning}")

    def _ambiguity(self, kind: str, offset: int) -> None:
        self.ambiguity_count += 1
        if len(self.ambiguities) < self._diagnostic_limit():
            self.ambiguities.append({"kind": kind, "page_index": len(self.pages), "offset": offset})

    def _record_sequence(self, signatures: list[str], offset: int) -> str:
        sequence = hashlib.sha256(
            json.dumps(signatures, separators=(",", ":")).encode()
        ).hexdigest()
        if len(set(signatures)) == 1 and len(signatures) > 1:
            self._ambiguity("indistinguishable_occurrences_in_window", offset)
        if sequence in self.windows:
            self._ambiguity("repeated_indistinguishable_window", offset)
        else:
            self.windows[sequence] = (len(self.pages), offset)
        self.previous_signature = signatures[-1]
        return sequence


async def collect_pages(
    state: PageState[Any],
    *,
    target: int,
    page_size: int,
    max_pages: int,
    source: str,
    fetch: Callable[[int, int], Awaitable[bytes]],
    consume: Callable[[bytes, int, int], None],
) -> None:
    while state.accepted < target:
        if len(state.pages) >= max_pages:
            raise SourceUnavailableError(
                source=source, last_error="Limite operacional de páginas excedido"
            )
        overlap = int(state.accepted > 0)
        offset = state.accepted - overlap
        count = min(page_size, target - state.accepted) + overlap
        content = await fetch(offset, count)
        consume(content, offset, count)
        del content
        if state.accepted <= offset + overlap:
            raise SourceUnavailableError(source=source, last_error="Página sem progresso")
