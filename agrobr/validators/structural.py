from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from agrobr import _log

from ..constants import Fonte
from ..models import Fingerprint

logger = _log.get_logger(__name__)

THRESHOLD_HIGH = 0.85
THRESHOLD_MEDIUM = 0.70
THRESHOLD_LOW = 0.50


@dataclass
class StructuralValidationResult:
    source: Fonte
    similarity: float
    passed: bool
    level: str
    differences: dict[str, Any]
    current_fingerprint: Fingerprint | None
    baseline_fingerprint: Fingerprint | None
    message: str


def validate_structure(
    current: Fingerprint,
    baseline: Fingerprint,
) -> StructuralValidationResult:
    similarity, differences = compare_fingerprints(current, baseline)

    if similarity >= THRESHOLD_HIGH:
        level = "high"
        passed = True
        message = "Structure matches baseline"
    elif similarity >= THRESHOLD_MEDIUM:
        level = "medium"
        passed = True
        message = f"Minor structural differences detected ({similarity:.1%} similarity)"
    elif similarity >= THRESHOLD_LOW:
        level = "low"
        passed = False
        message = f"Significant structural changes ({similarity:.1%} similarity)"
    else:
        level = "critical"
        passed = False
        message = f"Major layout change detected ({similarity:.1%} similarity)"

    logger.info(
        "structural_validation",
        source=current.source.value,
        similarity=similarity,
        level=level,
        passed=passed,
    )

    return StructuralValidationResult(
        source=current.source,
        similarity=similarity,
        passed=passed,
        level=level,
        differences=differences,
        current_fingerprint=current,
        baseline_fingerprint=baseline,
        message=message,
    )


_SCORE_WEIGHTS = {
    "structure": 0.25,
    "table_classes": 0.20,
    "key_ids": 0.15,
    "table_headers": 0.30,
    "element_counts": 0.10,
}


def _score_overlap(
    current: list[Any],
    reference: list[Any],
) -> tuple[float, dict[str, list[Any]] | None]:
    if not reference:
        return 1.0, None
    matches = sum(1 for item in reference if item in current)
    score = matches / len(reference)
    if score == 1.0:
        return score, None
    return score, {
        "missing": [item for item in reference if item not in current],
        "new": [item for item in current if item not in reference],
    }


def _score_headers(
    current: list[list[str]],
    reference: list[list[str]],
) -> float:
    best = 0.0
    for ref_headers in reference:
        for cur_headers in current:
            ref_set = set(ref_headers)
            cur_set = set(cur_headers)
            if ref_set or cur_set:
                jaccard = len(ref_set & cur_set) / len(ref_set | cur_set)
                best = max(best, jaccard)
    return best


def _score_element_counts(
    current: dict[str, int],
    reference: dict[str, int],
) -> tuple[float, dict[str, dict[str, int]]]:
    diffs: dict[str, dict[str, int]] = {}
    for key, ref_count in reference.items():
        cur_count = current.get(key, 0)
        if ref_count > 0 and abs(cur_count - ref_count) / ref_count > 0.5:
            diffs[key] = {"reference": ref_count, "current": cur_count}
    return max(0.0, 1 - len(diffs) * 0.2), diffs


def compare_fingerprints(
    current: Fingerprint,
    reference: Fingerprint,
) -> tuple[float, dict[str, Any]]:
    scores: dict[str, float] = {}
    details: dict[str, Any] = {}

    scores["structure"] = 1.0 if current.structure_hash == reference.structure_hash else 0.0
    if scores["structure"] == 0:
        details["structure_changed"] = {
            "current": current.structure_hash,
            "reference": reference.structure_hash,
        }

    scores["table_classes"], tc_diff = _score_overlap(
        current.table_classes, reference.table_classes
    )
    if tc_diff is not None:
        details["table_classes_diff"] = tc_diff

    scores["key_ids"], kid_diff = _score_overlap(current.key_ids, reference.key_ids)
    if kid_diff is not None:
        details["key_ids_diff"] = kid_diff

    if reference.table_headers:
        scores["table_headers"] = _score_headers(current.table_headers, reference.table_headers)
        if scores["table_headers"] < 0.9:
            details["table_headers_diff"] = {
                "reference": reference.table_headers,
                "current": current.table_headers,
            }
    else:
        scores["table_headers"] = 1.0

    scores["element_counts"], count_diffs = _score_element_counts(
        current.element_counts, reference.element_counts
    )
    if count_diffs:
        details["element_counts_diff"] = count_diffs

    final_score = math.fsum(scores[k] * _SCORE_WEIGHTS[k] for k in _SCORE_WEIGHTS)

    logger.debug(
        "fingerprint_comparison",
        scores=scores,
        final_score=final_score,
        has_changes=bool(details),
    )

    return final_score, details


def load_baseline(source: Fonte, baselines_dir: str | Path = ".structures") -> Fingerprint | None:
    import json

    path = Path(baselines_dir) / f"{source.value}_baseline.json"

    if not path.exists():
        path = Path(baselines_dir) / "baseline.json"
        if not path.exists():
            return None

    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)

        if "sources" in data and source.value in data["sources"]:
            source_data = data["sources"][source.value]
            return Fingerprint.model_validate(source_data)

        return Fingerprint.model_validate(data)
    except Exception as e:
        logger.warning("baseline_load_failed", source=source.value, error=str(e))
        return None


def save_baseline(
    fingerprint: Fingerprint,
    baselines_dir: str | Path = ".structures",
) -> None:
    import json

    path = Path(baselines_dir) / f"{fingerprint.source.value}_baseline.json"
    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, "w") as f:
        json.dump(fingerprint.model_dump(mode="json"), f, indent=2, default=str)

    logger.info("baseline_saved", source=fingerprint.source.value, path=str(path))


def validate_against_baseline(
    current: Fingerprint,
    baselines_dir: str | Path = ".structures",
) -> StructuralValidationResult:
    baseline = load_baseline(current.source, baselines_dir)

    if baseline is None:
        return StructuralValidationResult(
            source=current.source,
            similarity=1.0,
            passed=True,
            level="unknown",
            differences={},
            current_fingerprint=current,
            baseline_fingerprint=None,
            message="No baseline found - treating as valid",
        )

    return validate_structure(current, baseline)
