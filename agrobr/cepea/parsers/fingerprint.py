"""Fingerprinting de layout para detecção de mudanças."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import structlog
from bs4 import BeautifulSoup

from agrobr.constants import Fonte
from agrobr.models import Fingerprint

logger = structlog.get_logger()


def extract_fingerprint(
    html: str,
    source: Fonte,
    url: str,
) -> Fingerprint:
    """
    Extrai assinatura estrutural do HTML.

    Args:
        html: Conteúdo HTML
        source: Fonte de dados
        url: URL original

    Returns:
        Fingerprint com assinatura estrutural
    """
    soup = BeautifulSoup(html, "lxml")

    table_classes: list[list[str]] = []
    for table in soup.find_all("table")[:10]:
        classes = table.get("class", [])
        if isinstance(classes, str):
            classes = [classes]
        table_classes.append(sorted(classes))

    keywords = ["preco", "indicador", "cotacao", "valor", "tabela", "dados"]
    key_ids: list[str] = []
    for elem in soup.find_all(id=True):
        elem_id = elem.get("id", "").lower()
        if any(kw in elem_id for kw in keywords):
            key_ids.append(elem.get("id"))
    key_ids = sorted(set(key_ids))[:20]

    table_headers: list[list[str]] = []
    for table in soup.find_all("table")[:5]:
        headers: list[str] = []
        for th in table.find_all("th"):
            text = th.get_text(strip=True)[:50]
            if text:
                headers.append(text)
        if headers:
            table_headers.append(headers)

    element_counts = {
        "tables": len(soup.find_all("table")),
        "forms": len(soup.find_all("form")),
        "divs_with_id": len(soup.find_all("div", id=True)),
        "inputs": len(soup.find_all("input")),
        "selects": len(soup.find_all("select")),
        "links": len(soup.find_all("a")),
        "scripts": len(soup.find_all("script")),
    }

    structure_elements: list[tuple[str, int, tuple[str, ...]]] = []
    for tag in soup.find_all(["table", "div", "form", "section", "article"])[:30]:
        tag_classes = tag.get("class", [])
        if isinstance(tag_classes, str):
            tag_classes = [tag_classes]
        structure_elements.append(
            (
                tag.name,
                len(tag.find_all(recursive=False)),
                tuple(sorted(tag_classes))[:3] if tag_classes else (),
            )
        )

    structure_hash = hashlib.md5(str(structure_elements).encode()).hexdigest()[:12]

    return Fingerprint(
        source=source,
        url=url,
        collected_at=datetime.utcnow(),
        table_classes=table_classes,
        key_ids=key_ids,
        structure_hash=structure_hash,
        table_headers=table_headers,
        element_counts=element_counts,
    )


def compare_fingerprints(
    current: Fingerprint,
    reference: Fingerprint,
) -> tuple[float, dict[str, Any]]:
    """
    Compara duas fingerprints e retorna similaridade.

    Args:
        current: Fingerprint atual
        reference: Fingerprint de referência (baseline)

    Returns:
        tuple[float, dict]: (similaridade 0-1, detalhes das diferenças)
    """
    scores: dict[str, float] = {}
    details: dict[str, Any] = {}

    scores["structure"] = 1.0 if current.structure_hash == reference.structure_hash else 0.0
    if scores["structure"] == 0:
        details["structure_changed"] = {
            "current": current.structure_hash,
            "reference": reference.structure_hash,
        }

    if reference.table_classes:
        matches = sum(1 for tc in current.table_classes if tc in reference.table_classes)
        scores["table_classes"] = matches / len(reference.table_classes)
        if scores["table_classes"] < 1.0:
            details["table_classes_diff"] = {
                "missing": [
                    tc for tc in reference.table_classes if tc not in current.table_classes
                ],
                "new": [tc for tc in current.table_classes if tc not in reference.table_classes],
            }
    else:
        scores["table_classes"] = 1.0

    if reference.key_ids:
        matches = sum(1 for kid in reference.key_ids if kid in current.key_ids)
        scores["key_ids"] = matches / len(reference.key_ids)
        if scores["key_ids"] < 1.0:
            details["key_ids_diff"] = {
                "missing": [kid for kid in reference.key_ids if kid not in current.key_ids],
                "new": [kid for kid in current.key_ids if kid not in reference.key_ids],
            }
    else:
        scores["key_ids"] = 1.0

    if reference.table_headers:
        header_score = 0.0
        for ref_headers in reference.table_headers:
            for cur_headers in current.table_headers:
                ref_set = set(ref_headers)
                cur_set = set(cur_headers)
                if ref_set or cur_set:
                    jaccard = len(ref_set & cur_set) / len(ref_set | cur_set)
                    header_score = max(header_score, jaccard)
        scores["table_headers"] = header_score
        if scores["table_headers"] < 0.9:
            details["table_headers_diff"] = {
                "reference": reference.table_headers,
                "current": current.table_headers,
            }
    else:
        scores["table_headers"] = 1.0

    count_diffs: dict[str, dict[str, int]] = {}
    for key in reference.element_counts:
        ref_count = reference.element_counts.get(key, 0)
        cur_count = current.element_counts.get(key, 0)
        if ref_count > 0:
            diff_ratio = abs(cur_count - ref_count) / ref_count
            if diff_ratio > 0.5:
                count_diffs[key] = {"reference": ref_count, "current": cur_count}

    if count_diffs:
        scores["element_counts"] = max(0, 1 - len(count_diffs) * 0.2)
        details["element_counts_diff"] = count_diffs
    else:
        scores["element_counts"] = 1.0

    weights = {
        "structure": 0.25,
        "table_classes": 0.20,
        "key_ids": 0.15,
        "table_headers": 0.30,
        "element_counts": 0.10,
    }

    final_score = sum(scores[k] * weights[k] for k in weights)

    logger.debug(
        "fingerprint_comparison",
        scores=scores,
        final_score=final_score,
        has_changes=bool(details),
    )

    return final_score, details


def save_baseline_fingerprint(fingerprint: Fingerprint, path: str) -> None:
    """Salva fingerprint como baseline de referência."""
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(fingerprint.model_dump(mode="json"), f, indent=2, default=str)


def load_baseline_fingerprint(path: str) -> Fingerprint | None:
    """Carrega fingerprint de referência."""
    if not Path(path).exists():
        return None

    with open(path, encoding="utf-8") as f:
        data = json.load(f)
        return Fingerprint.model_validate(data)
