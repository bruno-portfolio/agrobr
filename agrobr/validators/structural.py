"""
Validação estrutural de páginas usando fingerprints.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import structlog

from ..constants import Fonte
from ..models import Fingerprint

logger = structlog.get_logger()

THRESHOLD_HIGH = 0.85
THRESHOLD_MEDIUM = 0.70
THRESHOLD_LOW = 0.50


@dataclass
class StructuralValidationResult:
    """Resultado da validação estrutural."""

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
    _threshold: float = THRESHOLD_HIGH,
) -> StructuralValidationResult:
    """
    Valida estrutura atual contra baseline.

    Args:
        current: Fingerprint atual
        baseline: Fingerprint de referência
        threshold: Limiar de similaridade (default: 0.85)

    Returns:
        StructuralValidationResult
    """
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


def compare_fingerprints(
    current: Fingerprint,
    reference: Fingerprint,
) -> tuple[float, dict[str, Any]]:
    """
    Compara duas fingerprints e retorna similaridade.

    Args:
        current: Fingerprint atual
        reference: Fingerprint de referência

    Returns:
        Tupla (similaridade 0-1, detalhes das diferenças)
    """
    scores = {}
    details = {}

    scores['structure'] = 1.0 if current.structure_hash == reference.structure_hash else 0.0
    if scores['structure'] == 0:
        details['structure_changed'] = {
            'current': current.structure_hash,
            'reference': reference.structure_hash,
        }

    if reference.table_classes:
        matches = sum(
            1 for tc in current.table_classes
            if tc in reference.table_classes
        )
        scores['table_classes'] = matches / len(reference.table_classes)
        if scores['table_classes'] < 1.0:
            details['table_classes_diff'] = {
                'missing': [tc for tc in reference.table_classes if tc not in current.table_classes],
                'new': [tc for tc in current.table_classes if tc not in reference.table_classes],
            }
    else:
        scores['table_classes'] = 1.0

    if reference.key_ids:
        matches = sum(1 for kid in reference.key_ids if kid in current.key_ids)
        scores['key_ids'] = matches / len(reference.key_ids)
        if scores['key_ids'] < 1.0:
            details['key_ids_diff'] = {
                'missing': [kid for kid in reference.key_ids if kid not in current.key_ids],
                'new': [kid for kid in current.key_ids if kid not in reference.key_ids],
            }
    else:
        scores['key_ids'] = 1.0

    if reference.table_headers:
        header_score = 0.0
        for ref_headers in reference.table_headers:
            for cur_headers in current.table_headers:
                ref_set = set(ref_headers)
                cur_set = set(cur_headers)
                if ref_set or cur_set:
                    jaccard = len(ref_set & cur_set) / len(ref_set | cur_set)
                    header_score = max(header_score, jaccard)
        scores['table_headers'] = header_score
        if scores['table_headers'] < 0.9:
            details['table_headers_diff'] = {
                'reference': reference.table_headers,
                'current': current.table_headers,
            }
    else:
        scores['table_headers'] = 1.0

    count_diffs = {}
    for key in reference.element_counts:
        ref_count = reference.element_counts.get(key, 0)
        cur_count = current.element_counts.get(key, 0)
        if ref_count > 0:
            diff_ratio = abs(cur_count - ref_count) / ref_count
            if diff_ratio > 0.5:
                count_diffs[key] = {'reference': ref_count, 'current': cur_count}

    if count_diffs:
        scores['element_counts'] = max(0, 1 - len(count_diffs) * 0.2)
        details['element_counts_diff'] = count_diffs
    else:
        scores['element_counts'] = 1.0

    weights = {
        'structure': 0.25,
        'table_classes': 0.20,
        'key_ids': 0.15,
        'table_headers': 0.30,
        'element_counts': 0.10,
    }

    final_score = sum(scores[k] * weights[k] for k in weights)

    return final_score, details


def load_baseline(source: Fonte, baselines_dir: str | Path = '.structures') -> Fingerprint | None:
    """
    Carrega fingerprint de baseline.

    Args:
        source: Fonte de dados
        baselines_dir: Diretório de baselines

    Returns:
        Fingerprint ou None
    """
    import json

    path = Path(baselines_dir) / f"{source.value}_baseline.json"

    if not path.exists():
        path = Path(baselines_dir) / "baseline.json"
        if not path.exists():
            return None

    try:
        with open(path) as f:
            data = json.load(f)

        if 'sources' in data and source.value in data['sources']:
            source_data = data['sources'][source.value]
            return Fingerprint.model_validate(source_data)

        return Fingerprint.model_validate(data)
    except Exception as e:
        logger.warning("baseline_load_failed", source=source.value, error=str(e))
        return None


def save_baseline(
    fingerprint: Fingerprint,
    baselines_dir: str | Path = '.structures',
) -> None:
    """
    Salva fingerprint como baseline.

    Args:
        fingerprint: Fingerprint a salvar
        baselines_dir: Diretório de baselines
    """
    import json

    path = Path(baselines_dir) / f"{fingerprint.source.value}_baseline.json"
    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, 'w') as f:
        json.dump(fingerprint.model_dump(mode='json'), f, indent=2, default=str)

    logger.info("baseline_saved", source=fingerprint.source.value, path=str(path))


def validate_against_baseline(
    current: Fingerprint,
    baselines_dir: str | Path = '.structures',
    threshold: float = THRESHOLD_HIGH,
) -> StructuralValidationResult:
    """
    Valida fingerprint atual contra baseline salvo.

    Args:
        current: Fingerprint atual
        baselines_dir: Diretório de baselines
        threshold: Limiar de similaridade

    Returns:
        StructuralValidationResult
    """
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

    return validate_structure(current, baseline, threshold)


class StructuralMonitor:
    """Monitor contínuo de estrutura."""

    def __init__(self, baselines_dir: str | Path = '.structures'):
        self.baselines_dir = Path(baselines_dir)
        self.history: list[StructuralValidationResult] = []

    async def check(self, source: Fonte) -> StructuralValidationResult:
        """
        Verifica estrutura de uma fonte.

        Args:
            source: Fonte a verificar

        Returns:
            StructuralValidationResult
        """
        from ..cepea import client as cepea_client
        from ..cepea.parsers.fingerprint import extract_fingerprint

        html = await cepea_client.fetch_indicador_page('soja')
        current = extract_fingerprint(html, source, 'soja')

        result = validate_against_baseline(current, self.baselines_dir)
        self.history.append(result)

        return result

    async def check_all(self) -> list[StructuralValidationResult]:
        """Verifica todas as fontes."""
        import asyncio

        sources = [Fonte.CEPEA]
        results = await asyncio.gather(*[self.check(s) for s in sources])
        return list(results)

    def get_drift_history(self) -> list[StructuralValidationResult]:
        """Retorna histórico de drifts detectados."""
        return [r for r in self.history if not r.passed]
