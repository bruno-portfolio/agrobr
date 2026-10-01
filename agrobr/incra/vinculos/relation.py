from __future__ import annotations

import re
import sys
from collections import Counter, defaultdict
from collections.abc import Iterator
from typing import Any

import pandas as pd

from agrobr import constants
from agrobr.exceptions import SourceUnavailableError

from . import budget, models

_TEXTO = pd.Series([""]).dtype


def integer_columns() -> tuple[str, ...]:
    return (
        "perimetro_posicao",
        "perimetro_referencia_posicao",
        "administrativo_referencia_posicao",
        "ocorrencias_perimetro_referencia",
        "ocorrencias_administrativo_referencia",
        "perimetro_codigo",
        "perimetro_familias",
        "administrativo_numero_publicado",
    )


def temporal_columns() -> dict[str, str]:
    return {f"perimetro_{name}": dtype for name, dtype in constants.INCRA_DTYPES_TEMPORAIS.items()}


def string_columns() -> tuple[str, ...]:
    excluded = {
        *integer_columns(),
        *temporal_columns(),
        "perimetro_area_ha",
        "referencia_repetida",
    }
    return tuple(name for name in constants.INCRA_VINCULOS_COLUMNS if name not in excluded)


def _cell(value: Any, position: int) -> models.CellEvidence:
    original = None if pd.isna(value) else value
    text = original or ""
    references = []
    residues = []
    cursor = 0
    for ordinal, match in enumerate(
        re.finditer(constants.INCRA_VINCULOS_REFERENCE_PATTERN, text), 1
    ):
        if text[cursor : match.start()].strip():
            residues.append(
                models.Span(start=cursor, end=match.start(), text=text[cursor : match.start()])
            )
        references.append(
            models.Reference(
                start=match.start(), end=match.end(), text=match.group(), ordinal=ordinal
            )
        )
        cursor = match.end()
    if text[cursor:].strip():
        residues.append(models.Span(start=cursor, end=len(text), text=text[cursor:]))
    return models.CellEvidence(
        position=position,
        original=original,
        references=references,
        residues=residues,
        state="recognized" if references else "unrecognized" if text.strip() else "absent",
    )


def _inventory(frame: pd.DataFrame, retained: int) -> tuple[list[models.CellEvidence], int]:
    evidence = []
    for position, value in enumerate(frame["processo"], 1):
        item = _cell(value, position)
        retained = budget.check(retained + budget.retained_size(item) + 8)
        evidence.append(item)
    return evidence, retained


def _indexes(evidence: list[models.CellEvidence]) -> dict[str, list[tuple[int, int]]]:
    result: dict[str, list[tuple[int, int]]] = defaultdict(list)
    for cell in evidence:
        for reference in cell.references:
            result[reference.text].append((cell.position, reference.ordinal))
    return result


def _row(
    left: models.CellEvidence | None,
    right: models.CellEvidence | None,
    left_ordinal: int | None,
    right_ordinal: int | None,
    left_index: dict[str, list[tuple[int, int]]],
    right_index: dict[str, list[tuple[int, int]]],
) -> list[Any]:
    cell = left if left is not None else right
    assert cell is not None
    ordinal = left_ordinal if left is not None else right_ordinal
    token = cell.references[ordinal - 1].text if ordinal is not None else None
    literal = token if token is not None else cell.original
    recognized = ordinal is not None
    left_count = len(left_index.get(token, [])) if token is not None else None
    right_count = len(right_index.get(token, [])) if token is not None else None
    state = (
        "vinculo_exato"
        if left and right
        else "sem_referencia_administrativa"
        if left
        else "sem_referencia_geografica"
    )
    if not recognized:
        state = "referencia_ausente" if cell.state == "absent" else "referencia_nao_reconhecida"
    return [
        state,
        "nup_literal"
        if recognized
        else "ausente"
        if cell.state == "absent"
        else "texto_nao_reconhecido",
        literal,
        left.position if left else None,
        left_ordinal,
        right_ordinal,
        left_count,
        right_count,
        max(left_count or 0, right_count or 0) > 1 if recognized else None,
    ]


def _edges(
    left: list[models.CellEvidence],
    right: list[models.CellEvidence],
    left_index: dict[str, list[tuple[int, int]]],
    right_index: dict[str, list[tuple[int, int]]],
) -> Iterator[
    tuple[models.CellEvidence | None, models.CellEvidence | None, int | None, int | None]
]:
    for cell in left:
        if not cell.references:
            yield cell, None, None, None
        for reference in cell.references:
            matches = right_index.get(reference.text, [])
            if not matches:
                yield cell, None, reference.ordinal, None
            for position, ordinal in matches:
                yield cell, right[position - 1], reference.ordinal, ordinal
    for cell in right:
        if not cell.references:
            yield None, cell, None, None
        for reference in cell.references:
            if reference.text not in left_index:
                yield None, cell, None, reference.ordinal


def _typed_frame(columns: dict[str, list[Any]]) -> pd.DataFrame:
    frame = pd.DataFrame(columns, columns=constants.INCRA_VINCULOS_COLUMNS)
    temporais: dict[str, Any] = temporal_columns()
    for name in frame.columns:
        dtype = (
            "Int64"
            if name in integer_columns()
            else "float64"
            if name == "perimetro_area_ha"
            else "boolean"
            if name == "referencia_repetida"
            else temporais.get(name, _TEXTO)
        )
        frame[name] = pd.Series(columns[name], dtype=dtype)
    return frame


def _planned_rows(
    left: list[models.CellEvidence],
    right: list[models.CellEvidence],
    left_index: dict[str, list[tuple[int, int]]],
    right_index: dict[str, list[tuple[int, int]]],
    max_rows: int | None,
) -> int:
    expected = sum(
        len(occurrences) * max(1, len(right_index.get(token, [])))
        for token, occurrences in left_index.items()
    )
    expected += sum(
        len(occurrences) for token, occurrences in right_index.items() if token not in left_index
    )
    expected += sum(not cell.references for cell in [*left, *right])
    if max_rows is not None and expected > max_rows:
        raise SourceUnavailableError(
            source="incra_vinculos",
            last_error=f"Expansão de {expected} linhas excede max_vinculos={max_rows}",
        )
    return expected


def _materialize(
    geographical: pd.DataFrame,
    administrative: pd.DataFrame,
    left: list[models.CellEvidence],
    right: list[models.CellEvidence],
    left_index: dict[str, list[tuple[int, int]]],
    right_index: dict[str, list[tuple[int, int]]],
    expected: int,
    retained: int,
) -> models.MaterializedRelation:
    columns: dict[str, list[Any]] = {name: [] for name in constants.INCRA_VINCULOS_COLUMNS}
    states: Counter[str] = Counter()
    linked_left: set[int] = set()
    linked_right: set[int] = set()
    represented_left: set[int] = set()
    represented_right: set[int] = set()
    for first, second, first_ordinal, second_ordinal in _edges(
        left, right, left_index, right_index
    ):
        values = _row(first, second, first_ordinal, second_ordinal, left_index, right_index)
        for cell, source, names, represented in (
            (first, geographical, constants.INCRA_COLUMNS, represented_left),
            (second, administrative, constants.INCRA_ANDAMENTO_COLUMNS, represented_right),
        ):
            if cell is None:
                values.extend([None] * len(names))
            else:
                represented.add(cell.position)
                source_row = source.iloc[cell.position - 1]
                values.extend(source_row[name] for name in names)
        retained = budget.check(retained + sum(sys.getsizeof(value) + 16 for value in values))
        for name, value in zip(constants.INCRA_VINCULOS_COLUMNS, values, strict=True):
            columns[name].append(value)
        states[values[0]] += 1
        if first is not None and second is not None:
            linked_left.add(first.position)
            linked_right.add(second.position)
    if (
        sum(states.values()) != expected
        or len(represented_left) != len(geographical)
        or len(represented_right) != len(administrative)
    ):
        raise ValueError("Materialização não preservou ocorrências")
    frame = _typed_frame(columns)
    retained = budget.check(
        retained
        + budget.retained_size(frame)
        + budget.retained_size([linked_left, linked_right, represented_left, represented_right])
    )
    return models.MaterializedRelation(
        frame=frame,
        states=dict(states),
        geographical_rows_represented=len(represented_left),
        administrative_rows_represented=len(represented_right),
        geographical_rows_linked=len(linked_left),
        administrative_rows_linked=len(linked_right),
        retained_bytes_estimate=retained,
    )


def _coverage(
    materialized: models.MaterializedRelation,
    left_index: dict[str, list[tuple[int, int]]],
    right_index: dict[str, list[tuple[int, int]]],
) -> dict[str, Any]:
    return {
        "geographical_rows": materialized.geographical_rows_represented,
        "administrative_rows": materialized.administrative_rows_represented,
        "geographical_tokens": sum(len(items) for items in left_index.values()),
        "administrative_tokens": sum(len(items) for items in right_index.values()),
        "output_rows": len(materialized.frame),
        **materialized.model_dump(exclude={"retained_bytes_estimate"}),
        "cardinalities": {
            token: {
                "geographical_occurrences": len(left_index.get(token, [])),
                "administrative_occurrences": len(right_index.get(token, [])),
            }
            for token in sorted(left_index.keys() | right_index.keys())
        },
    }


def build_relation(
    geographical: pd.DataFrame,
    administrative: pd.DataFrame,
    *,
    max_rows: int | None,
    retained_base_bytes: int = 0,
) -> models.RelationResult:
    retained = budget.check(
        retained_base_bytes
        + budget.retained_size(geographical)
        + budget.retained_size(administrative)
    )
    left, retained = _inventory(geographical, retained)
    right, retained = _inventory(administrative, retained)
    left_index, right_index = _indexes(left), _indexes(right)
    retained = budget.check(retained + budget.retained_size([left_index, right_index]))
    expected = _planned_rows(left, right, left_index, right_index, max_rows)
    budget.check(retained + expected * len(constants.INCRA_VINCULOS_COLUMNS) * 16)
    materialized = _materialize(
        geographical, administrative, left, right, left_index, right_index, expected, retained
    )
    counts = _coverage(materialized, left_index, right_index)
    return models.RelationResult(
        frame=materialized.frame,
        evidence={"geographical": left, "administrative": right},
        counts=counts,
        retained_bytes_estimate=budget.check(
            materialized.retained_bytes_estimate + budget.retained_size(counts)
        ),
    )
