from __future__ import annotations

import re
from collections import Counter
from collections.abc import Hashable, Mapping
from dataclasses import replace
from typing import Any

import pandas as pd

from agrobr import constants, contracts
from agrobr.contracts import incra

ReferenceKey = tuple[int, int | None]
ReferenceValue = tuple[str, str | None]


def _literal(value: Any) -> Any:
    return None if pd.isna(value) else value


def _source_frame(
    frame: pd.DataFrame, prefix: str, identity: str, parent: contracts.Contract
) -> tuple[pd.DataFrame, list[str]]:
    names = [prefix + name for name in parent.list_columns()]
    present = frame[identity].notna()
    errors = []
    if frame.loc[~present, names].notna().any().any():
        errors.append(f"Absent {prefix} side must contain only null source attributes")
    occurrences: dict[int, tuple[tuple[Any, ...], int]] = {}
    for index, row in enumerate(frame[[identity, *names]].itertuples(index=False, name=None)):
        if pd.isna(row[0]):
            continue
        position = int(row[0])
        signature = tuple(
            None if pd.isna(value) else float(value).hex() if name == "perimetro_area_ha" else value
            for name, value in zip(names, row[1:], strict=True)
        )
        if position in occurrences and occurrences[position][0] != signature:
            errors.append(f"Conflicting {prefix} source attributes at position {position}")
            break
        occurrences.setdefault(position, (signature, index))
    if sorted(occurrences) != list(range(1, len(occurrences) + 1)):
        errors.append(f"Represented {prefix} source positions must be consecutive from one")
    selected = [occurrences[position][1] for position in sorted(occurrences)]
    source = frame.iloc[selected][names].rename(
        columns=dict(zip(names, parent.list_columns(), strict=True))
    )
    valid, parent_errors = parent.validate(source)
    if not valid:
        errors.extend(f"{prefix} parent: {error}" for error in parent_errors)
    return source, errors


def _references(frame: pd.DataFrame) -> dict[ReferenceKey, ReferenceValue]:
    output: dict[ReferenceKey, ReferenceValue] = {}
    pattern = re.compile(constants.INCRA_VINCULOS_REFERENCE_PATTERN)
    for position, raw in enumerate(frame["processo"], 1):
        value = _literal(raw)
        matches = list(pattern.finditer(value or ""))
        if matches:
            output.update(
                ((position, ordinal), ("nup_literal", match.group()))
                for ordinal, match in enumerate(matches, 1)
            )
        else:
            kind = "ausente" if value is None or not value.strip() else "texto_nao_reconhecido"
            output[(position, None)] = (kind, value)
    return output


def _reference_counts(references: dict[ReferenceKey, ReferenceValue]) -> Counter[str]:
    return Counter(
        literal
        for kind, literal in references.values()
        if kind == "nup_literal" and literal is not None
    )


def _row_reference(
    row: Mapping[Hashable, Any],
    left: ReferenceValue | None,
    right: ReferenceValue | None,
    left_counts: Counter[str],
    right_counts: Counter[str],
) -> list[str]:
    reference = left if left is not None else right
    if reference is None or (left is not None and right is not None and left != right):
        return [
            "Relation must identify existing source reference occurrences with equal literal values"
        ]
    kind, literal = reference
    if (row["referencia_tipo"], _literal(row["referencia_literal"])) != reference:
        return ["Relation type/literal differs from its source occurrence"]
    if kind == "nup_literal":
        if literal is None:
            return ["Recognized reference requires a literal"]
        count_left, count_right = left_counts[literal], right_counts[literal]
        state = (
            "vinculo_exato"
            if count_left and count_right
            else "sem_referencia_administrativa"
            if count_left
            else "sem_referencia_geografica"
        )
        expected = (count_left, count_right, count_left > 1 or count_right > 1)
        actual = tuple(
            _literal(row[name])
            for name in (
                "ocorrencias_perimetro_referencia",
                "ocorrencias_administrativo_referencia",
                "referencia_repetida",
            )
        )
        if actual != expected:
            return ["Reference counters/multiplicity differ from represented source occurrences"]
        if bool(count_left) != (left is not None) or bool(count_right) != (right is not None):
            return ["Recognized reference omits an available counterpart"]
    else:
        state = "referencia_ausente" if kind == "ausente" else "referencia_nao_reconhecida"
        if left is not None and right is not None:
            return ["Unrecognized or absent references cannot create an association"]
        if any(
            pd.notna(row[name])
            for name in (
                "ocorrencias_perimetro_referencia",
                "ocorrencias_administrativo_referencia",
                "referencia_repetida",
            )
        ):
            return ["Unrecognized or absent references require null counters/multiplicity"]
    return (
        []
        if row["estado_vinculo"] == state
        else ["Relation state differs from reference/counterpart availability"]
    )


def _validate_relation(
    frame: pd.DataFrame, geographical: pd.DataFrame, administrative: pd.DataFrame
) -> list[str]:
    left, right = _references(geographical), _references(administrative)
    left_counts, right_counts = _reference_counts(left), _reference_counts(right)
    seen_left: set[ReferenceKey] = set()
    seen_right: set[ReferenceKey] = set()
    pairs: set[tuple[Any, ...]] = set()
    emitted: Counter[str] = Counter()
    previous: tuple[int, ...] | None = None
    for row in frame.to_dict("records"):
        lp, li, rp, ri = (
            _literal(row[name])
            for name in (
                "perimetro_posicao",
                "perimetro_referencia_posicao",
                "administrativo_numero_publicado",
                "administrativo_referencia_posicao",
            )
        )
        if (lp is None and li is not None) or (rp is None and ri is not None):
            return ["Reference position requires its source side"]
        left_key = (lp, li) if lp is not None else None
        right_key = (rp, ri) if rp is not None else None
        if (left_key is not None and left_key not in left) or (
            right_key is not None and right_key not in right
        ):
            return ["Reference ordinal does not identify a published token occurrence"]
        errors = _row_reference(
            row,
            left.get(left_key) if left_key is not None else None,
            right.get(right_key) if right_key is not None else None,
            left_counts,
            right_counts,
        )
        if errors:
            return errors
        pair = (lp, li, rp, ri)
        if pair in pairs:
            return ["A source-occurrence pair was emitted more than once"]
        pairs.add(pair)
        if left_key is not None:
            seen_left.add(left_key)
        if right_key is not None:
            seen_right.add(right_key)
        if row["referencia_tipo"] == "nup_literal":
            emitted[row["referencia_literal"]] += 1
        order = (0, lp, li or 0, rp or 0, ri or 0) if lp is not None else (1, rp, ri or 0, 0, 0)
        if previous is not None and order < previous:
            return ["Relation rows must follow source occurrence/token order"]
        previous = order
    if seen_left != set(left) or seen_right != set(right):
        return ["Relation does not represent every reference occurrence of its source rows"]
    expected = Counter(
        {
            literal: left_counts[literal] * right_counts[literal]
            if left_counts[literal] and right_counts[literal]
            else left_counts[literal] + right_counts[literal]
            for literal in left_counts.keys() | right_counts.keys()
        }
    )
    return [] if emitted == expected else ["Relation omits one or more exact occurrence pairs"]


_DTYPES = {
    contracts.ColumnType.INTEGER: "Int64",
    contracts.ColumnType.FLOAT: "float64",
    contracts.ColumnType.BOOLEAN: "boolean",
    contracts.ColumnType.DATE: "datetime64[ns]",
    contracts.ColumnType.DATETIME: "datetime64[ns, UTC]",
}


class VinculosContract(contracts.Contract):
    def empty_frame(self) -> pd.DataFrame:
        frame = super().empty_frame()
        for column in self.columns:
            if column.type == contracts.ColumnType.DATETIME:
                frame[column.name] = pd.Series(dtype=_DTYPES[column.type])
        return frame

    def validate(self, df: pd.DataFrame) -> tuple[bool, list[str]]:
        _, errors = super().validate(df)
        if not df.columns.is_unique:
            return False, errors
        if list(df) != self.list_columns():
            errors.append("Relation columns must follow the complete declared order")
        for column in self.columns:
            wanted = _DTYPES.get(column.type)
            if column.name in df and wanted is not None and str(df[column.name].dtype) != wanted:
                errors.append(f"Column '{column.name}' must use {wanted} dtype")
        if errors:
            return False, errors
        geographical, geo_errors = _source_frame(
            df, "perimetro_", "perimetro_posicao", incra.QUILOMBOLAS_V2
        )
        administrative, admin_errors = _source_frame(
            df, "administrativo_", "administrativo_numero_publicado", incra.ANDAMENTO_QUILOMBOLA_V1
        )
        errors.extend([*geo_errors, *admin_errors])
        if not errors:
            errors.extend(_validate_relation(df, geographical, administrative))
        return not errors, errors

    def to_dict(self) -> dict[str, Any]:
        schema = super().to_dict()
        schema["constraints"].update(
            attribute_order=self.list_columns(),
            string_dtype="default pandas text dtype (str on pandas 3, object on pandas 2)",
            integer_dtype="Int64",
            float_dtype="float64",
            boolean_dtype="boolean",
            date_dtype="datetime64[ns]",
            datetime_dtype="datetime64[ns, UTC]",
            parent_contracts={"incra_quilombolas": "2.0", "incra_andamento_quilombola": "1.0"},
            reference_pattern=constants.INCRA_VINCULOS_REFERENCE_PATTERN,
            reference_semantics="exact_published_NUP_lexemes_without_punctuation_repair_or_check_digit_validation",
            reference_kinds=["nup_literal", "texto_nao_reconhecido", "ausente"],
            relation_states=[
                "vinculo_exato",
                "sem_referencia_administrativa",
                "sem_referencia_geografica",
                "referencia_nao_reconhecida",
                "referencia_ausente",
            ],
            association_semantics="shared_documentary_reference_not_territorial_identity",
            multiplicity="all_occurrence_pairs_and_unmatched_occurrences_without_deduplication",
            position_semantics="source_acquisition_and_PDF_hash_scoped_occurrences_not_stable_primary_keys",
            absence_semantics="no_counterpart_in_this_pair_of_acquisitions_not_institutional_absence",
            provenance_semantics="independent_complete_parent_metadata_and_editions_without_common_revision_snapshot",
            source_columns="all_22_geographic_and_15_administrative_attributes_preserved_with_prefixes",
            source_null="extra_nulls_only_when_the_entire_source_side_is_absent",
            output_order="geographic_position_token_then_administrative_ordinal_token_with_administrative_only_occurrences_last",
        )
        return schema


def _columns() -> list[contracts.Column]:
    relational = []
    for name in constants.INCRA_VINCULOS_COLUMNS[:9]:
        if name in {"estado_vinculo", "referencia_tipo", "referencia_literal"}:
            relational.append(
                contracts.Column(
                    name, contracts.ColumnType.STRING, nullable=name == "referencia_literal"
                )
            )
        elif name == "referencia_repetida":
            relational.append(contracts.Column(name, contracts.ColumnType.BOOLEAN, nullable=True))
        else:
            relational.append(
                contracts.Column(
                    name,
                    contracts.ColumnType.INTEGER,
                    nullable=True,
                    min_value=0 if name.startswith("ocorrencias_") else 1,
                )
            )
    for prefix, parent in (
        ("perimetro_", incra.QUILOMBOLAS_V2),
        ("administrativo_", incra.ANDAMENTO_QUILOMBOLA_V1),
    ):
        relational.extend(
            replace(column, name=prefix + column.name, nullable=True) for column in parent.columns
        )
    return relational


VINCULOS_QUILOMBOLAS_V1 = VinculosContract(
    name="incra.vinculos_quilombolas",
    version="1.0",
    effective_from="2.0.0",
    primary_key=[],
    columns=_columns(),
    guarantees=[
        "Referências processuais literais associam todas as ocorrências correspondentes",
        "Registros sem contraparte e referências não reconhecidas continuam presentes",
        "Todos os atributos de ambos os canais são preservados em colunas distintas",
        "Código, processo, Feature.id e posição publicada não viram identidade territorial",
        "A relação conserva proveniências e edições independentes dos dois canais",
    ],
)

contracts.register_contract("incra_vinculos_quilombolas", VINCULOS_QUILOMBOLAS_V1)
