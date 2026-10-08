from __future__ import annotations

from typing import Any

import pandas as pd

from agrobr import constants, contracts

_DTYPES = {
    contracts.ColumnType.INTEGER: "Int64",
    contracts.ColumnType.FLOAT: "float64",
    contracts.ColumnType.DATE: "datetime64[ns]",
    contracts.ColumnType.DATETIME: "datetime64[ns, UTC]",
}


class IncraContract(contracts.Contract):
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
        expected = self.list_columns()
        if list(df) not in (expected, [*expected, "geometry"]):
            errors.append("Columns must follow the complete declared attribute order")
        for column in self.columns:
            wanted = _DTYPES.get(column.type)
            if column.name in df and wanted is not None and str(df[column.name].dtype) != wanted:
                errors.append(f"Column '{column.name}' must use {wanted} dtype")
        if errors:
            return False, errors
        if any(not value.strip() for value in df["feature_id"]):
            errors.append("Feature.id requires nonblank received text without a uniqueness claim")
        return not errors, errors

    def to_dict(self) -> dict[str, Any]:
        schema = super().to_dict()
        schema["constraints"].update(
            attribute_order=self.list_columns(),
            optional_geometry_column="geometry_after_all_attributes",
            source_property_projection=list(constants.INCRA_PROPERTIES),
            column_aliases=dict(constants.INCRA_RENAME_MAP),
            string_dtype="default pandas text dtype (str on pandas 3, object on pandas 2)",
            date_dtype="datetime64[ns]",
            datetime_dtype="datetime64[ns, UTC]",
            integer_dtype="Int64",
            float_dtype="float64",
            integer_source_bits=dict(constants.INCRA_INTEGER_BITS),
            source_missing_member="unsupported_projection_raises_ParseError",
            source_null="preserved_when_XSD_nillable_not_equivalent_to_missing_member",
            source_extra_member="layout_drift_raises_ParseError",
            text_semantics="literal_published_text_without_boolean_or_UF_coercion",
            date_source_fields=sorted(constants.INCRA_DATE_PROPERTIES),
            datetime_source_fields=sorted(constants.INCRA_DATETIME_PROPERTIES),
            temporal_semantics="XSD_1_0_date_and_dateTime_validated_locally_then_date_as_datetime64_ns_and_dateTime_as_UTC_offsetless_read_as_UTC_unreadable_date_becomes_NaT_with_warning",
            temporal_protocol_restriction="external_whitespace_rejected_before_local_XMLSchema_validation_without_stripping_output",
            registration_date="required_published_datetime_as_UTC_instant_not_an_immutable_edition",
            feature_id="required_nonblank_received_string_by_acquisition_policy_not_XSD_primary_key",
            identifier_semantics="zero_repeated_and_null_codes_preserved_without_primary_key",
            continuity_basis="all_published_properties_and_acquired_geometry_with_ID_changes_reported_separately",
            integer_semantics="exact_integral_JSON_number_in_signed32_source_domain_without_float_intermediate",
            float_semantics="finite_binary64_signed_zero_preserved_overflow_and_nonzero_underflow_rejected",
            area_semantics="published_hectares_without_recalculation_or_positive_domain_inference",
            geometry_semantics="received_coordinates_in_verified_requested_CRS_without_reprojection_or_topology_repair",
        )
        return schema


class AndamentoContract(contracts.Contract):
    def validate(self, df: pd.DataFrame) -> tuple[bool, list[str]]:
        _, errors = super().validate(df)
        if not df.columns.is_unique:
            return False, errors
        if list(df) != self.list_columns():
            errors.append("Administrative columns must follow the complete publication order")
        if "numero_publicado" in df and str(df["numero_publicado"].dtype) != "Int64":
            errors.append("Published ordinal must use Int64 dtype")
        if errors:
            return False, errors
        if any(int(value) != expected for expected, value in enumerate(df["numero_publicado"], 1)):
            errors.append("Published ordinals must be consecutive from one in publication order")
        if any(not value.strip() for value in df["regional"]):
            errors.append("Administrative regional group requires nonblank published text")
        return not errors, errors

    def to_dict(self) -> dict[str, Any]:
        schema = super().to_dict()
        schema["constraints"].update(
            attribute_order=self.list_columns(),
            string_dtype="default pandas text dtype (str on pandas 3, object on pandas 2)",
            integer_dtype="Int64",
            source_null="not_inferred_from_empty_PDF_cells",
            text_semantics="published_cell_text_with_line_breaks_empty_strings_and_partially_clipped_visible_text_preserved",
            temporal_semantics="administrative_cells_may_contain_dates_multiple_acts_annotations_or_empty_text",
            numerical_semantics="area_and_families_remain_published_text_without_numeric_inference",
            regional_semantics="published_graphical_group_not_inferred_from_UF",
            ordinal_semantics="positive_consecutive_publication_position_scoped_to_PDF_hash_not_semantic_primary_key",
            identity_semantics="process_references_may_repeat_or_contain_multiple_values",
            edition_semantics="publisher_link_date_reconciled_to_internal_PDF_date_URL_not_immutable",
            coverage_semantics="all_publication_rows_reconciled_to_declared_total_without_summing_overlapping_phase_counts",
        )
        return schema


def _geographic_column(name: str) -> contracts.Column:
    if name in {"codigo", "familias"}:
        return contracts.Column(
            name,
            contracts.ColumnType.INTEGER,
            nullable=True,
            min_value=-(2**31),
            max_value=2**31 - 1,
        )
    if name == "area_ha":
        return contracts.Column(name, contracts.ColumnType.FLOAT, nullable=True, unit="ha")
    if name == "data_cadastro":
        return contracts.Column(name, contracts.ColumnType.DATETIME, nullable=True)
    if name in constants.INCRA_DTYPES_TEMPORAIS:
        return contracts.Column(name, contracts.ColumnType.DATE, nullable=True)
    return contracts.Column(name, contracts.ColumnType.STRING, nullable=name != "feature_id")


QUILOMBOLAS_V2 = IncraContract(
    name="incra.quilombolas",
    version="2.0",
    effective_from="2.0.0",
    primary_key=[],
    columns=[_geographic_column(name) for name in constants.INCRA_COLUMNS],
    guarantees=[
        "Todos os 21 atributos publicados e Feature.id permanecem recuperáveis nas 22 colunas",
        "Datas publicadas saem em datetime64[ns] e o cadastro em datetime64[ns, UTC], validados como XSD na entrada",
        "Null, zero, string vazia e texto NULL não são intercambiáveis",
        "Código, processo e Feature.id não constituem chaves primárias",
        "Ocorrências repetidas não são eliminadas por identificador",
    ],
)

ANDAMENTO_QUILOMBOLA_V1 = AndamentoContract(
    name="incra.andamento_quilombola",
    version="1.0",
    effective_from="2.0.0",
    primary_key=[],
    columns=[
        contracts.Column(name, contracts.ColumnType.INTEGER, min_value=1)
        if name == "numero_publicado"
        else contracts.Column(name, contracts.ColumnType.STRING)
        for name in constants.INCRA_ANDAMENTO_COLUMNS
    ],
    guarantees=[
        "As 15 posições da publicação são preservadas com ordem e proveniência de edição",
        "Texto, células vazias, anotações e múltiplos atos permanecem recuperáveis",
        "A regional é atribuída pelo agrupamento gráfico publicado",
        "Processos repetidos não eliminam registros ou estabelecem chave primária",
        "A quantidade de registros é conciliada ao total declarado da publicação",
    ],
)

contracts.register_contract("incra_quilombolas", QUILOMBOLAS_V2)
contracts.register_contract("incra_andamento_quilombola", ANDAMENTO_QUILOMBOLA_V1)
