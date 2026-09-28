from __future__ import annotations

import math
from typing import Any

import pandas as pd

from agrobr import constants, contracts


def _source_name(name: str) -> str:
    return next((raw for raw, alias in constants.FUNAI_RENAME_MAP.items() if alias == name), name)


class FunaiContract(contracts.Contract):
    def empty_frame(self) -> pd.DataFrame:
        frame = super().empty_frame()
        for column in self.columns:
            if column.type == contracts.ColumnType.STRING:
                frame[column.name] = pd.Series(dtype=pd.StringDtype(storage="python"))
            elif column.type == contracts.ColumnType.FLOAT:
                frame[column.name] = pd.Series(dtype="float64")
        return frame

    def validate(self, df: pd.DataFrame) -> tuple[bool, list[str]]:
        _, errors = super().validate(df)
        if not df.columns.is_unique:
            return False, errors
        expected = self.list_columns()
        if list(df) not in (expected, [*expected, "geometry"]):
            errors.append("Columns must follow the complete declared attribute order")
        for column in self.columns:
            if column.name not in df:
                continue
            dtype = df[column.name].dtype
            if column.type == contracts.ColumnType.STRING:
                if (
                    not isinstance(dtype, pd.StringDtype)
                    or dtype.storage != "python"
                    or str(dtype) != "string"
                ):
                    errors.append(f"Column '{column.name}' must use StringDtype python with pd.NA")
            else:
                wanted = "Int64" if column.type == contracts.ColumnType.INTEGER else "float64"
                if str(dtype) != wanted:
                    errors.append(f"Column '{column.name}' must use {wanted} dtype")
        if errors:
            return False, errors
        if any(not value.strip() for value in df["feature_id"]):
            errors.append("Feature.id requires nonblank published text without a uniqueness claim")
        for column in self.columns:
            bits = constants.FUNAI_INTEGER_BITS.get(_source_name(column.name))
            if bits is not None and any(
                not -(2 ** (bits - 1)) <= int(value) < 2 ** (bits - 1)
                for value in df[column.name].dropna()
            ):
                errors.append(f"Column '{column.name}' exceeds its signed{bits} source domain")
        if any(not math.isfinite(value) for value in df["area_ha"].dropna()):
            errors.append("Published area must be finite when present")
        return not errors, errors

    def to_dict(self) -> dict[str, Any]:
        schema = super().to_dict()
        schema["constraints"].update(
            attribute_order=self.list_columns(),
            optional_geometry_column="geometry_after_all_attributes",
            source_property_projection=list(constants.FUNAI_PROPERTIES),
            column_aliases=dict(constants.FUNAI_RENAME_MAP),
            string_dtype="string[python] with pd.NA",
            integer_dtype="Int64",
            float_dtype="float64",
            integer_source_bits=dict(constants.FUNAI_INTEGER_BITS),
            source_missing_member="unsupported_projection_raises_ParseError",
            source_null="preserved_when_XSD_nillable_not_equivalent_to_missing_member",
            source_extra_member="layout_drift_raises_ParseError",
            text_semantics="literal_published_text_without_date_boolean_or_UF_coercion",
            update_date_semantics="literal_text_not_an_immutable_edition_identifier",
            feature_id="required_nonblank_received_string_observed_volatile_between_requests",
            identifier_semantics="published_identifiers_are_not_primary_keys",
            continuity_basis="all_published_properties_and_acquired_geometry_with_id_drift_reported_separately",
            integer_semantics="exact_integral_JSON_number_in_signed_XSD_domain_without_float_intermediate",
            float_semantics="finite_binary64_signed_zero_preserved_overflow_and_nonzero_underflow_rejected",
            area_semantics="published_hectares_without_recalculation_or_positive_domain_inference",
            geometry_semantics="received_coordinates_in_verified_requested_CRS_without_reprojection_or_topology_repair",
        )
        return schema


def _column(name: str) -> contracts.Column:
    bits = constants.FUNAI_INTEGER_BITS.get(_source_name(name))
    if bits is not None:
        return contracts.Column(
            name,
            contracts.ColumnType.INTEGER,
            nullable=True,
            min_value=-(2 ** (bits - 1)),
            max_value=2 ** (bits - 1) - 1,
        )
    if name == "area_ha":
        return contracts.Column(name, contracts.ColumnType.FLOAT, nullable=True, unit="ha")
    return contracts.Column(name, contracts.ColumnType.STRING, nullable=name != "feature_id")


TERRAS_INDIGENAS_V2 = FunaiContract(
    name="funai.terras_indigenas",
    version="2.0",
    effective_from="2.0.0",
    primary_key=[],
    columns=[_column(name) for name in constants.FUNAI_COLUMNS],
    guarantees=[
        "Todos os 18 atributos publicados permanecem recuperáveis nas 19 colunas",
        "UF, data e indicadores administrativos mantêm o texto literal publicado",
        "Null, string vazia, espaços e texto NULL não são intercambiáveis",
        "Identificadores inteiros são preservados sem conversão intermediária para float",
        "Feature.id é preservado como recebido, inclusive quando varia entre requisições",
        "Identificadores repetidos não eliminam ocorrências nem estabelecem chave primária",
        "Data de atualização publicada não representa edição imutável ou vigência jurídica",
    ],
)

contracts.register_contract("funai_terras_indigenas", TERRAS_INDIGENAS_V2)
