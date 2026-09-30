from __future__ import annotations

from typing import Any

import pandas as pd

from agrobr import constants, contracts
from agrobr.normalize import regions


class SolosContract(contracts.Contract):
    def validate(self, df: pd.DataFrame) -> tuple[bool, list[str]]:
        _, errors = super().validate(df)
        if not df.columns.is_unique:
            return False, errors
        expected_columns = self.list_columns()
        if list(df) not in (expected_columns, [*expected_columns, "geometry"]):
            errors.append("Columns must follow the complete declared attribute order")
        for column in self.columns:
            if column.name not in df:
                continue
            dtype = df[column.name].dtype
            if column.type == contracts.ColumnType.STRING:
                if dtype != pd.Series([""]).dtype:
                    errors.append(f"Column '{column.name}' must use native pandas text dtype")
            else:
                expected = (
                    "Int64"
                    if column.type == contracts.ColumnType.INTEGER
                    else "datetime64[ns]"
                    if column.type == contracts.ColumnType.DATE
                    else "float64"
                )
                if str(dtype) != expected:
                    errors.append(f"Column '{column.name}' must use {expected} dtype")
        if errors:
            return False, errors
        if any(not value.strip() for value in df["feature_id"]):
            errors.append("Feature.id requires nonblank published text without a uniqueness claim")
        if "uf" in df:
            normalized = df["uf_original"].str.strip().str.upper()
            normalized = normalized.where(normalized.isin(regions.UFS_VALIDAS))
            if not df["uf"].equals(normalized):
                errors.append(
                    "Normalized UF must match the preserved published UF or remain missing"
                )
        return not errors, errors

    def to_dict(self) -> dict[str, Any]:
        schema = super().to_dict()
        perfis = self.name == "embrapa_solos.perfis"
        properties = (
            constants.EMBRAPA_SOLOS_PERFIS_PROPERTIES
            if perfis
            else constants.EMBRAPA_SOLOS_MAPA_PROPERTIES
        )
        aliases = (
            constants.EMBRAPA_SOLOS_PERFIS_RENAME_MAP
            if perfis
            else constants.EMBRAPA_SOLOS_MAPA_RENAME_MAP
        )
        schema["constraints"].update(
            attribute_order=self.list_columns(),
            optional_geometry_column="geometry_after_all_attributes",
            string_dtype="native pandas text (str on pandas 3, object on pandas 2)",
            integer_dtype="Int64",
            float_dtype="float64",
            source_property_projection=list(properties),
            column_aliases=dict(aliases),
            source_missing_member="unsupported_projection_raises_ParseError",
            source_null="preserved_when_XSD_nillable_not_equivalent_to_missing_member",
            source_extra_member="layout_drift_raises_ParseError",
            text_semantics="literal_published_text_except_typed_calendar_columns",
            feature_id="required_nonblank_published_string_not_unique",
            identifier_semantics="published_identifiers_are_not_primary_keys",
            integer_semantics="exact_signed_XSD_integer_without_float_intermediate",
            float_semantics="finite_binary64_signed_zero_preserved_overflow_and_nonzero_underflow_rejected",
            geometry_semantics="published_geometry_without_reprojection_topology_repair_or_area_recalculation",
        )
        if perfis:
            schema["constraints"].update(
                uf_domain=sorted(regions.UFS_VALIDAS),
                uf_original="unaltered_published_text",
                uf_normalization="trim_uppercase_known_UF_else_missing_with_occurrence_preserved",
                calendar_columns={"ano": "Int64", "data_colet": "datetime64[ns]"},
                calendar_null="source_null_or_literal_NULL_becomes_missing",
                coordinates="finite_published_attributes_out_of_range_preserved_with_diagnostic",
                positional_accuracy="may_include_coordinates_assigned_to_municipality",
                horizon_semantics="source_occurrence_with_published_point_identifier_not_merged",
            )
        else:
            schema["constraints"].update(
                area_semantics="published_attribute_finite_negative_values_preserved_with_diagnostic",
                classification_semantics="three_published_components_kept_separate",
            )
        return schema


def _column(name: str) -> contracts.Column:
    if name == "ano":
        return contracts.Column(name, contracts.ColumnType.INTEGER, nullable=True)
    if name == "data_colet":
        return contracts.Column(name, contracts.ColumnType.DATE, nullable=True)
    if name in ("fid", "codigo_pon"):
        bits = 32 if name == "fid" else 64
        return contracts.Column(
            name,
            contracts.ColumnType.INTEGER,
            nullable=name != "fid",
            min_value=-(2 ** (bits - 1)),
            max_value=2 ** (bits - 1) - 1,
        )
    if name in ("latitude", "longitude", "area_km2"):
        return contracts.Column(
            name,
            contracts.ColumnType.FLOAT,
            nullable=True,
            unit="km2" if name == "area_km2" else None,
        )
    return contracts.Column(name, contracts.ColumnType.STRING, nullable=name != "feature_id")


PERFIS_V3 = SolosContract(
    name="embrapa_solos.perfis",
    version="3.0",
    effective_from="2.0.0",
    primary_key=[],
    columns=[_column(name) for name in constants.EMBRAPA_SOLOS_PERFIS_COLUMNS],
    guarantees=[
        "Todos os 83 atributos publicados estão representados nas 85 colunas",
        "Valores laboratoriais e profundidades permanecem textos literais",
        "Ano usa Int64 e data_colet usa datetime64[ns]; null e texto NULL viram ausentes",
        "Nas colunas textuais, null, texto NULL, string vazia, espaços e zero são distintos",
        "Código de ponto e horizonte são preservados sem fundir ocorrências",
        "Feature.id e identificadores publicados não estabelecem chave primária",
        "UF original é preservada, inclusive quando a normalização produz valor ausente",
        "Posição publicada não garante precisão do local de amostragem",
    ],
)

MAPA_V2 = SolosContract(
    name="embrapa_solos.mapa",
    version="2.0",
    effective_from="2.0.0",
    primary_key=[],
    columns=[_column(name) for name in constants.EMBRAPA_SOLOS_MAPA_COLUMNS],
    guarantees=[
        "Todos os 18 atributos publicados permanecem recuperáveis nas 19 colunas",
        "Os três componentes classificatórios são preservados separadamente",
        "Legendas e classe dominante não são reconstruídas a partir dos componentes",
        "Área é o atributo publicado, sem recálculo pela geometria",
        "Identificadores repetidos não eliminam ocorrências",
        "Mapa em escala 1:5 milhões não estabelece precisão de imóvel",
    ],
)

contracts.register_contract("embrapa_solos_perfis", PERFIS_V3)
contracts.register_contract("embrapa_solos_mapa", MAPA_V2)
