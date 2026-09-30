from __future__ import annotations

import re
from typing import Any

import pandas as pd

from agrobr import constants, contracts


class MapbiomasMunicipalContract(contracts.Contract):
    def empty_frame(self) -> pd.DataFrame:
        return super().empty_frame().astype({"area_ha": "float64"})

    def validate(self, df: pd.DataFrame) -> tuple[bool, list[str]]:
        valid, errors = super().validate(df)
        if not df.columns.is_unique:
            return valid, errors
        for column in self.columns:
            if (
                column.type == contracts.ColumnType.STRING
                and column.name in df
                and not all(
                    (column.nullable and pd.isna(value))
                    or (isinstance(value, str) and value.strip())
                    for value in df[column.name]
                )
            ):
                errors.append(f"Column '{column.name}' must contain non-empty strings")
        for name, dtype in {
            "classe_id": "Int64",
            "ano": "Int64",
            "area_ha": "float64",
            "id_registro": "Int64",
        }.items():
            if name in df and str(df[name].dtype) != dtype:
                errors.append(f"Column '{name}' must have dtype {dtype}")
        if "geocodigo" in df and not all(
            isinstance(value, str) and re.fullmatch(constants.MAPBIOMAS_GEOCODE_PATTERN, value)
            for value in df["geocodigo"]
        ):
            errors.append("Column 'geocodigo' must contain seven ASCII digits as text")
        return not errors, errors

    def to_dict(self) -> dict[str, Any]:
        schema = super().to_dict()
        schema["constraints"].update(
            {
                "geocodigo_pattern": constants.MAPBIOMAS_GEOCODE_PATTERN,
                "physical_dtypes": {
                    "classe_id": "Int64",
                    "ano": "Int64",
                    "area_ha": "float64",
                    "id_registro": "Int64",
                },
                "non_empty_strings": [
                    column.name
                    for column in self.columns
                    if column.type == contracts.ColumnType.STRING
                ],
                "primary_key_scope": "one_collection_and_resource",
            }
        )
        return schema


MAPBIOMAS_COBERTURA_MUNICIPAL_V1 = MapbiomasMunicipalContract(
    name="mapbiomas.cobertura_municipal",
    version="1.1",
    effective_from="2.0.0",
    primary_key=["bioma", "uf", "geocodigo", "classe_id", "id_registro", "ano"],
    columns=[
        contracts.Column(name="bioma", type=contracts.ColumnType.STRING),
        contracts.Column(name="uf", type=contracts.ColumnType.STRING),
        contracts.Column(name="municipio", type=contracts.ColumnType.STRING),
        contracts.Column(name="classe_id", type=contracts.ColumnType.INTEGER, min_value=0),
        contracts.Column(name="classe", type=contracts.ColumnType.STRING, nullable=True),
        contracts.Column(name="nivel_0", type=contracts.ColumnType.STRING),
        contracts.Column(name="ano", type=contracts.ColumnType.INTEGER, min_value=1985),
        contracts.Column(name="area_ha", type=contracts.ColumnType.FLOAT, unit="ha", min_value=0),
        contracts.Column(name="geocodigo", type=contracts.ColumnType.STRING),
        contracts.Column(name="id_registro", type=contracts.ColumnType.INTEGER, min_value=0),
        contracts.Column(
            name="cod_municipio",
            type=contracts.ColumnType.INTEGER,
            nullable=True,
            stable=False,
            description="Código IBGE do município (7 dígitos), a chave comum dos datasets municipais; nulo onde a linha não é de município.",
        ),
    ],
    guarantees=[
        "Geocodigo is the seven-digit textual identifier published by MapBiomas, including lakes",
        "Published biome, state and municipal intersections remain separate in the primary key",
        "Published row IDs preserve distinct records within the collection; zero is valid",
        "No null or blank identity, description or level-zero fields are emitted; classe is null "
        "only for a published class id outside the known legend, with a warning",
        "Area is finite and non-negative in hectares; zero is preserved",
        "Class, year and published row ID use Int64; area uses float64, including the typed empty frame",
        "Uniqueness applies within one collection and processed resource, not across revisions",
    ],
)
