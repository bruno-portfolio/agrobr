from __future__ import annotations

import re
from typing import Any

import pandas as pd

from agrobr import constants, contracts
from agrobr.normalize import regions


class ZoneamentoContract(contracts.Contract):
    def validate(self, df: pd.DataFrame) -> tuple[bool, list[str]]:
        valid, errors = super().validate(df)
        if not df.columns.is_unique:
            return valid, errors
        domains = {
            "solo_codigo": constants.ZARC_SOIL_CODES,
            "ciclo_codigo": constants.ZARC_CYCLE_CODES,
            **dict.fromkeys(constants.ZARC_RISK_COLUMNS, constants.ZARC_RISK_VALUES),
        }
        for name in constants.ZARC_INTEGER_COLUMNS:
            if name not in df:
                continue
            if str(df[name].dtype) != "Int64":
                errors.append(f"Column '{name}' must use nullable Int64 dtype")
            elif name in domains and not df[name].dropna().isin(domains[name]).all():
                errors.append(f"Column '{name}' contains values outside the validated domain")
        self._validate_text(df, errors)
        if {"safra", "safra_inicio", "safra_fim"}.issubset(df.columns) and not all(
            _valid_season(*values)
            for values in zip(df["safra"], df["safra_inicio"], df["safra_fim"], strict=True)
        ):
            errors.append("Season must agree with the two published season fields")
        if "registro_origem" in df and str(df["registro_origem"].dtype) == "Int64":
            positions = df["registro_origem"].dropna()
            if (
                (positions <= 0).any()
                or positions.duplicated().any()
                or not positions.is_monotonic_increasing
            ):
                errors.append("Source record positions must be positive, unique and increasing")
        return not errors, errors

    def _validate_text(self, df: pd.DataFrame, errors: list[str]) -> None:
        for name in ("cultura", "cultura_original", "portaria", "cultura_codigo"):
            if name in df and not all(
                isinstance(value, str) and value.strip() for value in df[name]
            ):
                errors.append(f"Column '{name}' must contain non-empty published text")
        for name in constants.ZARC_TEXT_CODE_COLUMNS:
            if name in df and not all(
                isinstance(value, str) and re.fullmatch(r"[0-9]*", value) is not None
                for value in df[name]
            ):
                errors.append(f"Column '{name}' must preserve textual ASCII digits or empty cells")
        if "geocodigo" in df and not all(
            isinstance(value, str) and re.fullmatch(r"[0-9]{7}", value) is not None
            for value in df["geocodigo"]
        ):
            errors.append("Geocode must contain exactly seven ASCII digits")
        if "uf" in df and not df["uf"].isin(regions.UFS_VALIDAS).all():
            errors.append("UF must contain official uppercase Brazilian state codes")

    def to_dict(self) -> dict[str, Any]:
        schema = super().to_dict()
        schema["constraints"].update(
            integer_dtype="Int64",
            risk_values=sorted(constants.ZARC_RISK_VALUES),
            soil_codes=sorted(constants.ZARC_SOIL_CODES),
            cycle_codes=sorted(constants.ZARC_CYCLE_CODES),
            geocode_pattern=r"[0-9]{7}",
            text_code_columns=list(constants.ZARC_TEXT_CODE_COLUMNS),
            text_code_pattern=r"[0-9]*",
            blank_text="preserved_as_empty_string",
            risk_empty_cell="null_distinct_from_zero",
            source_record_scope="SHA256 of the complete CSV body in MetaInfo.raw_content_hash",
            source_record_positive_unique_increasing=True,
            semantic_primary_key="not_asserted",
            source_multiplicity="preserved_without_deduplication",
            season_matches_published_fields=True,
            non_annual_seasons=constants.ZARC_NON_ANNUAL_SEASONS.copy(),
            productivity="published_text_without_inferred_unit",
        )
        return schema


def _valid_season(season: object, initial: object, final: object) -> bool:
    if not (isinstance(season, str) and isinstance(initial, str) and isinstance(final, str)):
        return False
    if initial == "":
        return (
            final in constants.ZARC_NON_ANNUAL_SEASONS
            and season == (constants.ZARC_NON_ANNUAL_SEASONS[final])
        )
    return (
        re.fullmatch(r"[0-9]{4}", initial) is not None
        and re.fullmatch(r"[0-9]{4}", final) is not None
        and int(final) == int(initial) + 1
        and season == f"{initial}/{final}"
    )


ZONEAMENTO_AGRICOLA_V2 = ZoneamentoContract(
    name="zarc.zoneamento",
    version="2.1",
    effective_from="2.0.0",
    primary_key=[],
    columns=[
        *(
            contracts.Column(
                name=name,
                type=(
                    contracts.ColumnType.INTEGER
                    if name in constants.ZARC_INTEGER_COLUMNS
                    else contracts.ColumnType.STRING
                ),
                nullable=name in constants.ZARC_RISK_COLUMNS,
                description=(
                    "Posição CSV antes de filtros; somente identificável junto ao hash do corpo"
                    if name == "registro_origem"
                    else ""
                ),
            )
            for name in constants.ZARC_OUTPUT_COLUMNS
        ),
        contracts.Column(
            name="cod_municipio",
            type=contracts.ColumnType.INTEGER,
            nullable=True,
            stable=False,
            description="Código IBGE do município (7 dígitos), a chave comum dos datasets municipais; nulo onde a linha não é de município.",
        ),
    ],
    guarantees=[
        "Cada ocorrência publicada é preservada, inclusive duplicatas literais e riscos distintos",
        "Não há chave semântica única afirmada para as edições processadas",
        "Registro de origem é posicional e restrito ao hash do CSV, sem estabilidade entre revisões",
        "Riscos usam Int64; célula vazia é nula e não se confunde com o valor zero",
        "Valores 0 e 50 publicados são preservados sem interpretação agronômica presumida",
        "Códigos textuais mantêm zeros à esquerda e ausências publicadas como strings vazias",
        "Produtividade permanece texto publicado, inclusive decimal com vírgula e unidade não inferida",
        "Safras anuais e modalidades não anuais mantêm os dois campos originais",
        "Validação até EOF comprova processamento do corpo, sem afirmar total externo ou snapshot",
    ],
)
