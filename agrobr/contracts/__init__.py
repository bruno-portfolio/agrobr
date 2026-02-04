"""Stability Contracts para garantia de schema e compatibilidade."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

import pandas as pd


class ColumnType(StrEnum):
    """Tipos de dados suportados nos contratos."""

    DATE = "date"
    DATETIME = "datetime"
    STRING = "str"
    INTEGER = "int"
    FLOAT = "float"
    DECIMAL = "Decimal"
    BOOLEAN = "bool"


class BreakingChangePolicy(StrEnum):
    """Politica para mudancas que quebram contrato."""

    MAJOR_VERSION = "major"
    NEVER = "never"
    DEPRECATE_FIRST = "deprecate"


@dataclass
class Column:
    """Definicao de uma coluna no contrato."""

    name: str
    type: ColumnType
    nullable: bool = False
    unit: str | None = None
    description: str = ""
    stable: bool = True
    deprecated: bool = False
    deprecated_in: str | None = None
    removed_in: str | None = None

    def validate(self, series: pd.Series) -> list[str]:
        """Valida uma serie contra esta definicao."""
        errors = []

        if not self.nullable and series.isna().any():
            null_count = series.isna().sum()
            errors.append(f"Column '{self.name}' has {null_count} null values but nullable=False")

        if self.type == ColumnType.DATE:
            if not pd.api.types.is_datetime64_any_dtype(series):
                try:
                    pd.to_datetime(series.dropna())
                except Exception:
                    errors.append(f"Column '{self.name}' cannot be converted to date")

        elif self.type == ColumnType.INTEGER:
            if not pd.api.types.is_integer_dtype(series):
                non_null = series.dropna()
                if len(non_null) > 0:
                    try:
                        non_null.astype(int)
                    except (ValueError, TypeError):
                        errors.append(f"Column '{self.name}' contains non-integer values")

        elif self.type in (
            ColumnType.FLOAT,
            ColumnType.DECIMAL,
        ) and not pd.api.types.is_numeric_dtype(series):
            errors.append(f"Column '{self.name}' is not numeric")

        return errors


@dataclass
class Contract:
    """Contrato de estabilidade para um dataset."""

    name: str
    version: str
    columns: list[Column]
    guarantees: list[str] = field(default_factory=list)
    breaking_policy: BreakingChangePolicy = BreakingChangePolicy.MAJOR_VERSION
    effective_from: str = ""

    def validate(self, df: pd.DataFrame) -> tuple[bool, list[str]]:
        """
        Valida DataFrame contra o contrato.

        Args:
            df: DataFrame a validar

        Returns:
            Tupla (valido, lista de erros)
        """
        errors = []

        required_cols = [c.name for c in self.columns if not c.nullable and c.stable]
        missing = set(required_cols) - set(df.columns)
        if missing:
            errors.append(f"Missing required columns: {missing}")

        for col_def in self.columns:
            if col_def.name in df.columns:
                col_errors = col_def.validate(df[col_def.name])
                errors.extend(col_errors)

        return len(errors) == 0, errors

    def get_column(self, name: str) -> Column | None:
        """Retorna definicao de uma coluna pelo nome."""
        for col in self.columns:
            if col.name == name:
                return col
        return None

    def list_columns(self, stable_only: bool = False) -> list[str]:
        """Lista nomes das colunas."""
        if stable_only:
            return [c.name for c in self.columns if c.stable]
        return [c.name for c in self.columns]

    def to_markdown(self) -> str:
        """Gera documentacao Markdown do contrato."""
        lines = [
            f"# Contract: {self.name}",
            f"**Version:** {self.version}",
            f"**Effective from:** {self.effective_from}",
            f"**Breaking policy:** {self.breaking_policy.value}",
            "",
            "## Columns",
            "",
            "| Column | Type | Nullable | Unit | Stable | Description |",
            "|--------|------|----------|------|--------|-------------|",
        ]

        for col in self.columns:
            stable = "Yes" if col.stable else "No"
            nullable = "Yes" if col.nullable else "No"
            unit = col.unit or "-"
            desc = col.description or "-"
            deprecated = " (deprecated)" if col.deprecated else ""
            lines.append(
                f"| {col.name}{deprecated} | {col.type.value} | {nullable} | {unit} | {stable} | {desc} |"
            )

        if self.guarantees:
            lines.extend(["", "## Guarantees", ""])
            for g in self.guarantees:
                lines.append(f"- {g}")

        return "\n".join(lines)

    def to_dict(self) -> dict[str, Any]:
        """Converte contrato para dicionario."""
        return {
            "name": self.name,
            "version": self.version,
            "effective_from": self.effective_from,
            "breaking_policy": self.breaking_policy.value,
            "columns": [
                {
                    "name": c.name,
                    "type": c.type.value,
                    "nullable": c.nullable,
                    "unit": c.unit,
                    "stable": c.stable,
                    "deprecated": c.deprecated,
                    "description": c.description,
                }
                for c in self.columns
            ],
            "guarantees": self.guarantees,
        }


__all__ = [
    "Column",
    "ColumnType",
    "Contract",
    "BreakingChangePolicy",
]
