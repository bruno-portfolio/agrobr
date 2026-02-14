from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

import pandas as pd


class ColumnType(StrEnum):
    DATE = "date"
    DATETIME = "datetime"
    STRING = "str"
    INTEGER = "int"
    FLOAT = "float"
    DECIMAL = "Decimal"
    BOOLEAN = "bool"


class BreakingChangePolicy(StrEnum):
    MAJOR_VERSION = "major"
    NEVER = "never"
    DEPRECATE_FIRST = "deprecate"


@dataclass
class Column:
    name: str
    type: ColumnType
    nullable: bool = False
    unit: str | None = None
    description: str = ""
    stable: bool = True
    deprecated: bool = False
    deprecated_in: str | None = None
    removed_in: str | None = None
    min_value: float | None = None
    max_value: float | None = None

    def validate(self, series: pd.Series) -> list[str]:
        errors: list[str] = []

        if not self.nullable and series.isna().any():
            null_count = int(series.isna().sum())
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

        non_null = series.dropna()
        if (
            self.min_value is not None
            and pd.api.types.is_numeric_dtype(non_null)
            and len(non_null) > 0
            and (non_null < self.min_value).any()
        ):
            actual_min = float(non_null.min())
            errors.append(
                f"Column '{self.name}' has values below minimum {self.min_value} (got {actual_min})"
            )

        if (
            self.max_value is not None
            and pd.api.types.is_numeric_dtype(non_null)
            and len(non_null) > 0
            and (non_null > self.max_value).any()
        ):
            actual_max = float(non_null.max())
            errors.append(
                f"Column '{self.name}' has values above maximum {self.max_value} (got {actual_max})"
            )

        return errors


@dataclass
class Contract:
    name: str
    version: str
    columns: list[Column]
    primary_key: list[str] = field(default_factory=list)
    guarantees: list[str] = field(default_factory=list)
    breaking_policy: BreakingChangePolicy = BreakingChangePolicy.MAJOR_VERSION
    effective_from: str = ""

    def validate(self, df: pd.DataFrame) -> tuple[bool, list[str]]:
        errors: list[str] = []

        required_cols = [c.name for c in self.columns if not c.nullable and c.stable]
        missing = set(required_cols) - set(df.columns)
        if missing:
            errors.append(f"Missing required columns: {missing}")

        for col_def in self.columns:
            if col_def.name in df.columns:
                col_errors = col_def.validate(df[col_def.name])
                errors.extend(col_errors)

        if self.primary_key and len(df) > 0:
            pk_cols = [c for c in self.primary_key if c in df.columns]
            if pk_cols == self.primary_key:
                duplicates = df.duplicated(subset=pk_cols, keep=False)
                if duplicates.any():
                    dup_count = int(duplicates.sum())
                    errors.append(f"Primary key {pk_cols} has {dup_count} duplicate rows")

        return len(errors) == 0, errors

    def get_column(self, name: str) -> Column | None:
        for col in self.columns:
            if col.name == name:
                return col
        return None

    def list_columns(self, stable_only: bool = False) -> list[str]:
        if stable_only:
            return [c.name for c in self.columns if c.stable]
        return [c.name for c in self.columns]

    def to_markdown(self) -> str:
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
                f"| {col.name}{deprecated} | {col.type.value} "
                f"| {nullable} | {unit} | {stable} | {desc} |"
            )

        if self.primary_key:
            lines.extend(["", f"**Primary key:** {self.primary_key}"])

        if self.guarantees:
            lines.extend(["", "## Guarantees", ""])
            for g in self.guarantees:
                lines.append(f"- {g}")

        return "\n".join(lines)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "schema_version": self.version,
            "effective_from": self.effective_from,
            "breaking_policy": self.breaking_policy.value,
            "primary_key": self.primary_key,
            "required_columns": [c.name for c in self.columns if not c.nullable and c.stable],
            "dtypes": {c.name: c.type.value for c in self.columns},
            "nullable": {c.name: c.nullable for c in self.columns},
            "columns": [
                {
                    "name": c.name,
                    "type": c.type.value,
                    "nullable": c.nullable,
                    "unit": c.unit,
                    "stable": c.stable,
                    "deprecated": c.deprecated,
                    "description": c.description,
                    "min_value": c.min_value,
                    "max_value": c.max_value,
                }
                for c in self.columns
            ],
            "constraints": self._build_constraints(),
            "guarantees": self.guarantees,
        }

    def _build_constraints(self) -> dict[str, Any]:
        constraints: dict[str, Any] = {}
        if self.primary_key:
            constraints["no_duplicates"] = True
        for col in self.columns:
            if col.min_value is not None:
                constraints[f"{col.name}_min"] = col.min_value
            if col.max_value is not None:
                constraints[f"{col.name}_max"] = col.max_value
        return constraints

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, ensure_ascii=False)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Contract:
        columns = []
        for col_data in data.get("columns", []):
            columns.append(
                Column(
                    name=col_data["name"],
                    type=ColumnType(col_data["type"]),
                    nullable=col_data.get("nullable", False),
                    unit=col_data.get("unit"),
                    description=col_data.get("description", ""),
                    stable=col_data.get("stable", True),
                    deprecated=col_data.get("deprecated", False),
                    min_value=col_data.get("min_value"),
                    max_value=col_data.get("max_value"),
                )
            )
        return cls(
            name=data["name"],
            version=data.get("schema_version", data.get("version", "1.0")),
            columns=columns,
            primary_key=data.get("primary_key", []),
            guarantees=data.get("guarantees", []),
            breaking_policy=BreakingChangePolicy(data.get("breaking_policy", "major")),
            effective_from=data.get("effective_from", ""),
        )

    @classmethod
    def from_json(cls, json_str: str) -> Contract:
        return cls.from_dict(json.loads(json_str))


_CONTRACT_REGISTRY: dict[str, Contract] = {}


def register_contract(dataset_name: str, contract: Contract) -> None:
    _CONTRACT_REGISTRY[dataset_name] = contract


def get_contract(dataset_name: str) -> Contract:
    if dataset_name not in _CONTRACT_REGISTRY:
        raise KeyError(
            f"No contract registered for dataset '{dataset_name}'. "
            f"Available: {list(_CONTRACT_REGISTRY.keys())}"
        )
    return _CONTRACT_REGISTRY[dataset_name]


def list_contracts() -> list[str]:
    return sorted(_CONTRACT_REGISTRY.keys())


def has_contract(dataset_name: str) -> bool:
    return dataset_name in _CONTRACT_REGISTRY


def validate_dataset(df: pd.DataFrame, contract: Contract | str) -> None:
    from agrobr.exceptions import ContractViolationError

    resolved = get_contract(contract) if isinstance(contract, str) else contract

    valid, errors = resolved.validate(df)
    if not valid:
        raise ContractViolationError(
            dataset=resolved.name,
            violation="; ".join(errors),
            expected=resolved.list_columns(stable_only=True),
            got=list(df.columns),
        )


def generate_json_schemas(output_dir: str) -> list[str]:
    from pathlib import Path

    path = Path(output_dir)
    path.mkdir(parents=True, exist_ok=True)

    generated: list[str] = []
    for dataset_name in sorted(_CONTRACT_REGISTRY):
        contract = _CONTRACT_REGISTRY[dataset_name]
        filepath = path / f"{dataset_name}.json"
        filepath.write_text(contract.to_json(), encoding="utf-8")
        generated.append(str(filepath))

    return generated


__all__ = [
    "BreakingChangePolicy",
    "Column",
    "ColumnType",
    "Contract",
    "generate_json_schemas",
    "get_contract",
    "has_contract",
    "list_contracts",
    "register_contract",
    "validate_dataset",
]
