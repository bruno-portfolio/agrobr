from __future__ import annotations

import math
from typing import Any, Literal

import pandas as pd

from agrobr.contracts import desmatamento as contracts
from agrobr.exceptions import ContractViolationError
from agrobr.normalize import regions


def _fail(tipo: str, reason: str) -> ContractViolationError:
    return ContractViolationError(dataset=f"desmatamento_{tipo}", violation=reason)


def _validate_keys(frame: pd.DataFrame, tipo: str) -> None:
    temporal = "ano" if tipo == "prodes" else "data"
    required = {temporal, "uf", "classe", "bioma", "area_km2", "satelite", "sensor"}
    if tipo == "deter":
        required.update(("municipio", "municipio_id"))
    if missing := required - set(frame.columns):
        raise _fail(tipo, f"Campos de agregação ausentes: {sorted(missing)}")
    invalid = {
        name: int(
            frame[name].map(lambda value: not isinstance(value, str) or not value.strip()).sum()
        )
        for name in ("uf", "classe", "bioma")
    }
    invalid[temporal] = int(frame[temporal].isna().sum())
    if any(invalid.values()):
        raise _fail(tipo, f"Chaves ausentes ou vazias; nenhuma ocorrência descartada: {invalid}")
    if not frame["uf"].isin(regions.UFS_VALIDAS).all():
        raise _fail(tipo, "UF não reconhecida impede agregação")
    if not frame["bioma"].isin(regions.BIOMAS_VALIDOS).all():
        raise _fail(tipo, "Bioma não reconhecido impede agregação")
    if tipo == "prodes":
        year = frame["ano"]
        if (
            not pd.api.types.is_numeric_dtype(year)
            or pd.api.types.is_bool_dtype(year)
            or not year.between(1, 9999).all()
            or not (year % 1 == 0).all()
        ):
            raise _fail(tipo, "Agregação anual exige anos integrais entre 1 e 9999")
    elif (
        str(frame["data"].dtype) != "datetime64[ns]"
        or not frame["data"].eq(frame["data"].dt.normalize()).all()
    ):
        raise _fail(tipo, "Agregação diária exige datas civis datetime64[ns] sem horário")


def _agreed_value(values: pd.Series) -> Any:
    return values.iloc[0] if values.nunique(dropna=False) == 1 else None


def _complete_sum(values: pd.Series) -> float:
    return float("nan") if values.isna().any() else math.fsum(values)


def aggregate(
    frame: pd.DataFrame, tipo: Literal["prodes", "deter"]
) -> tuple[pd.DataFrame, dict[str, Any]]:
    contract = (
        contracts.DESMATAMENTO_PRODES_V2 if tipo == "prodes" else contracts.DESMATAMENTO_DETER_V2
    )
    details: dict[str, Any] = {
        "operation": "sum_published_feature_area",
        "input_occurrences": len(frame),
        "discarded_occurrences": 0,
        "group_keys": list(contract.primary_key),
        "missing_area_policy": "any_missing_area_makes_group_total_missing",
        "summation": "math.fsum_of_source_float64_values",
        "official_rate": False,
    }
    if frame.empty:
        details.update(output_groups=0, missing_area_groups=0, heterogeneous={})
        return contract.empty_frame(), details
    _validate_keys(frame, tipo)
    for column in contract.columns:
        if column.name in frame and (errors := column.validate(frame[column.name])):
            raise _fail(tipo, "; ".join(errors))
    groups = frame.groupby(contract.primary_key, dropna=False, sort=True, observed=True)
    try:
        total = groups["area_km2"].agg(_complete_sum)
    except OverflowError as exc:
        raise _fail(tipo, "Soma das áreas excede a capacidade de float64") from exc
    output = pd.DataFrame({"area_km2": total})
    heterogeneous = {}
    for name in ("satelite", "sensor"):
        output[name] = groups[name].agg(_agreed_value)
        heterogeneous[name] = int((groups[name].nunique(dropna=False) > 1).sum())
    output = output.reset_index()
    if tipo == "deter":
        output["cod_municipio"] = regions.cod_municipio(output["municipio_id"])
    output = output[contract.list_columns()]
    for column in contract.columns:
        if column.type.value == "str":
            output[column.name] = output[column.name].astype(contracts.TEXTO)
        elif column.type.value == "int":
            output[column.name] = output[column.name].astype("Int64")
    valid, errors = contract.validate(output)
    if not valid:
        raise _fail(tipo, "; ".join(errors))
    details.update(
        output_groups=len(output),
        missing_area_groups=int(output["area_km2"].isna().sum()),
        heterogeneous=heterogeneous,
    )
    return output, details
