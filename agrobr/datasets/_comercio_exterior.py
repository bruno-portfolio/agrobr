from __future__ import annotations

import copy
import dataclasses
import math
from datetime import UTC, datetime
from numbers import Integral
from typing import Any

import pandas as pd

from agrobr.datasets.deterministic import get_snapshot
from agrobr.exceptions import ContractViolationError, InvalidParameterError
from agrobr.models import MetaInfo


def validate_options(return_meta: bool) -> None:
    if type(return_meta) is not bool:
        raise InvalidParameterError("return_meta deve ser booleano")
    if get_snapshot() is not None:
        raise InvalidParameterError("Comércio exterior não suporta deterministic: fontes mutáveis")


def _sum_values(values: pd.Series) -> int | float:
    if values.isna().any():
        return float("nan")
    if all(isinstance(value, Integral) and not isinstance(value, bool) for value in values):
        return sum(int(value) for value in values)
    try:
        value = math.fsum(float(value) for value in values)
    except (OverflowError, ValueError) as exc:
        raise ContractViolationError("comercio_exterior", "Soma não finita") from exc
    if not math.isfinite(value):
        raise ContractViolationError("comercio_exterior", "Soma não finita")
    return value


def agregar_ncms(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.drop(columns="ncm", errors="ignore")

    group_cols = [column for column in ("ano", "mes", "produto", "uf") if column in df.columns]
    value_cols = [
        column
        for column in ("kg_liquido", "valor_fob_usd", "valor_frete_usd", "valor_seguro_usd")
        if column in df.columns
    ]
    if not group_cols or not value_cols:
        return df.drop(columns="ncm", errors="ignore")

    output_cols = [*group_cols, *value_cols]
    derive_tonnes = "volume_ton" in df.columns and "kg_liquido" in value_cols
    if derive_tonnes:
        output_cols.append("volume_ton")
    records = []
    for key, group in df.groupby(group_cols, sort=True, dropna=False):
        record: dict[str, Any] = dict(
            zip(group_cols, key if isinstance(key, tuple) else (key,), strict=True)
        )
        record.update({column: _sum_values(group[column]) for column in value_cols})
        if derive_tonnes:
            record["volume_ton"] = record["kg_liquido"] / 1000
        records.append(record)
    result = pd.DataFrame.from_records(records, columns=output_cols)
    for column in group_cols:
        result[column] = result[column].astype(df[column].dtype)
    return result.loc[:, [column for column in df.columns if column != "ncm" and column in result]]


def adapt_comexstat(
    frame: pd.DataFrame, meta: MetaInfo | None, *, combine_ncms: bool
) -> tuple[pd.DataFrame, MetaInfo | None]:
    output = agregar_ncms(frame) if combine_ncms else frame.copy()
    for column in (
        "kg_liquido",
        "valor_fob_usd",
        "valor_frete_usd",
        "valor_seguro_usd",
        "volume_ton",
    ):
        if column in output:
            output[column] = output[column].astype("float64")
    if meta is None:
        return output, None
    details = copy.deepcopy(meta.source_details)
    details["dataset_transformation"] = {
        "operation": "aggregate_ncm_prefix" if combine_ncms else "preserve_exact_ncm",
        "group_by": [name for name in ("ano", "mes", "produto", "uf") if name in output],
        "input_rows": len(frame),
        "output_rows": len(output),
        "weight_sum": "Accurate sum of source float64 kg; missing propagates",
        "monetary_sum": "Accurate sum of the source float64 values; missing propagates",
        "output_dtypes": {name: str(dtype) for name, dtype in output.dtypes.items()},
    }
    return output, dataclasses.replace(
        meta, records_count=len(output), columns=list(output.columns), source_details=details
    )


def restore_empty_comexstat(frame: pd.DataFrame, meta: MetaInfo | None) -> pd.DataFrame:
    if not frame.empty or meta is None:
        return frame
    dtypes = meta.source_details.get("dataset_transformation", {}).get("output_dtypes", {})
    if not dtypes:
        return frame
    return pd.DataFrame({name: pd.Series(dtype=dtype) for name, dtype in dtypes.items()})


def dataset_meta(meta: MetaInfo) -> MetaInfo:
    now = datetime.now(UTC)
    return dataclasses.replace(meta, timestamp=now)
