from __future__ import annotations

import importlib
from types import ModuleType
from typing import Any, cast

import pandas as pd

from agrobr.exceptions import InvalidParameterError
from agrobr.models import MetaInfo
from agrobr.utils import result as result_utils


def preflight(as_polars: bool, return_meta: bool, kwargs: dict[str, Any]) -> ModuleType | None:
    if type(as_polars) is not bool or type(return_meta) is not bool:
        raise InvalidParameterError("as_polars e return_meta devem ser booleanos")
    if kwargs:
        raise InvalidParameterError(f"Argumentos desconhecidos: {sorted(kwargs)}")
    if not as_polars:
        return None
    try:
        return importlib.import_module("polars")
    except ImportError as exc:
        raise ImportError(
            "polars é necessário para as_polars=True. Instale agrobr[polars]"
        ) from exc


def finalize(
    frame: pd.DataFrame, meta: MetaInfo, polars: ModuleType | None, return_meta: bool
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    frame = result_utils.datas_em_ns(frame)
    result: Any = frame
    if polars is not None:
        series = []
        for column in frame:
            if column in {"data", "mes"}:
                values = [value.to_pydatetime() for value in frame[column]]
                dtype = polars.Datetime("ns")
            elif column == "uf":
                values = frame[column].tolist()
                dtype = polars.Utf8
            else:
                values = [None if pd.isna(value) else float(value) for value in frame[column]]
                dtype = polars.Float64
            series.append(polars.Series(column, values, dtype=dtype))
        result = polars.DataFrame(series)
    return cast(
        "pd.DataFrame | tuple[pd.DataFrame, MetaInfo]", (result, meta) if return_meta else result
    )
