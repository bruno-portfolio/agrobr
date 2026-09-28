from __future__ import annotations

import copy
from typing import TYPE_CHECKING, Any, Literal, overload

import pandas as pd

if TYPE_CHECKING:
    from agrobr.models import MetaInfo

ATRIBUTO_AVISOS = "agrobr_avisos"


def datas_em_ns(df: pd.DataFrame) -> pd.DataFrame:
    convertidas = {
        coluna: df[coluna].dt.as_unit("ns")
        for coluna in df.columns
        if pd.api.types.is_datetime64_any_dtype(df[coluna]) and df[coluna].dt.unit != "ns"
    }
    return df.assign(**convertidas) if convertidas else df


def build_source_meta(
    source: str,
    source_url: str,
    source_method: str,
    fetch_ms: int,
    parse_ms: int,
    df: pd.DataFrame,
    parser_version: int,
    *,
    schema_version: str = "1.0",
    attempted_sources: list[str] | None = None,
    selected_source: str | None = None,
    raw_content_hash: str | None = None,
    raw_content_size: int = 0,
    source_details: dict[str, Any] | None = None,
) -> MetaInfo:
    from agrobr.models import MetaInfo
    from agrobr.utils.time import utcnow

    now = utcnow()
    return MetaInfo(
        source=source,
        source_url=source_url,
        source_method=source_method,
        fetched_at=now,
        fetch_duration_ms=fetch_ms,
        parse_duration_ms=parse_ms,
        records_count=len(df),
        columns=df.columns.tolist(),
        parser_version=parser_version,
        schema_version=schema_version,
        attempted_sources=attempted_sources if attempted_sources is not None else [source],
        selected_source=selected_source if selected_source is not None else source,
        fetch_timestamp=now,
        raw_content_hash=raw_content_hash,
        raw_content_size=raw_content_size,
        source_details=copy.deepcopy(source_details) if source_details is not None else {},
        validation_warnings=list(df.attrs.get(ATRIBUTO_AVISOS, [])),
    )


@overload
def finalize_result(
    df: pd.DataFrame,
    meta: Any = ...,
    *,
    as_polars: bool = ...,
    return_meta: Literal[True],
    string_columns: tuple[str, ...] = ...,
) -> tuple[pd.DataFrame, Any]: ...


@overload
def finalize_result(
    df: pd.DataFrame,
    meta: Any = ...,
    *,
    as_polars: bool = ...,
    return_meta: Literal[False] = ...,
    string_columns: tuple[str, ...] = ...,
) -> pd.DataFrame: ...


@overload
def finalize_result(
    df: pd.DataFrame,
    meta: Any = ...,
    *,
    as_polars: bool = ...,
    return_meta: bool = ...,
    string_columns: tuple[str, ...] = ...,
) -> pd.DataFrame | tuple[pd.DataFrame, Any]: ...


def finalize_result(
    df: pd.DataFrame,
    meta: Any = None,
    *,
    as_polars: bool = False,
    return_meta: bool = False,
    string_columns: tuple[str, ...] = (),
) -> pd.DataFrame | tuple[pd.DataFrame, Any]:
    df = datas_em_ns(df)
    if as_polars:
        try:
            import polars as pl
        except ImportError:
            raise ImportError(
                "polars é necessário para as_polars=True. Instale com: pip install agrobr[polars]"
            ) from None

        schema_overrides = dict.fromkeys(string_columns, pl.Utf8) if string_columns else None
        result_df = pl.from_pandas(df, schema_overrides=schema_overrides)
        if return_meta:
            return result_df, meta  # type: ignore[return-value]
        return result_df  # type: ignore[return-value,no-any-return]

    if return_meta:
        return df, meta
    return df
