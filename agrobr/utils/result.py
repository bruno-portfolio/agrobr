from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, overload

if TYPE_CHECKING:
    import pandas as pd

    from agrobr.models import MetaInfo


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
    )


@overload
def finalize_result(
    df: pd.DataFrame,
    meta: Any = ...,
    *,
    as_polars: bool = ...,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, Any]: ...


@overload
def finalize_result(
    df: pd.DataFrame,
    meta: Any = ...,
    *,
    as_polars: bool = ...,
    return_meta: Literal[False] = ...,
) -> pd.DataFrame: ...


@overload
def finalize_result(
    df: pd.DataFrame,
    meta: Any = ...,
    *,
    as_polars: bool = ...,
    return_meta: bool = ...,
) -> pd.DataFrame | tuple[pd.DataFrame, Any]: ...


def finalize_result(
    df: pd.DataFrame,
    meta: Any = None,
    *,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, Any]:
    if as_polars:
        try:
            import polars as pl

            result_df = pl.from_pandas(df)
            if return_meta:
                return result_df, meta  # type: ignore[return-value]
            return result_df  # type: ignore[return-value,no-any-return]
        except ImportError:
            raise ImportError(
                "polars é necessário para as_polars=True. Instale com: pip install agrobr[polars]"
            ) from None

    if return_meta:
        return df, meta
    return df
