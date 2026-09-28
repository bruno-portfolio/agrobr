from __future__ import annotations

import io
import json
import zipfile
from typing import Any

import pandas as pd


def frame_bytes(frame: pd.DataFrame) -> bytes:
    return frame.to_json(
        orient="split", index=False, force_ascii=False, double_precision=15, date_format="iso"
    ).encode("utf-8")


def load_frame(
    raw: bytes,
    file_format: str,
    json_path: list[str | int],
    filters: dict[str, str | int | float],
) -> pd.DataFrame:
    if file_format == "csv":
        frame = pd.read_csv(io.BytesIO(raw), dtype=str)
    elif file_format == "json":
        records = json.loads(raw)
        for key in json_path:
            records = records[key]
        frame = pd.DataFrame(records)
    else:
        raise ValueError(f"Unsupported frame format: {file_format}")
    for column, value in filters.items():
        frame = frame.loc[frame[column] == value]
    if frame.empty:
        raise ValueError("Input selection is empty")
    return frame.reset_index(drop=True)


def mutate_frame(
    frame: pd.DataFrame,
    cells: dict[str, str | int | float],
    drop_columns: list[str],
    *,
    noop: bool,
) -> bytes:
    altered = frame.copy(deep=True)
    for address, value in cells.items():
        row_text, separator, column = address.partition("/")
        if not separator or column not in altered.columns:
            raise ValueError(f"Unknown frame cell: {address}")
        row = int(row_text)
        if row < 0 or row >= len(altered):
            raise ValueError(f"Frame row is out of bounds: {row}")
        altered.at[row, column] = value
    if not set(drop_columns) <= set(altered.columns):
        raise ValueError("Unknown column selected for removal")
    altered = altered.drop(columns=drop_columns)
    if frame_bytes(altered) == frame_bytes(frame):
        raise ValueError("Frame mutation is equivalent to the input")
    return frame_bytes(frame if noop else altered)


def replace_frame_values(original: pd.DataFrame, payload: bytes) -> pd.DataFrame:
    old = json.loads(frame_bytes(original))
    new = json.loads(payload)
    if len(new["data"]) != len(old["data"]):
        raise ValueError("Frame mutation cannot change the row count")
    if not set(new["columns"]) <= set(old["columns"]):
        raise ValueError("Frame mutation cannot add columns")
    altered = original.loc[:, new["columns"]].copy(deep=True)
    for new_column, name in enumerate(new["columns"]):
        old_column = old["columns"].index(name)
        for row, values in enumerate(new["data"]):
            if values[new_column] != old["data"][row][old_column]:
                altered.iat[row, new_column] = values[new_column]
    altered.attrs = original.attrs.copy()
    return altered


def mutate_bytes(
    raw: bytes,
    before_hex: str,
    after_hex: str,
    offset: int,
    archive_member: str | None,
    *,
    noop: bool,
) -> tuple[bytes, dict[str, Any]]:
    before, after = bytes.fromhex(before_hex), bytes.fromhex(after_hex)
    if not before or before == after or offset < 0:
        raise ValueError("Byte mutation must change a nonempty span at a valid offset")
    content = raw
    if archive_member:
        with zipfile.ZipFile(io.BytesIO(raw)) as archive:
            content = archive.read(archive_member)
    if content[offset : offset + len(before)] != before:
        raise ValueError("Original bytes do not match the declared span")
    changed = content[:offset] + after + content[offset + len(before) :]
    if archive_member:
        output = io.BytesIO()
        with zipfile.ZipFile(io.BytesIO(raw)) as archive, zipfile.ZipFile(output, "w") as target:
            for entry in archive.infolist():
                target.writestr(
                    entry,
                    changed if entry.filename == archive_member else archive.read(entry.filename),
                )
        changed = output.getvalue()
    return (raw if noop else changed), {
        "archive_member": archive_member,
        "offset": offset,
        "before_hex": before_hex,
        "after_hex": after_hex,
    }
