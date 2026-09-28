from __future__ import annotations

from array import array

import pandas as pd

from agrobr import constants

from . import models


class ZarcBuffers:
    def __init__(self) -> None:
        self.text: dict[str, list[str]] = {name: [] for name in constants.ZARC_STRING_COLUMNS}
        self.integers: dict[str, array[int]] = {
            name: array("b" if name in models.DEC_COLS else "q")
            for name in constants.ZARC_INTEGER_COLUMNS
        }
        self.strings: dict[str, str] = {}

    def append(self, record: models.ZarcRecord, culture: str, season: str, origin: int) -> None:
        for name, column in self.text.items():
            value = (
                culture
                if name == "cultura"
                else season
                if name == "safra"
                else getattr(record, name)
            )
            column.append(self.strings.setdefault(value, value))
        self.integers["solo_codigo"].append(record.solo_codigo)
        self.integers["ciclo_codigo"].append(record.ciclo_codigo)
        self.integers["registro_origem"].append(origin)
        for name, value in zip(models.DEC_COLS, record.riscos, strict=True):
            self.integers[name].append(-1 if value is None else value)

    def frame(self) -> pd.DataFrame:
        columns: dict[str, pd.Series] = {}
        for name in constants.ZARC_OUTPUT_COLUMNS:
            if name in self.integers:
                values = self.integers.pop(name)
                column = pd.Series(values, dtype="Int64")
                if name in models.DEC_COLS:
                    column.loc[column == -1] = None
                columns[name] = column
            else:
                columns[name] = pd.Series(self.text.pop(name), dtype=object)
        return pd.DataFrame(columns, copy=False)
