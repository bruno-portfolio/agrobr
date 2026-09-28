from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING

from agrobr import constants
from agrobr.comexstat import _numbers, _retention, models
from agrobr.exceptions import ResourceLimitError

if TYPE_CHECKING:
    from agrobr.comexstat.query import ComexQuery


class ResourceScan:
    def __init__(self, query: ComexQuery) -> None:
        self.query = query
        self.memory = _retention.Retention(query.max_memoria_bytes)
        self.rows: list[tuple[object, ...]] = []
        self.groups: dict[tuple[object, ...], list[_numbers.ExactSum]] = {}
        self.measures = ("qtd_estatistica", "kg_liquido", "valor_fob_usd") + (
            ("valor_frete_usd", "valor_seguro_usd") if query.fluxo == "importacao" else ()
        )
        self.source = {name: _numbers.ExactSum() for name in self.measures}
        self.selected = {name: _numbers.ExactSum() for name in self.measures}
        self.selected_rows = 0
        self.validated_rows = 0

    def matches(self, record: models.ExportRecord) -> bool:
        return self.query.ncm.seleciona(record.ncm) and all(
            requested is None or requested == actual
            for requested, actual in (
                (self.query.uf, record.uf),
                (self.query.pais, record.cod_pais),
                (self.query.via, record.cod_via),
                (self.query.urf, record.cod_urf),
            )
        )

    def accept(self, record: models.ExportRecord) -> None:
        if record.ano != self.query.ano:
            raise ValueError(f"ano publicado {record.ano} diverge do recurso {self.query.ano}")
        self.validated_rows += 1
        for name in self.measures:
            self.source[name].add(getattr(record, name))
        if not self.matches(record):
            return
        self.selected_rows += 1
        if self.query.max_linhas is not None and self.selected_rows > self.query.max_linhas:
            raise ResourceLimitError(
                source="comexstat", reason="Ocorrências selecionadas excedem max_linhas"
            )
        for name in self.measures:
            self.selected[name].add(getattr(record, name))
        if self.query.agregacao == "mensal":
            self.add_month(record)
        else:
            self.add_detail(record)

    def add_detail(self, record: models.ExportRecord) -> None:
        raw = (
            constants.COMEXSTAT_IMPORT_PROPERTIES
            if self.query.fluxo == "importacao"
            else constants.COMEXSTAT_EXPORT_PROPERTIES
        )
        values: list[object] = []
        self.memory.reserve(128 + len(raw) * 48)
        for field in raw:
            value = getattr(record, constants.COMEXSTAT_RENAME_MAP[field])
            if isinstance(value, str):
                value = self.memory.intern(value)
            elif isinstance(value, Decimal):
                value = _numbers.binary64(value)
            values.append(value)
        self.rows.append(tuple(values))

    def add_month(self, record: models.ExportRecord) -> None:
        key: tuple[object, ...] = (record.ano, record.mes, record.ncm, record.uf)
        if key not in self.groups:
            self.memory.reserve(2048 + len(self.measures) * 4096)
            key = tuple(
                self.memory.intern(value) if isinstance(value, str) else value for value in key
            )
            self.groups[key] = [_numbers.ExactSum() for _ in self.measures[1:]]
        for accumulator, name in zip(self.groups[key], self.measures[1:], strict=True):
            accumulator.add(getattr(record, name))

    def output_rows(self) -> list[tuple[object, ...]]:
        if self.query.agregacao == "detalhado":
            return self.rows
        self.memory.reserve(len(self.groups) * (160 + len(self.measures) * 40))
        for key in sorted(self.groups):
            values = self.groups[key]
            kg = values[0].integer()
            money = [_numbers.binary64(value.value()) for value in values[1:]]
            volume = float("nan") if kg is None else float(Decimal(f"{kg}e-3"))
            self.rows.append((*key, kg, money[0], volume, *money[1:]))
        return self.rows

    def statistics(self) -> dict[str, dict[str, dict[str, int | str | None]]]:
        return {
            "source": {name: value.details() for name, value in self.source.items()},
            "selected": {name: value.details() for name, value in self.selected.items()},
        }
