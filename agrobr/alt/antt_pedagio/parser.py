from __future__ import annotations

import csv
import io
import re
from collections.abc import Callable
from functools import lru_cache
from typing import Any, BinaryIO, Literal

import pandas as pd
import pydantic

from agrobr import constants
from agrobr.exceptions import InvalidParameterError

from . import _csv, _traffic, models

PARSER_VERSION = 3
_ABOVE_AXLES = re.compile(r"ve[ií]culo\s+comercial\s+acima\s+de\s+([0-9]+)\s+eixos?", re.IGNORECASE)


def parse_trafego_file(
    handle: BinaryIO,
    *,
    ano: int,
    frequencia: Literal["mensal", "diaria"],
    keep: Callable[[models.TrafegoRecord], bool] | None = None,
    max_rows: int = constants.ANTT_PARSER_MAX_ROWS,
    max_memory_bytes: int = constants.ANTT_PARSER_MAX_MEMORY_BYTES,
) -> models.ParsedTraffic:
    if type(ano) is not int or not 1678 <= ano <= 2261:
        raise InvalidParameterError("ano deve ser inteiro civil suportado")
    if frequencia not in ("mensal", "diaria"):
        raise InvalidParameterError("frequencia deve ser mensal ou diaria")
    if type(max_rows) is not int or max_rows < 0:
        raise InvalidParameterError("max_rows deve ser inteiro não negativo")
    if type(max_memory_bytes) is not int or max_memory_bytes <= 0:
        raise InvalidParameterError("max_memory_bytes deve ser inteiro positivo")
    if keep is not None and not callable(keep):
        raise InvalidParameterError("keep deve ser callable ou None")
    try:
        encoding = _csv.detect_file_encoding(handle)
        with _csv.open_reader(handle, encoding) as reader:
            return _traffic.scan(
                reader,
                ano=ano,
                frequencia=frequencia,
                encoding=encoding,
                keep=keep,
                max_rows=max_rows,
                max_memory_bytes=max_memory_bytes,
            )
    except (csv.Error, UnicodeError, LookupError) as exc:
        raise _csv.fail(f"CSV inválido: {type(exc).__name__}: {exc}") from exc


def _has_heavy_lower_bound(category: str | None) -> bool:
    match = _ABOVE_AXLES.fullmatch(category.strip()) if category else None
    if match is None:
        return False
    digits = match[1].lstrip("0")
    return len(digits) > 1 or (len(digits) == 1 and digits >= "2")


def heavy_vehicle_status(record: models.TrafegoRecord) -> bool | None:
    if record.tipo_veiculo in ("Passeio", "Moto"):
        return False
    if record.tipo_veiculo != "Comercial":
        return None
    if record.n_eixos is not None:
        return record.n_eixos >= 3
    if _has_heavy_lower_bound(record.categoria_eixo):
        return True
    return None


def parse_pracas_file(handle: BinaryIO) -> pd.DataFrame:
    try:
        encoding = _csv.detect_file_encoding(handle)
        first = handle.readline(constants.ANTT_CSV_READ_CHUNK).decode(encoding)
        handle.seek(0)
        separator = ";" if first.count(";") >= first.count(",") else ","
        with _csv.open_reader(handle, encoding, separator) as reader:
            header: list[str] = next(reader, [])
            aliases = {"latitude": "lat", "longitude": "lon", "praca": "praca_de_pedagio"}
            names = [aliases.get(name, name) for name in header]
            if not names or len(set(names)) != len(names):
                raise _csv.fail("Cadastro vazio ou cabeçalho duplicado/ambíguo")
            if set(names) - set(models.PracaRecord.model_fields):
                raise _csv.fail("Cadastro com colunas desconhecidas")
            if not {"concessionaria", "praca_de_pedagio"} <= set(names):
                raise _csv.fail("Cadastro sem colunas obrigatórias de vínculo")
            rows = _read_praca_records(reader, names)
        frame = pd.DataFrame(
            {
                name: pd.Series(
                    [row[name] for row in rows],
                    dtype="float64" if name in ("lat", "lon") else pd.StringDtype(storage="python"),
                )
                for name in names
            }
        )
        if "municipio" not in frame:
            frame["municipio"] = (
                frame["municipal"]
                if "municipal" in frame
                else pd.Series(pd.NA, index=frame.index, dtype="string")
            )
        frame.attrs["parsing"] = {
            "encoding": encoding,
            "validated_rows": len(rows),
            "eof_reached": True,
        }
        return frame
    except (csv.Error, UnicodeError, LookupError) as exc:
        raise _csv.fail(f"CSV cadastro inválido: {type(exc).__name__}: {exc}") from exc


def _read_praca_records(reader: Any, names: list[str]) -> list[dict[str, Any]]:
    rows = []
    for index, values in enumerate(reader, 1):
        if not values:
            continue
        if values == names:
            raise _csv.fail(f"Cadastro registro {index}: cabeçalho repetido")
        if len(values) != len(names):
            raise _csv.fail(
                f"Cadastro registro {index}: largura {len(values)}, esperada {len(names)}"
            )
        try:
            record = models.PracaRecord.model_validate(dict(zip(names, values, strict=True)))
        except pydantic.ValidationError as exc:
            fields = [".".join(map(str, error["loc"])) for error in exc.errors(include_input=False)]
            raise _csv.fail(f"Cadastro registro {index}: campos inválidos {fields}") from exc
        rows.append(record.model_dump())
    return rows


def parse_pracas(content: bytes) -> pd.DataFrame:
    return parse_pracas_file(io.BytesIO(content))


_NOMES_ANTERIORES_CONCESSIONARIA: dict[str, str] = {
    "AUTOPISTA FERNÃO DIAS": "MOTIVA MINAS SP",
    "CRO": "NOVA ROTA DO OESTE",
    "ECO050": "ECOVIAS MINAS GOIÁS",
    "ECO101": "ECOVIAS CAPIXABA",
    "ECOPONTE": "ECOVIAS PONTE",
    "ECORIOMINAS": "ECOVIAS RIO MINAS",
    "MSVIA": "PANTANAL",
}
_NOMES_ATUAIS = {
    antigo.casefold(): atual.casefold()
    for antigo, atual in _NOMES_ANTERIORES_CONCESSIONARIA.items()
}


@lru_cache(maxsize=4096)
def chave_praca(concessionaria: str, praca: str) -> tuple[str, str]:
    """Chave do cadastro de praças: sem diferença de caixa nem de espaços, sem tirar acento.

    O nome anterior da concessionária (o tráfego de 2023 ainda publica ``CRO``) vira o do cadastro.
    """
    nome = " ".join(concessionaria.split()).casefold()
    return _NOMES_ATUAIS.get(nome, nome), " ".join(praca.split()).casefold()


def _nullable_text(value: Any) -> str | None:
    return None if value is None or pd.isna(value) else str(value)


def build_pracas_enrichment(
    frame: pd.DataFrame,
) -> tuple[dict[tuple[str, str], tuple[str | None, str | None, str | None]], dict[str, Any]]:
    plazas = "praca_de_pedagio" if "praca_de_pedagio" in frame else "praca"
    groups: dict[tuple[str, str], set[tuple[str | None, str | None, str | None]]] = {}
    missing = 0
    for row in frame.to_dict("records"):
        concession, plaza = (
            _nullable_text(row.get("concessionaria")),
            _nullable_text(row.get(plazas)),
        )
        if not concession or not concession.strip() or not plaza or not plaza.strip():
            missing += 1
            continue
        values = (
            _nullable_text(row.get("rodovia")),
            _nullable_text(row.get("uf")),
            _nullable_text(row.get("municipio")),
        )
        groups.setdefault(chave_praca(concession, plaza), set()).add(values)
    mapping = {key: next(iter(values)) for key, values in groups.items() if len(values) == 1}
    conflicts = [key for key, values in groups.items() if len(values) > 1]
    return mapping, {
        "matching": (
            "concessionaria/praca with casefold and collapsed spaces, former concessionaire names "
            "mapped to the registry name; no accent folding/fuzzy/first or row multiplication"
        ),
        "source_rows": len(frame),
        "matched_unique_keys": len(mapping),
        "missing_key_rows": missing,
        "conflicting_keys": len(conflicts),
        "conflict_examples": [list(key) for key in conflicts[:10]],
        "conflict_examples_omitted": max(0, len(conflicts) - 10),
    }
