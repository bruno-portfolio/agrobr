from __future__ import annotations

import itertools
import math
import re
from typing import Any, Literal

import pandas as pd

from agrobr import constants
from agrobr.normalize import numeric

from . import models
from ._context import fail, key
from ._workbook import Aba

ROMANO = re.compile(r"^[IVX]+\s*[-–]")
GRUPO = re.compile(r"^([0-9]+)\s*[-–]")
SUBITEM = re.compile(r"^['’]?\s*([0-9]+)[.]")
LETRA = re.compile(r"[(]\s*([A-Z])\s*[)]$")
FORMULA = re.compile(r"[(]\s*([A-Z](?:\s*[+]\s*[A-Z])+)\s*=\s*([A-Z])\s*[)]$")
GESTAO = re.compile(r"GESTAO D[AE] PROPRIEDADE FAMILIAR")
LEITURAS = {
    "grupo": (("ambos", "manter"), ("subitens", "grupo"), ("grupo", "subitens")),
    "gestao": (("com_agregado", "manter"), ("sem_agregado", "gestao")),
}


def columns(sheet: Aba) -> tuple[int, dict[str, int], dict[str, str]]:
    headers: dict[str, int] = {}
    literals: dict[str, str] = {}
    start = None
    for row_index, row in enumerate(sheet.linhas[:30]):
        if "item" in headers and "valor_ha" in headers and start is not None:
            label = row[headers["item"]]
            if isinstance(label, str) and re.match(
                r"^(?:[IVX]+\s*[-–]|[0-9]+\s*[-–]|TOTAL\b|CUSTO\b)", key(label)
            ):
                return start, headers, literals
        for col, value in enumerate(row):
            if not isinstance(value, str):
                continue
            norm = key(value)
            field = None
            if norm in {"DISCRIMINACAO", "ESPECIFICACAO", "ITEM"}:
                field = "item"
            elif norm in {"CUSTO POR HA", "(R$/HA)", "R$/HA", "CUSTO/HA"}:
                field = "valor_ha"
            elif ("CUSTO /" in norm or "R$/" in norm) and "HA" not in norm:
                field = "valor_unidade_produto"
            elif "PARTICIPACAO" in norm and "CV" in norm:
                field = "participacao_cv_pct"
            elif "PARTICIPACAO" in norm and "CT" in norm:
                field = "participacao_ct_pct"
            elif norm in {"(%)", "%"}:
                field = "participacao_pct"
            elif norm in {"PACAO", "CIPACAO"} and row_index > 0:
                previous = sheet.linhas[row_index - 1][col]
                if (
                    isinstance(previous, str)
                    and key(previous).removesuffix("-") + norm == "PARTICIPACAO"
                ):
                    field = "participacao_pct"
            if field:
                if field in headers and headers[field] != col:
                    raise fail(f"Coluna duplicada {field} na aba {sheet.nome}")
                headers[field] = col
                literals[field] = value
                start = row_index + 1
    if "item" in headers and "valor_ha" in headers and start is not None:
        return start, headers, literals
    raise fail(f"Cabeçalho/unidades não reconhecidos na aba {sheet.nome}")


def percentage_format(value: str) -> bool:
    tokens = re.sub(r'"[^"]*"|\\.|_.|\*.|\[[^\]]*\]', "", value)
    sections = tokens.split(";")[:3]
    flags = {"%" in section for section in sections if section}
    if len(flags) > 1:
        raise fail("Formato percentual condicional não homologado")
    return flags == {True}


def number(value: Any, sheet: Aba, row: int, col: int, *, percentage: bool) -> float | None:
    if value == "" or value is None:
        return None
    result = (
        None
        if isinstance(value, bool) or not isinstance(value, (str, int, float))
        else numeric.parse_numeric_br(value)
    )
    if result is None:
        raise fail(f"Medida inválida em {sheet.nome}!R{row + 1}C{col + 1}: {value!r}")
    if not math.isfinite(result):
        raise fail(f"Medida não finita em {sheet.nome}!R{row + 1}C{col + 1}")
    if (
        percentage
        and not isinstance(value, str)
        and percentage_format(sheet.formatos.get((row, col), ""))
    ):
        result *= 100
        if not math.isfinite(result):
            raise fail(f"Overflow de escala percentual em {sheet.nome}!R{row + 1}C{col + 1}")
    return result


def _column_spans(
    sheet: Aba, start: int, mapping: dict[str, int], literals: dict[str, str]
) -> tuple[dict[str, tuple[int, int]], list[dict[str, Any]]]:
    spans = {}
    notes = []
    occupied: set[int] = set()
    for field, col in mapping.items():
        rows = [r for r in range(start) if sheet.linhas[r][col] == literals[field]]
        span = sheet.mesclas_cabecalho.get((rows[-1], col), (col, col + 1))
        columns = set(range(*span))
        if occupied & columns:
            raise fail(f"Cabeçalhos sobrepostos na aba {sheet.nome}: {field}")
        occupied.update(columns)
        spans[field] = span
    for col, value in enumerate(sheet.linhas[start - 1]):
        if col not in occupied and value not in (None, ""):
            if isinstance(value, str) and re.fullmatch(
                r"A PARTIR DE [0-9]{4} ESSE CUSTO FOI INATIVADO[.]?", key(value)
            ):
                notes.append(
                    {"linha": start, "coluna": col + 1, "tipo": "deactivation_note", "texto": value}
                )
            elif (
                isinstance(value, str)
                and key(value).startswith("OBS.: A PARTIR DE")
                and key(value).endswith("ESTAO NA SERIE HISTORICA CORRESPONDENTE.")
            ):
                notes.append(
                    {
                        "linha": start,
                        "coluna": col + 1,
                        "tipo": "series_relocation_note",
                        "texto": value,
                    }
                )
            else:
                raise fail(
                    f"Cabeçalho não reconhecido em {sheet.nome}!R{start}C{col + 1}: {value!r}"
                )
    return spans, notes


def _row_columns(
    row: list[Any], spans: dict[str, tuple[int, int]], sheet: str, index: int
) -> tuple[dict[str, tuple[int, Any]], list[tuple[int, Any]]]:
    found = {}
    used: set[int] = set()
    for field, (first, end) in spans.items():
        cells = [
            (col, row[col])
            for col in range(first, min(end, len(row)))
            if row[col] not in (None, "")
        ]
        if len(cells) > 1:
            raise fail(f"Mais de um valor na coluna {field}, aba {sheet}, linha {index + 1}")
        found[field] = cells[0] if cells else (first, "")
        used.update(range(first, end))
    extra = [
        (col, value) for col, value in enumerate(row) if col not in used and value not in (None, "")
    ]
    return found, extra


def _repeated_header(row: list[Any], mapping: dict[str, int], literals: dict[str, str]) -> bool:
    return all(
        isinstance(row[col], str) and key(row[col]) == key(literals[field])
        for field, col in mapping.items()
    ) and all(col in mapping.values() or value in (None, "") for col, value in enumerate(row))


def parse_selected(sheet: Aba, context: models.ContextoCusto) -> models.ResultadoCusto:
    start, mapping, literals = columns(sheet)
    spans, notes = _column_spans(sheet, start, mapping, literals)
    observations = []
    section = None
    closed = False
    raw_measures = []
    for row_index in range(start, len(sheet.linhas)):
        row = sheet.linhas[row_index]
        if _repeated_header(row, mapping, literals):
            notes.append(
                {"linha": row_index + 1, "tipo": "repeated_header", "texto": row[mapping["item"]]}
            )
            continue
        texts = [value for value in row if value not in (None, "")]
        if all(isinstance(value, str) for value in texts) and re.fullmatch(
            r"PAGINA [0-9]+ DE [0-9]+", key(" ".join(texts))
        ):
            notes.append({"linha": row_index + 1, "tipo": "page_footer", "texto": texts})
            continue
        if (
            len(texts) == 1
            and isinstance(texts[0], str)
            and key(texts[0]).startswith("ELABORACAO: CONAB/")
        ):
            notes.append(
                {
                    "linha": row_index + 1,
                    "tipo": "attribution_footer",
                    "texto": texts[0],
                    "coluna": row.index(texts[0]) + 1,
                }
            )
            continue
        cells, extra = _row_columns(row, spans, sheet.nome, row_index)
        label = cells.pop("item")[1]
        measure_fields = {name: col for name, (col, _) in cells.items()}
        values = {
            name: number(value, sheet, row_index, col, percentage=name.startswith("participacao"))
            for name, (col, value) in cells.items()
        }
        present = any(value is not None for value in values.values())
        for col, value in extra:
            if not isinstance(value, str):
                raise fail(
                    f"Medida em coluna não reconhecida na linha {row_index + 1}: "
                    f"{sheet.nome}!R{row_index + 1}C{col + 1}={value!r} "
                    "(sem cabeçalho reconhecido)"
                )
        if label == "" and not present:
            if extra:
                raise fail(f"Conteúdo fora das colunas reconhecidas na linha {row_index + 1}")
            continue
        if not isinstance(label, str) or not label.strip():
            raise fail(f"Medida sem descrição em {sheet.nome}, linha {row_index + 1}")
        norm = key(label)
        if norm == "OBS.: COTACAO DO DOLAR PARA FERTILIZANTES E DEFENSIVOS:":
            if extra or sum(value is not None for value in values.values()) > 1:
                raise fail(f"Nota cambial ambígua na aba {sheet.nome}, linha {row_index + 1}")
            notes.append(
                {
                    "linha": row_index + 1,
                    "tipo": "exchange_rate_note",
                    "texto": label,
                    "celulas": {
                        f"R{row_index + 1}C{col + 1}": value
                        for col, value in enumerate(row)
                        if value not in (None, "")
                    },
                }
            )
            continue
        group_match = GRUPO.match(norm)
        next_label = (
            sheet.linhas[row_index + 1][mapping["item"]]
            if row_index + 1 < len(sheet.linhas)
            else ""
        )
        next_item = SUBITEM.match(key(next_label)) if isinstance(next_label, str) else None
        is_group = bool(group_match and next_item and next_item.group(1) == group_match.group(1))
        if not present and is_group:
            notes.append(
                {
                    "linha": row_index + 1,
                    "texto": label,
                    "tipo": "group_header",
                    "additional_cells": extra,
                }
            )
            continue
        identified_row = bool(re.match(r"^[0-9]+(?:[.][0-9]+)?\s*[-–]", norm)) or norm.startswith(
            ("CUSTO ", "TOTAL ")
        )
        if not identified_row and ROMANO.match(norm) and not any(values.values()):
            section = label.strip()
            closed = False
            notes.append(
                {
                    "linha": row_index + 1,
                    "texto": label,
                    "tipo": "section_header",
                    "additional_cells": extra,
                    **({"valores": _published(values)} if present else {}),
                }
            )
            continue
        if not present and not identified_row:
            notes.append({"linha": row_index + 1, "texto": label, "additional_cells": extra})
            continue
        if extra:
            raise fail(
                f"Colunas não reconhecidas com conteúdo na linha {row_index + 1}: {extra[:3]}"
            )
        kind: Literal["item", "subtotal", "total"] = (
            "total"
            if norm.startswith("CUSTO ")
            else "subtotal"
            if norm.startswith("TOTAL ")
            else "item"
        )
        if kind == "item" and closed and section is not None:
            notes.append(
                {
                    "linha": row_index + 1,
                    "texto": label,
                    "tipo": "memo_after_total",
                    "valores": _published(values),
                }
            )
            continue
        closed = closed or kind != "item"
        technology = re.search(r"(ALTA|MEDIA|BAIXA) TECNOLOGIA", key(context.sistema))
        observation = models.ObservacaoCusto(
            cultura=context.cultura,
            uf=context.uf,
            safra=context.safra,
            tecnologia=technology.group(1).lower() if technology else None,
            categoria=models.classify_categoria(label, None if kind == "total" else section),
            item=label,
            unidade=literals["valor_ha"],
            valor_ha=values["valor_ha"],
            participacao_pct=values.get("participacao_pct"),
            local=context.local,
            ano_referencia=context.ano_referencia,
            referencia=context.referencia,
            data_referencia=context.data_referencia,
            planilha=context.planilha,
            aba=context.aba,
            sistema=context.sistema,
            linha=row_index + 1,
            tipo_linha=kind,
            secao=section,
            unidade_produto=literals.get("valor_unidade_produto"),
            valor_unidade_produto=values.get("valor_unidade_produto"),
            participacao_cv_pct=values.get("participacao_cv_pct"),
            participacao_ct_pct=values.get("participacao_ct_pct"),
        )
        observations.append(observation)
        raw_measures.append(
            {
                "linha": row_index + 1,
                "values": {
                    name: {
                        "column": col + 1,
                        "value": row[col],
                        "number_format": sheet.formatos.get((row_index, col), ""),
                    }
                    for name, col in measure_fields.items()
                },
            }
        )
    if not observations:
        raise fail(f"Nenhuma observação numérica na aba {sheet.nome}")
    observations, excluded, diagnostics = _readings(observations)
    kept = {observation.linha for observation in observations}
    notes.extend(excluded)
    return models.ResultadoCusto(
        contexto=context,
        observacoes=observations,
        detalhes={
            "header_row_end": start,
            "column_mapping": mapping,
            "column_spans": spans,
            "published_headers": literals,
            "source_rows": len(sheet.linhas),
            "validated_numeric_rows": len(observations),
            "notes": notes,
            "subtotal_checks": diagnostics,
            "raw_measures": [measure for measure in raw_measures if measure["linha"] in kept],
            "percentage_basis": "Excel percent-formatted numeric cells multiplied by 100; ordinary numbers unchanged",
            "totals_basis": "published_rows_only_no_recalculation",
        },
    )


def _published(values: dict[str, float | None]) -> dict[str, float]:
    return {name: value for name, value in values.items() if value is not None}


def _tolerance(parcels: int) -> float:
    return constants.CONAB_CUSTOS_TOLERANCIA_PARCELA * (parcels + 1)


def _units(items: list[models.ObservacaoCusto]) -> list[tuple[str, list[int]]]:
    labels = [key(item.item) for item in items]
    units: list[tuple[str, list[int]]] = []
    grouped: set[int] = set()
    for index, label in enumerate(labels):
        group = GRUPO.match(label)
        if not group:
            continue
        members = [index]
        for later in range(index + 1, len(labels)):
            sub = SUBITEM.match(labels[later])
            if not sub or sub.group(1) != group.group(1):
                break
            members.append(later)
        if len(members) > 1:
            units.append(("grupo", members))
            grouped.update(members)
    units.extend(
        ("gestao", [index])
        for index, label in enumerate(labels)
        if GESTAO.search(label) and index not in grouped
    )
    return units


def _dropped(kind: str, members: list[int], target: str) -> dict[int, str]:
    if target == "manter":
        return {}
    if kind == "gestao":
        return {members[0]: "aggregate_row"}
    if target == "grupo":
        return {members[0]: "group_header"}
    return dict.fromkeys(members[1:], "group_component")


def _best_reading(
    items: list[models.ObservacaoCusto], published: float
) -> tuple[bool, dict[int, str], dict[str, str]]:
    units = _units(items)
    options = [LEITURAS[kind] for kind, _ in units]
    best: tuple[tuple[int, ...], dict[int, str], dict[str, str]] | None = None
    for choice in itertools.product(*(range(len(option)) for option in options)):
        dropped: dict[int, str] = {}
        for (kind, members), picked, option in zip(units, choice, options, strict=True):
            dropped.update(_dropped(kind, members, option[picked][1]))
        kept = [item for index, item in enumerate(items) if index not in dropped]
        total = sum(item.valor_ha or 0.0 for item in kept)
        if abs(total - published) > _tolerance(len(kept)):
            continue
        rank = (sum(choice), *choice)
        if best is None or rank < best[0]:
            names = {
                items[members[0]].item.strip(): option[picked][0]
                for (kind, members), picked, option in zip(units, choice, options, strict=True)
            }
            best = (rank, dropped, names)
    if best is None:
        return False, {}, {}
    return True, best[1], best[2]


def _sections(
    observations: list[models.ObservacaoCusto],
) -> list[tuple[list[int], int]]:
    blocks: list[tuple[list[int], int]] = []
    items: list[int] = []
    section: str | None = None
    closed = False
    for index, observation in enumerate(observations):
        if observation.secao != section:
            section, items, closed = observation.secao, [], False
        if section is None or closed:
            continue
        if observation.tipo_linha == "item":
            items.append(index)
        elif observation.tipo_linha == "subtotal":
            blocks.append((items, index))
            closed = True
    return blocks


def _readings(
    observations: list[models.ObservacaoCusto],
) -> tuple[list[models.ObservacaoCusto], list[dict[str, Any]], list[dict[str, Any]]]:
    dropped: dict[int, str] = {}
    diagnostics = []
    for items, subtotal_index in _sections(observations):
        subtotal = observations[subtotal_index]
        if subtotal.valor_ha is None:
            continue
        members = [observations[index] for index in items]
        closes, removed, names = _best_reading(members, subtotal.valor_ha)
        dropped.update({items[position]: kind for position, kind in removed.items()})
        total = sum(observations[index].valor_ha or 0.0 for index in items if index not in dropped)
        diagnostics.append(
            {
                "tipo": "subtotal",
                "linha": subtotal.linha,
                "item": subtotal.item,
                "secao": subtotal.secao,
                "publicado": subtotal.valor_ha,
                "soma_itens": total,
                "fecha": closes,
                "leitura": names,
            }
        )
    kept = [observation for index, observation in enumerate(observations) if index not in dropped]
    diagnostics.extend(_formula_checks(kept))
    excluded = [
        {
            "linha": observations[index].linha,
            "texto": observations[index].item,
            "tipo": kind,
            "valores": {"valor_ha": observations[index].valor_ha},
        }
        for index, kind in sorted(dropped.items())
    ]
    return kept, excluded, diagnostics


def _formula_checks(observations: list[models.ObservacaoCusto]) -> list[dict[str, Any]]:
    letters: dict[str, float] = {}
    checks = []
    for observation in observations:
        if observation.tipo_linha == "item" or observation.valor_ha is None:
            continue
        label = key(observation.item)
        formula = FORMULA.search(label)
        single = LETRA.search(label)
        if formula:
            parts = [part.strip() for part in formula.group(1).split("+")]
            if all(part in letters for part in parts):
                expected = sum(letters[part] for part in parts)
                checks.append(
                    {
                        "tipo": "formula",
                        "linha": observation.linha,
                        "item": observation.item,
                        "secao": observation.secao,
                        "publicado": observation.valor_ha,
                        "soma_itens": expected,
                        "fecha": abs(expected - observation.valor_ha) <= _tolerance(len(parts)),
                        "leitura": {},
                    }
                )
            letters[formula.group(2)] = observation.valor_ha
        elif single:
            letters[single.group(1)] = observation.valor_ha
    return checks


def frame(observations: list[models.ObservacaoCusto]) -> pd.DataFrame:
    records = [row.model_dump() for row in observations]
    result = pd.DataFrame.from_records(records, columns=list(models.ObservacaoCusto.model_fields))
    float_columns = {
        "quantidade_ha",
        "preco_unitario",
        "valor_ha",
        "participacao_pct",
        "valor_unidade_produto",
        "participacao_cv_pct",
        "participacao_ct_pct",
    }
    for name in result.columns:
        if name in float_columns:
            result[name] = pd.Series(result[name], dtype="float64")
        elif name in {"ano_referencia", "linha"}:
            result[name] = pd.Series(result[name], dtype="Int64")
        elif name == "data_referencia":
            result[name] = pd.to_datetime(result[name]).astype("datetime64[ns]")
        else:
            result[name] = pd.Series(result[name], dtype="string[python]")
    return result


def totals(result: models.ResultadoCusto) -> dict[str, Any]:
    output: dict[str, Any] = result.contexto.model_dump(mode="json")
    output.update(coe_ha=None, cv_ha=None, cot_ha=None, ct_ha=None)
    fields = {
        "CUSTO OPERACIONAL EFETIVO": "coe_ha",
        "CUSTO VARIAVEL": "cv_ha",
        "CUSTO OPERACIONAL": "cot_ha",
        "CUSTO TOTAL": "ct_ha",
    }
    seen = set()
    published = []
    for observation in result.observacoes:
        if observation.tipo_linha != "total":
            continue
        norm = key(observation.item)
        published.append(observation.model_dump(mode="json"))
        matched = next((field for prefix, field in fields.items() if norm.startswith(prefix)), None)
        if matched is not None:
            if matched in seen:
                raise fail(f"Total publicado ambíguo: {matched}")
            seen.add(matched)
            output[matched] = observation.valor_ha
    output["totais_publicados"] = published
    return output
