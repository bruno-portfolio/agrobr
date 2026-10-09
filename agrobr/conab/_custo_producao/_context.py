from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any

from agrobr import constants
from agrobr.exceptions import InvalidParameterError, ParseError
from agrobr.normalize.regions import remover_acentos

from . import models
from ._workbook import Aba, Workbook


def key(value: str) -> str:
    return " ".join(remover_acentos(value).upper().split())


def fail(reason: str) -> ParseError:
    return ParseError(
        source="conab_custo", parser_version=constants.CONAB_CUSTOS_PARSER_VERSION, reason=reason
    )


def _locality(value: str, sheet_name: str) -> tuple[str, str, bool]:
    text = value.split(":", 1)[1]
    match = re.fullmatch(r"\s*(.+?)\s*[-–]\s*([A-Z]{2})\s*", text)
    if not match:
        match = re.fullmatch(r"\s*(.+?)\s+([A-Z]{2})\s*", text)
    if match:
        return match.group(1).strip(), match.group(2), False
    parenthesis = re.search(r"\(([A-Z]{2})\)", text)
    if parenthesis:
        return text.strip(), parenthesis.group(1), False
    sheet_match = re.fullmatch(r"(?P<local>.+)-(?P<uf>[A-Z]{2})-(?P<ano>[0-9]{4})", sheet_name)
    local = text.split("(", 1)[0].strip()
    if sheet_match and local:
        return local, sheet_match.group("uf"), True
    raise fail(f"Local não identificado: {value!r}")


def _follows_cost_title(sheet: Aba, row: int, column: int) -> bool:
    if row == 0 or column >= len(sheet.linhas[row - 1]):
        return False
    previous = sheet.linhas[row - 1][column]
    return isinstance(previous, str) and key(previous).startswith("CUSTO DE PRODUCAO")


def context(sheet: Aba, resource: models.RecursoCusto, index: int) -> models.ContextoCusto:
    header_end = next(
        (
            r
            for r, row in enumerate(sheet.linhas[:30])
            if any(
                isinstance(value, str) and key(value) in {"DISCRIMINACAO", "ESPECIFICACAO", "ITEM"}
                for value in row
            )
        ),
        30,
    )
    cells = [
        (r, c, value)
        for r, row in enumerate(sheet.linhas[:header_end])
        for c, value in enumerate(row)
        if value != ""
    ]
    texts = [(r, c, value) for r, c, value in cells if isinstance(value, str)]
    locations = []
    harvests = []
    evidence = {}
    system = None
    reference = None
    reference_date = None
    year = None
    references_seen = set()
    for r, c, value in texts:
        norm = key(value)
        product = (
            "CAFE"
            if resource.cultura in {"cafe_arabica", "cafe_conilon"}
            else resource.cultura.replace("_", " ").upper()
        )
        product_pattern = r"MILHO[12]?" if product == "MILHO" else re.escape(product)
        if re.search(rf"\b{product_pattern}\b", norm) and (
            "PLANTIO" in norm
            or "TECNOLOGIA" in norm
            or " - " in norm
            or norm.startswith(("PRODUTO:", "PRODUTO "))
            or _follows_cost_title(sheet, r, c)
        ):
            if system is not None and system != value.strip():
                raise fail(f"Sistemas conflitantes na aba {sheet.nome}")
            system = value.strip()
            evidence["sistema"] = f"R{r + 1}C{c + 1}"
        if re.match(r"^(?:[1-3][ªºA]?\s+)?SAFRA\b", norm):
            match = re.search(r"(?<![0-9])([0-9]+/[0-9]+|[0-9]{4})(?![0-9])", value)
            if match:
                harvests.append(match.group(1))
            loc = re.search(
                r"-\s*(.+?)\s*-\s*([A-Z]{2})\s*$", value[match.end() :] if match else value
            )
            if loc:
                locations.append((loc.group(1).strip(), loc.group(2)))
                evidence["local"] = f"R{r + 1}C{c + 1}"
        if norm.startswith(("LOCAL:", "REGIAO:")):
            local, uf, inferred = _locality(value, sheet.nome)
            locations.append((local, uf))
            if inferred:
                evidence["uf_origem"] = "nome_da_aba"
            evidence["local"] = f"R{r + 1}C{c + 1}"
        if norm.startswith("MES/ANO:"):
            reference = value.split(":", 1)[1].strip()
            match = re.fullmatch(r"[^/]+/([0-9]{4})", reference)
            if not match:
                raise fail(f"Referência inválida: {reference}")
            year = int(match.group(1))
            evidence["referencia"] = f"R{r + 1}C{c + 1}"
            references_seen.add(reference)
        if norm.startswith("A PRECOS DE"):
            dates = [
                v for rr, cc, v in cells if rr == r and cc > c and isinstance(v, (datetime, date))
            ]
            if len(dates) == 1:
                reference_date = dates[0].date() if isinstance(dates[0], datetime) else dates[0]
                reference = reference_date.isoformat()
                year = reference_date.year
                evidence["referencia"] = f"R{r + 1}C{c + 2}"
                references_seen.add(reference)
            else:
                references = [
                    v.strip()
                    for rr, cc, v in cells
                    if rr == r
                    and cc > c
                    and isinstance(v, str)
                    and re.fullmatch(
                        r"[^/]+/[0-9]{4}|[0-9]{1,2}[./][0-9]{1,2}[./][0-9]{4}", v.strip()
                    )
                ]
                if len(references) == 1:
                    reference = references[0]
                    year = int(reference[-4:])
                    evidence["referencia"] = f"R{r + 1}C{c + 2}"
                    references_seen.add(reference)
    if len(references_seen) > 1:
        raise fail(f"Referências conflitantes na aba {sheet.nome}")
    if (
        len(set(locations)) != 1
        or len(set(harvests)) > 1
        or not system
        or reference is None
        or year is None
    ):
        raise fail(f"Contexto não unívoco ou incompleto na aba {sheet.nome}")
    local, uf = locations[0]
    return models.ContextoCusto(
        cultura=resource.cultura,
        planilha=resource.planilha,
        aba=sheet.nome,
        indice_aba=index,
        local=local,
        uf=uf,
        ano_referencia=year,
        referencia=reference,
        data_referencia=reference_date,
        safra=harvests[0] if harvests else None,
        sistema=system,
        celulas_contexto=evidence,
    )


def inventory(
    book: Workbook, resource: models.RecursoCusto
) -> tuple[list[models.ContextoCusto], list[dict[str, Any]]]:
    contexts = []
    rejected = []
    for index, name in enumerate(book.names):
        if key(name) in {"INDICE", "INDEX", "SUMARIO"}:
            continue
        try:
            contexts.append(context(book.read(name, head=True), resource, index))
        except ParseError as error:
            rejected.append({"aba": name, "indice_aba": index, "error": str(error)})
    return contexts, rejected


LIMITE_DE_ROTULOS = 15


def lista_curta(itens: list[str]) -> str:
    resto = len(itens) - LIMITE_DE_ROTULOS
    return ", ".join(itens[:LIMITE_DE_ROTULOS]) + (f" e mais {resto}" if resto > 0 else "")


def candidatos(
    contexts: list[models.ContextoCusto], query: models.ConsultaCusto
) -> list[models.ContextoCusto]:
    candidates = []
    for candidate in contexts:
        if query.aba is not None and query.aba != candidate.aba:
            continue
        if query.local is not None and key(query.local) != key(candidate.local):
            continue
        if query.uf is not None and key(query.uf) != candidate.uf:
            continue
        if query.ano is not None and query.ano != candidate.ano_referencia:
            continue
        if query.safra is not None and query.safra != candidate.safra:
            continue
        candidates.append(candidate)
    return candidates


def select(
    contexts: list[models.ContextoCusto], query: models.ConsultaCusto
) -> models.ContextoCusto:
    candidates = candidatos(contexts, query)
    if len(candidates) != 1:
        labels = [f"{c.aba}: {c.local}/{c.uf}, {c.referencia}" for c in candidates[:15]]
        raise InvalidParameterError(
            f"Seleção de custos requer uma aba/contexto único; {len(candidates)} candidatos. {labels}"
        )
    return candidates[0]
