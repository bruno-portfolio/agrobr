from __future__ import annotations

import re
import warnings
from collections import Counter
from datetime import date, datetime
from typing import Any, cast

import pandas as pd

from agrobr import _log
from agrobr.exceptions import ParseError
from agrobr.normalize.numeric import safe_float
from agrobr.utils.io import open_excel_safe
from agrobr.utils.result import ATRIBUTO_AVISOS

from .models import normalize_produto

logger = _log.get_logger(__name__)

PARSER_VERSION = 2
_MEDIDAS = ("pct", "plantio_pct", "colheita_pct")


def parse_pc_xls(data: bytes) -> pd.DataFrame:
    result, _ = parse_pc_xls_with_engine(data)
    return result


def parse_pc_xls_with_engine(data: bytes) -> tuple[pd.DataFrame, str]:
    try:
        xls = open_excel_safe(data, source="deral", parser_version=PARSER_VERSION)
    except ParseError as exc:
        raise ParseError(
            source="deral",
            parser_version=PARSER_VERSION,
            reason=f"Falha ao abrir PC.xls: {exc}",
        ) from exc

    with xls:
        return _parse_pc_workbook(xls), str(cast(Any, xls).engine)


def _parse_pc_workbook(xls: pd.ExcelFile) -> pd.DataFrame:
    quadros: list[tuple[str, str, list[dict[str, Any]]]] = []

    for sheet_name in xls.sheet_names:
        try:
            df = pd.read_excel(xls, sheet_name=sheet_name, header=None)
        except Exception as exc:
            logger.warning("deral_sheet_error", sheet=sheet_name, error=str(exc))
            raise ParseError(
                source="deral",
                parser_version=PARSER_VERSION,
                reason=f"Falha ao ler a aba {sheet_name}: {exc}",
            ) from exc

        if _is_multi_produto_sheet(df):
            aba = str(sheet_name)
            quadros.append(
                (aba, _find_data_referencia(df, aba), _extract_multi_produto_sheet(df, aba))
            )
        else:
            logger.debug("deral_skip_sheet", sheet=sheet_name)

    all_records, avisos = _datar_quadros(quadros)
    if not all_records:
        raise ParseError(
            source="deral",
            parser_version=PARSER_VERSION,
            reason=f"Nenhum registro reconhecido nas abas: {xls.sheet_names}",
        )

    result = pd.DataFrame(all_records)
    result["data"] = pd.to_datetime(result["data"], format="%d/%m/%Y").dt.as_unit("ns")
    result = result.sort_values(["produto", "data", "condicao"]).reset_index(drop=True)
    for aviso in avisos:
        result.attrs.setdefault(ATRIBUTO_AVISOS, []).append(aviso)
        warnings.warn(aviso, UserWarning, stacklevel=2)

    logger.info("deral_parse_ok", records=len(result))
    return result


def _datar_quadros(
    quadros: list[tuple[str, str, list[dict[str, Any]]]],
) -> tuple[list[dict[str, Any]], list[str]]:
    """Fixa a data de cada quadro (aba, data da célula, registros) e devolve os registros e os avisos.

    Vale a data da célula de referência da aba. O DERAL monta a aba de um ano sobre a de outro e às
    vezes deixa a data copiada: quando a data da célula é também a de outra aba e o nome da aba diz
    outra data, vale a do nome. Duas abas que ainda assim fiquem com a mesma data levantam
    ``ParseError``. A aba sem registros (feriado sem observações) não entra na conta.
    """
    all_records: list[dict[str, Any]] = []
    avisos: list[str] = []
    quadros = [quadro for quadro in quadros if quadro[2]]
    vezes = Counter(data_ref for _, data_ref, _ in quadros)
    abas_da_data: dict[str, list[str]] = {}
    for aba, data_ref, records in quadros:
        data_do_nome = _data_do_nome_da_aba(aba)
        if data_do_nome is not None and data_do_nome != data_ref and vezes[data_ref] > 1:
            avisos.append(
                f"deral: a aba {aba!r} publica na planilha a data {data_ref}, que é a de outra "
                f"aba; o quadro usa a data do nome da aba, {data_do_nome}."
            )
            data_ref = data_do_nome
            for record in records:
                record["data"] = data_ref
        elif aviso := _aviso_de_aba_divergente(aba, data_ref):
            avisos.append(aviso)
        abas_da_data.setdefault(data_ref, []).append(aba)
        all_records.extend(records)

    for data_ref, abas in abas_da_data.items():
        if len(abas) > 1:
            raise ParseError(
                source="deral",
                parser_version=PARSER_VERSION,
                reason=f"As abas {abas} publicam a mesma data de referência, {data_ref}",
            )
    return all_records, avisos


def _is_multi_produto_sheet(df: pd.DataFrame) -> bool:
    if len(df) < 6 or len(df.columns) < 7:
        return False

    for row_idx in range(min(8, len(df))):
        row_text = " ".join(str(v).lower() for v in df.iloc[row_idx] if pd.notna(v))
        if "safras" in row_text and "condi" in row_text:
            return True
        if "condi" in row_text and ("boa" in row_text or "ruim" in row_text):
            return True
        if "plantada" in row_text and "colhida" in row_text:
            return True
    return False


def _extract_multi_produto_sheet(
    df: pd.DataFrame,
    sheet_name: str,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []

    header_row = -1
    col_ruim = col_media = col_boa = -1
    col_plantada = col_colhida = -1

    for row_idx in range(min(10, len(df))):
        for col_idx in range(len(df.columns)):
            cell_str = str(df.iloc[row_idx, col_idx]).strip().lower()
            if cell_str == "ruim":
                col_ruim = col_idx
                header_row = row_idx
            elif cell_str in ("média", "media"):
                col_media = col_idx
            elif cell_str == "boa":
                col_boa = col_idx
            elif cell_str == "plantada":
                col_plantada = col_idx
            elif cell_str == "colhida":
                col_colhida = col_idx

    columns = {
        "ruim": col_ruim,
        "média": col_media,
        "boa": col_boa,
        "plantada": col_plantada,
        "colhida": col_colhida,
    }
    missing = [label for label, column in columns.items() if column < 0]
    if missing:
        raise ParseError(
            source="deral",
            parser_version=PARSER_VERSION,
            reason=f"Cabeçalhos obrigatórios ausentes na aba {sheet_name}: {', '.join(missing)}",
        )

    data_ref = _find_data_referencia(df, sheet_name)

    for row_idx in range(header_row + 1, len(df)):
        cell0 = df.iloc[row_idx, 0]
        if pd.isna(cell0):
            continue
        cell_str = str(cell0).strip()

        if not cell_str or cell_str.upper().startswith("SAFRA"):
            continue

        produto = _detect_produto_from_row_label(cell_str)
        if produto is None:
            continue

        for col_idx, condicao in [
            (col_ruim, "ruim"),
            (col_media, "media"),
            (col_boa, "boa"),
        ]:
            pct = _published_percentage(df.iloc[row_idx, col_idx])
            records.append(
                {
                    "produto": normalize_produto(produto),
                    "data": data_ref,
                    "condicao": condicao,
                    "pct": pct,
                    "plantio_pct": _published_percentage(df.iloc[row_idx, col_plantada]),
                    "colheita_pct": _published_percentage(df.iloc[row_idx, col_colhida]),
                }
            )

    if all(record[medida] is None for record in records for medida in _MEDIDAS):
        return []
    return records


def _detect_produto_from_row_label(label: str) -> str | None:
    s = label.strip().lower()

    from .models import _PRODUTO_ALIASES

    without_parentheses = re.sub(r"[()]", "", s)
    if without_parentheses in _PRODUTO_ALIASES:
        return _PRODUTO_ALIASES[without_parentheses]

    safra_match = re.search(r"([12])\s*[ªaºo�]?\s*safra", without_parentheses)
    base = re.sub(r"[12]\s*[ªaºo�]?\s*safra", "", without_parentheses).strip()
    if base in _PRODUTO_ALIASES:
        canonical = _PRODUTO_ALIASES[base]
        if safra_match and canonical in {"feijao", "milho"}:
            return f"{canonical}_{safra_match.group(1)}"
        if safra_match and canonical == "soja" and safra_match.group(1) == "2":
            return None
        return canonical

    for alias, canonical in _PRODUTO_ALIASES.items():
        if alias in without_parentheses:
            return canonical

    return None


def _find_data_referencia(df: pd.DataFrame, sheet_name: str) -> str:
    for row in df.itertuples(index=False, name=None):
        for cell in row:
            if pd.isna(cell):
                continue
            if isinstance(cell, (datetime, date)):
                return cell.strftime("%d/%m/%Y")
            if not isinstance(cell, str):
                continue
            match = re.search(r"(?<!\d)(\d{1,2})([/-])(\d{1,2})\2(\d{4}|\d{2})(?!\d)", cell)
            if not match:
                continue
            year = int(match.group(4))
            if len(match.group(4)) == 2:
                year += 2000
            try:
                reference = date(year, int(match.group(3)), int(match.group(1)))
            except ValueError:
                continue
            return reference.strftime("%d/%m/%Y")
    raise ParseError(
        source="deral",
        parser_version=PARSER_VERSION,
        reason=f"Data de referência não encontrada na aba {sheet_name}",
    )


def _data_do_nome_da_aba(sheet_name: str) -> str | None:
    match = re.fullmatch(r"(\d{1,2})-(\d{1,2})-(\d{4}|\d{2})", sheet_name.strip())
    if match is None:
        return None
    dia, mes, ano = match.groups()
    try:
        return date(int(ano) if len(ano) == 4 else 2000 + int(ano), int(mes), int(dia)).strftime(
            "%d/%m/%Y"
        )
    except ValueError:
        return None


def _aviso_de_aba_divergente(sheet_name: str, data_ref: str) -> str | None:
    if _data_do_nome_da_aba(sheet_name) in (None, data_ref):
        return None
    return (
        f"deral: a aba {sheet_name!r} publica a data {data_ref} na planilha; "
        "o quadro usa a data da planilha, e não o nome da aba."
    )


def _published_percentage(value: Any) -> float | None:
    if isinstance(value, str) and value.strip() == "-":
        return 0.0
    return safe_float(value, strip="%")


def filter_by_produto(df: pd.DataFrame, produto: str) -> pd.DataFrame:
    if df.empty or not produto:
        return df
    key = normalize_produto(produto)
    if key in {"feijao", "milho"}:
        return df[df["produto"].isin([key, f"{key}_1", f"{key}_2"])].reset_index(drop=True)
    return df[df["produto"] == key].reset_index(drop=True)
