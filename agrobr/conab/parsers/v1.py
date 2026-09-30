from __future__ import annotations

import math
import re
from datetime import date
from decimal import Decimal
from io import BytesIO
from typing import Any, cast

import pandas as pd
import pydantic

from agrobr import _log, constants
from agrobr.conab import structure
from agrobr.exceptions import ParseError
from agrobr.models import Safra
from agrobr.normalize import dates, regions
from agrobr.normalize.dates import anos_para_safra
from agrobr.normalize.numeric import safe_float
from agrobr.utils.io import read_excel_safe

logger = _log.get_logger(__name__)

_SECTION_RE = re.compile(r"^\d+\.\s+\S")


def _cell_str(row: pd.Series, idx: int) -> str:
    return str(row.iloc[idx]).strip() if pd.notna(row.iloc[idx]) else ""


def _parse_safra_cell(cell: str) -> str | None:
    if "Safra" in cell or ("/" in cell and "VAR" not in cell.upper()):
        safra_match = cell.replace("Safra ", "").strip()
        if "/" in safra_match:
            parts = safra_match.split("/")
            if len(parts) != 2:
                return None
            ano1 = parts[0].strip()
            ano2 = parts[1].strip()
            if len(ano1) == 2:
                ano1 = "20" + ano1
            return f"{ano1}/{ano2}"
        year_str = safra_match.replace(".0", "").strip()
        if re.match(r"^\d{4}$", year_str):
            return anos_para_safra(int(year_str))
        return None
    year_str = cell.replace(".0", "").strip()
    if re.match(r"^\d{4}$", year_str):
        return year_str
    return None


def _build_suprimento(
    produto: str,
    safra: str,
    data: dict[str, Decimal | None],
) -> dict[str, Any]:
    est_ini = data.get("estoque_inicial")
    prod = data.get("producao")
    imp = data.get("importacao")

    sup = None
    if est_ini is not None and prod is not None and imp is not None:
        sup = est_ini + prod + imp

    sem = data.get("sementes_outros")
    proc = data.get("processamento")
    consumo = None
    if sem is not None and proc is not None:
        consumo = sem + proc

    return {
        "produto": produto.upper(),
        "safra": safra,
        "estoque_inicial": est_ini,
        "producao": prod,
        "importacao": imp,
        "suprimento_total": sup,
        "consumo": consumo,
        "exportacao": data.get("exportacao"),
        "estoque_final": data.get("estoque_final"),
        "unidade": "mil_ton",
    }


class ConabParserV1:
    version: int = constants.CONAB_SAFRA_PARSER_VERSION
    source: str = "conab"
    valid_from: date = date(2020, 1, 1)
    valid_until: date | None = None

    def parse_safra_produto(
        self,
        xlsx: BytesIO,
        produto: str,
        safra_ref: str | None = None,
        levantamento: int | None = None,
        data_publicacao: date | None = None,
    ) -> list[Safra]:
        sheet_name = constants.CONAB_PRODUTOS.get(produto.lower())
        if not sheet_name:
            raise ParseError(
                source="conab",
                parser_version=self.version,
                reason=f"Produto não suportado: {produto}",
            )

        sheet_name = self._aba_da_safra(xlsx, sheet_name, safra_ref)
        df = read_excel_safe(
            xlsx,
            source="conab",
            parser_version=self.version,
            label=f"aba {sheet_name}",
            sheet_name=sheet_name,
            header=None,
        )

        header_row = self._find_header_row(df)
        if header_row is None:
            raise ParseError(
                source="conab",
                parser_version=self.version,
                reason=f"Não encontrou header na aba {sheet_name}",
            )

        safras = []
        data_row = header_row + 3
        if not any(
            _cell_str(df.iloc[index], 0).upper() in constants.CONAB_UFS
            for index in range(data_row, len(df))
        ):
            raise ParseError(
                source="conab",
                parser_version=self.version,
                reason=f"Nenhuma linha de UF na aba {sheet_name}",
            )

        safra_cols = self._extract_safra_columns(df, header_row)

        for idx in range(data_row, len(df)):
            row = df.iloc[idx]
            uf = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else None

            if not uf or uf in ["NaN", "nan", ""]:
                continue

            if uf.upper() in constants.CONAB_REGIOES:
                continue

            if uf.upper() not in constants.CONAB_UFS and not any(c.isalpha() for c in uf):
                continue

            for safra_str, cols in safra_cols.items():
                if safra_ref and safra_str != safra_ref:
                    continue

                area = self._parse_decimal(row.iloc[cols["area"]])
                produtividade = self._parse_decimal(row.iloc[cols["produtividade"]])
                producao = self._parse_decimal(row.iloc[cols["producao"]])

                if area is None and producao is None:
                    continue

                try:
                    safra = Safra(
                        fonte=constants.Fonte.CONAB,
                        produto=produto.lower(),
                        safra=safra_str,
                        uf=uf.upper() if len(uf) == 2 else None,
                        area_plantada=area,
                        producao=producao,
                        produtividade=produtividade,
                        unidade_area="mil_ha",
                        unidade_producao="mil_ton",
                        levantamento=levantamento or 1,
                        data_publicacao=data_publicacao,
                        meta={"rotulo": uf.upper()},
                        parser_version=self.version,
                    )
                    safras.append(safra)
                except pydantic.ValidationError as e:
                    raise ParseError(
                        source="conab",
                        parser_version=self.version,
                        reason=f"Linha inválida em {sheet_name}!A{idx + 1}, {uf}, {safra_str}: {e}",
                    ) from e

        logger.info(
            "conab_parse_safra_success",
            produto=produto,
            records=len(safras),
        )

        return safras

    def _aba_da_safra(self, xlsx: BytesIO, esperada: str, safra_ref: str | None) -> str:
        """De out/2019 a jan/2022, a aba de inverno leva o ano no nome ("Trigo 2021"), e a edição
        pode trazer a do ano anterior, às vezes com o cabeçalho quebrado: vale a mais recente."""
        rotulos: tuple[str, ...] = (esperada,)
        if safra_ref:
            ano = dates.safra_para_anos(safra_ref)[1]
            rotulos += tuple(f"{esperada} {ano + delta}" for delta in (1, 0, -1))
        abas = structure.find_sheets(xlsx, rotulos, parser_version=self.version)
        if not abas:
            raise ParseError(
                source="conab",
                parser_version=self.version,
                reason=f"Aba obrigatória ausente: {esperada}",
            )
        return abas[0]

    _SUPRIMENTO_SEPARATE_SHEETS: dict[str, str] = {
        "soja": "Suprimento - Soja",
    }

    _SUPRIMENTO_ITEM_MAP = constants.CONAB_SOJA_COMPONENTES

    def parse_suprimento(
        self,
        xlsx: BytesIO,
        produto: str | None = None,
    ) -> list[dict[str, Any]]:
        if produto and produto.lower() in self._SUPRIMENTO_SEPARATE_SHEETS:
            sheet_name = self._SUPRIMENTO_SEPARATE_SHEETS[produto.lower()]
            selected = structure.find_sheet(xlsx, sheet_name, parser_version=self.version)
            if selected is not None:
                return self._parse_suprimento_wide(xlsx, selected, produto)
        sheet_name = structure.resolve_sheet(xlsx, "Suprimento", parser_version=self.version)
        suprimentos = self._parse_suprimento_long(xlsx, produto, sheet_name=sheet_name)
        if produto:
            return suprimentos
        for separado, nome in self._SUPRIMENTO_SEPARATE_SHEETS.items():
            selected = structure.find_sheet(xlsx, nome, parser_version=self.version)
            if selected is not None:
                suprimentos += self._parse_suprimento_wide(xlsx, selected, separado)
        return suprimentos

    def _parse_suprimento_long(
        self,
        xlsx: BytesIO,
        produto: str | None = None,
        *,
        sheet_name: str = "Suprimento",
    ) -> list[dict[str, Any]]:
        if hasattr(xlsx, "seek"):
            xlsx.seek(0)
        df = read_excel_safe(
            xlsx,
            source="conab",
            parser_version=self.version,
            label="aba Suprimento",
            sheet_name=sheet_name,
            header=None,
        )

        header_row = None
        for idx, row in df.iterrows():
            if "PRODUTO" in str(row.iloc[0]).upper():
                header_row = idx
                break

        if header_row is None:
            raise ParseError(
                source="conab",
                parser_version=self.version,
                reason="Não encontrou header na aba Suprimento",
            )

        columns = structure.supply_columns(df.iloc[cast(int, header_row)], self.version)

        suprimentos: dict[tuple[str, str], dict[str, Any]] = {}
        current_produto = None
        current_safra = None

        for idx in range(cast(int, header_row) + 1, len(df)):
            row = df.iloc[idx]

            produto_cell = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else None
            if produto_cell and produto_cell not in ["NaN", "nan", ""]:
                current_produto = produto_cell.replace("\n", " ").strip()
                current_safra = None

            if current_produto is None:
                continue

            if (
                produto
                and regions.remover_acentos(produto).casefold()
                not in regions.remover_acentos(current_produto).casefold()
            ):
                continue

            safra_cell = _cell_str(row, 1)
            if safra_cell:
                current_safra = _parse_safra_cell(safra_cell.rstrip(" *"))
            levantamento = _cell_str(row, 2) or None
            if not current_safra or (not safra_cell and not levantamento):
                continue

            suprimento = {
                "produto": current_produto,
                "safra": current_safra,
                "levantamento": levantamento,
                **{
                    field: self._parse_decimal(row.iloc[column])
                    for field, column in columns.items()
                },
                "demanda_total": (
                    self._parse_decimal(row.iloc[columns["demanda_total"]])
                    if "demanda_total" in columns
                    else None
                ),
                "unidade": "mil_ton",
            }

            key = (current_produto, current_safra)
            if key in suprimentos and (
                not levantamento or levantamento == suprimentos[key]["levantamento"]
            ):
                raise ParseError(
                    source="conab",
                    parser_version=self.version,
                    reason=f"Revisão ambígua em Suprimento!A{idx + 1}: {key}",
                )
            suprimentos[key] = suprimento

        logger.info(
            "conab_parse_suprimento_success",
            produto=produto,
            records=len(suprimentos),
        )

        return list(suprimentos.values())

    def _parse_suprimento_wide(
        self,
        xlsx: BytesIO,
        sheet_name: str,
        produto: str,
    ) -> list[dict[str, Any]]:
        if hasattr(xlsx, "seek"):
            xlsx.seek(0)
        sheet_name = structure.resolve_sheet(xlsx, sheet_name, parser_version=self.version)
        df = read_excel_safe(
            xlsx,
            source="conab",
            parser_version=self.version,
            label=f"aba {sheet_name}",
            sheet_name=sheet_name,
            header=None,
        )

        safra_row = None
        for idx, row in df.iterrows():
            cell = _cell_str(row, 0).upper()
            if "PRODUTO" in cell or "SAFRA" in cell:
                safra_row = cast(int, idx) + 1
                break

        if safra_row is None:
            raise ParseError(
                source="conab",
                parser_version=self.version,
                reason=f"Não encontrou header na aba {sheet_name}",
            )

        safras: dict[str, int] = {}
        row_safras = df.iloc[safra_row]
        for col_idx in range(1, len(row_safras)):
            cell = _cell_str(row_safras, col_idx)
            if re.fullmatch(r"\d{4}/\d{2}", cell):
                if cell in safras:
                    raise ParseError(
                        source="conab",
                        parser_version=self.version,
                        reason=f"Safra duplicada em {sheet_name}: {cell}",
                    )
                safras[cell] = col_idx

        if not safras:
            raise ParseError(
                source="conab",
                parser_version=self.version,
                reason=f"Não encontrou safras na aba {sheet_name}",
            )

        items: dict[str, dict[str, Decimal | None]] = {s: {} for s in safras}

        in_section_1 = False
        for idx in range(safra_row + 1, len(df)):
            row = df.iloc[idx]
            label = _cell_str(row, 0)
            if not label:
                continue

            if _SECTION_RE.match(label):
                if label.startswith("1."):
                    in_section_1 = True
                    continue
                else:
                    break

            if not in_section_1:
                continue

            component = structure.normalize_label(re.sub(r"^\d+\.\d+\.?\s*", "", label))
            field_name = self._SUPRIMENTO_ITEM_MAP.get(component)
            if field_name is None:
                raise ParseError(
                    source="conab",
                    parser_version=self.version,
                    reason=f"Item desconhecido em {sheet_name}!A{idx + 1}: {label}",
                )

            for safra_str, col_idx in safras.items():
                if field_name in items[safra_str]:
                    raise ParseError(
                        source="conab",
                        parser_version=self.version,
                        reason=f"Item duplicado em {sheet_name}: {field_name}",
                    )
                items[safra_str][field_name] = self._parse_decimal(row.iloc[col_idx])

        required = set(self._SUPRIMENTO_ITEM_MAP.values())
        for values in items.values():
            if required - values.keys():
                raise ParseError(
                    source="conab",
                    parser_version=self.version,
                    reason=f"Itens ausentes em {sheet_name}: {sorted(required - values.keys())}",
                )
        return [_build_suprimento(produto, safra_str, items[safra_str]) for safra_str in safras]

    def parse_brasil_total(
        self,
        xlsx: BytesIO,
        safra_ref: str | None = None,
    ) -> list[dict[str, Any]]:
        df = read_excel_safe(
            xlsx,
            source="conab",
            parser_version=self.version,
            label="aba Brasil - Total por Produto",
            sheet_name="Brasil - Total por Produto",
            header=None,
        )

        totais: list[dict[str, Any]] = []

        header_row = self._find_header_row(df)
        if header_row is None:
            raise ParseError(
                source="conab",
                parser_version=self.version,
                reason="Não encontrou header na aba Brasil - Total por Produto",
            )

        safra_cols = self._extract_safra_columns(df, header_row)
        data_row = header_row + 3
        secao: str | None = None
        titulo: str | None = None

        for idx in range(data_row, len(df)):
            row = df.iloc[idx]
            produto = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else None

            if not produto or produto in ["NaN", "nan", "", "TOTAL"]:
                continue

            if re.match(r"^(?:Legenda|Fonte|Nota)\s*:", produto, re.IGNORECASE):
                break
            if any("PRODUTIVIDADE" in _cell_str(row, col).upper() for col in range(1, len(row))):
                secao = produto
                continue

            maiusculo = produto == produto.upper()
            grupo = secao if maiusculo else titulo
            if maiusculo:
                titulo = produto
            if produto == "SUBTOTAL":
                secao = None

            for safra_str, cols in safra_cols.items():
                if safra_ref and safra_str != safra_ref:
                    continue

                total = {
                    "produto": produto,
                    "grupo": grupo,
                    "safra": safra_str,
                    "area_plantada": self._parse_decimal(row.iloc[cols["area"]]),
                    "produtividade": self._parse_decimal(row.iloc[cols["produtividade"]]),
                    "producao": self._parse_decimal(row.iloc[cols["producao"]]),
                    "unidade_area": "mil_ha",
                    "unidade_producao": "mil_ton",
                }
                totais.append(total)

        logger.info(
            "conab_parse_brasil_total_success",
            records=len(totais),
        )

        return totais

    def _find_header_row(self, df: pd.DataFrame) -> int | None:
        for idx, row in df.iterrows():
            cell0 = _cell_str(row, 0).upper()
            if "REGI" in cell0 or "UF" in cell0 or "PRODUTO" in cell0:
                return cast(int, idx)
        return None

    def _extract_safra_columns(
        self,
        df: pd.DataFrame,
        header_row: int,
    ) -> dict[str, dict[str, int]]:
        if header_row + 1 >= len(df):
            raise ParseError(
                source="conab",
                parser_version=self.version,
                reason="Não foi possível detectar colunas de safra no header da planilha",
            )
        cols: dict[str, dict[str, int]] = {}
        current_field = None
        fields = set()
        for col_idx in range(1, len(df.columns)):
            header = structure.normalize_label(_cell_str(df.iloc[header_row], col_idx))
            field = next(
                (
                    name
                    for prefix, (name, _) in constants.CONAB_SAFRA_METRICS.items()
                    if header.startswith(prefix)
                ),
                None,
            )
            if field is not None:
                if field in fields:
                    raise ParseError(
                        source="conab",
                        parser_version=self.version,
                        reason=f"Métrica de safra ambígua: {field}",
                    )
                unit = constants.CONAB_SAFRA_METRICS[field][1]
                if "(" in header and unit not in header:
                    raise ParseError(
                        source="conab",
                        parser_version=self.version,
                        reason=f"Unidade inesperada para {field}: {header}",
                    )
                fields.add(field)
                current_field = field
            cell = _cell_str(df.iloc[header_row + 1], col_idx)
            period = _parse_safra_cell(cell)
            annual = re.fullmatch(r"(?:Safra\s+)?(\d{4})(?:\.0)?", cell)
            if annual:
                period = anos_para_safra(int(annual.group(1)) - 1)
            if period is None:
                continue
            if current_field is None:
                raise ParseError(
                    source="conab",
                    parser_version=self.version,
                    reason=f"Safra sem métrica: coluna {col_idx}, {cell}",
                )
            target = cols.setdefault(period, {})
            if current_field in target:
                raise ParseError(
                    source="conab",
                    parser_version=self.version,
                    reason=f"Safra ambígua: {period}, {current_field}",
                )
            target[current_field] = col_idx
        required = {"area", "produtividade", "producao"}
        if not cols or any(set(mapping) != required for mapping in cols.values()):
            raise ParseError(
                source="conab",
                parser_version=self.version,
                reason="Não foi possível detectar colunas de safra completas no header da planilha",
            )
        return cols

    def _parse_decimal(self, value: Any) -> Decimal | None:
        if pd.isna(value):
            return None

        if isinstance(value, int | float):
            return Decimal(str(value)) if math.isfinite(value) else None

        parsed = safe_float(str(value))
        if parsed is None or not math.isfinite(parsed):
            return None
        return Decimal(str(parsed))
