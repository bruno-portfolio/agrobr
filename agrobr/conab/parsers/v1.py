"""Parser v1 para planilhas XLSX da CONAB."""

from __future__ import annotations

from datetime import date
from decimal import Decimal, InvalidOperation
from io import BytesIO
from typing import Any

import pandas as pd
import structlog

from agrobr import constants
from agrobr.exceptions import ParseError
from agrobr.models import Safra

logger = structlog.get_logger()


class ConabParserV1:
    """Parser para planilhas XLSX de safras da CONAB."""

    version: int = 1
    source: str = "conab"
    valid_from: date = date(2020, 1, 1)
    valid_until: date | None = None

    def parse_safra_produto(
        self,
        xlsx: BytesIO,
        produto: str,
        safra_ref: str | None = None,
        levantamento: int | None = None,
    ) -> list[Safra]:
        """
        Extrai dados de safra por produto.

        Args:
            xlsx: BytesIO com arquivo XLSX
            produto: Nome do produto (soja, milho, etc)
            safra_ref: Safra de referência (ex: "2025/26")
            levantamento: Número do levantamento

        Returns:
            Lista de objetos Safra por UF

        Raises:
            ParseError: Se não conseguir parsear os dados
        """
        sheet_name = constants.CONAB_PRODUTOS.get(produto.lower())
        if not sheet_name:
            raise ParseError(
                source="conab",
                parser_version=self.version,
                reason=f"Produto não suportado: {produto}",
            )

        try:
            df = pd.read_excel(xlsx, sheet_name=sheet_name, header=None)
        except Exception as e:
            raise ParseError(
                source="conab",
                parser_version=self.version,
                reason=f"Erro ao ler aba {sheet_name}: {e}",
            ) from e

        header_row = self._find_header_row(df)
        if header_row is None:
            raise ParseError(
                source="conab",
                parser_version=self.version,
                reason=f"Não encontrou header na aba {sheet_name}",
            )

        safras = []
        data_row = header_row + 3

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
                        data_publicacao=date.today(),
                        parser_version=self.version,
                    )
                    safras.append(safra)
                except Exception as e:
                    logger.warning(
                        "conab_parse_row_error",
                        uf=uf,
                        safra=safra_str,
                        error=str(e),
                    )

        logger.info(
            "conab_parse_safra_success",
            produto=produto,
            records=len(safras),
        )

        return safras

    def parse_suprimento(
        self,
        xlsx: BytesIO,
        produto: str | None = None,
    ) -> list[dict[str, Any]]:
        """
        Extrai dados de balanço de oferta/demanda.

        Args:
            xlsx: BytesIO com arquivo XLSX
            produto: Filtrar por produto (opcional)

        Returns:
            Lista de dicts com dados de suprimento
        """
        try:
            df = pd.read_excel(xlsx, sheet_name="Suprimento", header=None)
        except Exception as e:
            raise ParseError(
                source="conab",
                parser_version=self.version,
                reason=f"Erro ao ler aba Suprimento: {e}",
            ) from e

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

        suprimentos = []
        current_produto = None

        for idx in range(int(header_row) + 1, len(df)):
            row = df.iloc[idx]

            produto_cell = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else None
            if produto_cell and produto_cell not in ["NaN", "nan", ""]:
                current_produto = produto_cell.replace("\n", " ").strip()

            if current_produto is None:
                continue

            if produto and produto.lower() not in current_produto.lower():
                continue

            safra = str(row.iloc[1]).strip() if pd.notna(row.iloc[1]) else None
            if not safra or "/" not in safra:
                continue

            suprimento = {
                "produto": current_produto,
                "safra": safra,
                "levantamento": str(row.iloc[2]).strip() if pd.notna(row.iloc[2]) else None,
                "estoque_inicial": self._parse_decimal(row.iloc[3]),
                "producao": self._parse_decimal(row.iloc[4]),
                "importacao": self._parse_decimal(row.iloc[5]),
                "suprimento_total": self._parse_decimal(row.iloc[6]),
                "consumo": self._parse_decimal(row.iloc[7]),
                "exportacao": self._parse_decimal(row.iloc[8]),
                "demanda_total": self._parse_decimal(row.iloc[9]),
                "estoque_final": self._parse_decimal(row.iloc[10]),
                "unidade": "mil_ton",
            }

            suprimentos.append(suprimento)

        logger.info(
            "conab_parse_suprimento_success",
            produto=produto,
            records=len(suprimentos),
        )

        return suprimentos

    def parse_brasil_total(
        self,
        xlsx: BytesIO,
        safra_ref: str | None = None,
    ) -> list[dict[str, Any]]:
        """
        Extrai dados totais do Brasil por produto.

        Args:
            xlsx: BytesIO com arquivo XLSX
            safra_ref: Safra de referência (opcional)

        Returns:
            Lista de dicts com totais por produto
        """
        try:
            df = pd.read_excel(xlsx, sheet_name="Brasil - Total por Produto", header=None)
        except Exception as e:
            raise ParseError(
                source="conab",
                parser_version=self.version,
                reason=f"Erro ao ler aba Brasil - Total por Produto: {e}",
            ) from e

        totais: list[dict[str, Any]] = []

        header_row = self._find_header_row(df)
        if header_row is None:
            return totais

        safra_cols = self._extract_safra_columns(df, header_row)
        data_row = header_row + 3

        for idx in range(data_row, len(df)):
            row = df.iloc[idx]
            produto = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else None

            if not produto or produto in ["NaN", "nan", "", "TOTAL"]:
                continue

            for safra_str, cols in safra_cols.items():
                if safra_ref and safra_str != safra_ref:
                    continue

                total = {
                    "produto": produto,
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
        """Encontra a linha do header."""
        for idx, row in df.iterrows():
            cell0 = str(row.iloc[0]).upper() if pd.notna(row.iloc[0]) else ""
            if "REGI" in cell0 or "UF" in cell0 or "PRODUTO" in cell0:
                return int(idx)
        return None

    def _extract_safra_columns(
        self,
        df: pd.DataFrame,
        header_row: int,
    ) -> dict[str, dict[str, int]]:
        """
        Extrai mapeamento de safras para colunas.

        Returns:
            Dict com safra -> {area: col, produtividade: col, producao: col}
        """
        safra_row = df.iloc[header_row + 1]
        header_cols = df.iloc[header_row]
        cols = {}

        area_start = None
        prod_start = None
        producao_start = None

        for col_idx in range(1, len(header_cols)):
            cell = (
                str(header_cols.iloc[col_idx]).upper()
                if pd.notna(header_cols.iloc[col_idx])
                else ""
            )
            if "ÁREA" in cell or "AREA" in cell:
                area_start = col_idx
            elif "PRODUTIVIDADE" in cell:
                prod_start = col_idx
            elif "PRODUÇÃO" in cell or "PRODUCAO" in cell:
                producao_start = col_idx

        safras_encontradas = []
        for col_idx in range(1, len(safra_row)):
            cell = str(safra_row.iloc[col_idx]).strip() if pd.notna(safra_row.iloc[col_idx]) else ""

            if "Safra" in cell or ("/" in cell and "VAR" not in cell.upper()):
                safra_match = cell.replace("Safra ", "").strip()
                if "/" in safra_match:
                    parts = safra_match.split("/")
                    if len(parts) == 2:
                        ano1 = parts[0].strip()
                        ano2 = parts[1].strip()

                        if len(ano1) == 2:
                            ano1 = "20" + ano1
                        if len(ano2) == 2:
                            pass

                        safra_full = f"{ano1}/{ano2}"
                        if safra_full not in safras_encontradas:
                            safras_encontradas.append(safra_full)

        if area_start and prod_start and producao_start and safras_encontradas:
            for i, safra in enumerate(safras_encontradas):
                cols[safra] = {
                    "area": area_start + i,
                    "produtividade": prod_start + i,
                    "producao": producao_start + i,
                }
        elif safras_encontradas:
            for i, safra in enumerate(safras_encontradas):
                base_col = 1 + (i * 3)
                cols[safra] = {
                    "area": base_col,
                    "produtividade": base_col + 3 * len(safras_encontradas),
                    "producao": base_col + 6 * len(safras_encontradas),
                }

        if not cols:
            cols["2025/26"] = {
                "area": 2,
                "produtividade": 5,
                "producao": 8,
            }

        return cols

    def _parse_decimal(self, value: Any) -> Decimal | None:
        """Converte valor para Decimal."""
        if pd.isna(value):
            return None

        try:
            if isinstance(value, int | float):
                return Decimal(str(value))

            value_str = str(value).strip().replace(",", ".")
            value_str = value_str.replace(" ", "")

            if not value_str or value_str in ["0", "-", "NaN", "nan"]:
                return None

            return Decimal(value_str)
        except (InvalidOperation, ValueError):
            return None
