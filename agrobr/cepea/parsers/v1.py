"""Parser v1 para indicadores CEPEA - Layout 2024."""

from __future__ import annotations

import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

import structlog
from bs4 import BeautifulSoup

from agrobr.constants import Fonte
from agrobr.exceptions import ParseError
from agrobr.models import Indicador

from .base import BaseParser
from .fingerprint import extract_fingerprint

logger = structlog.get_logger()


class CepeaParserV1(BaseParser):
    """Parser para layout CEPEA 2024."""

    version = 1
    source = "cepea"
    valid_from = date(2024, 1, 1)
    valid_until = None

    def can_parse(self, html: str) -> tuple[bool, float]:
        """Verifica se este parser consegue processar o HTML."""
        soup = BeautifulSoup(html, "lxml")

        confidence = 0.0
        checks_passed = 0
        total_checks = 5

        tables = soup.find_all("table")
        if tables:
            checks_passed += 1

        indicador_table = soup.find("table", id=re.compile(r"indicador|preco|cotacao", re.I))
        if not indicador_table:
            indicador_table = soup.find("table", class_=re.compile(r"indicador|preco|cotacao", re.I))
        if indicador_table:
            checks_passed += 1

        headers = soup.find_all("th")
        header_texts = [th.get_text(strip=True).lower() for th in headers]
        date_keywords = ["data", "dia", "date"]
        value_keywords = ["valor", "preço", "preco", "price", "r$"]

        if any(kw in " ".join(header_texts) for kw in date_keywords):
            checks_passed += 1
        if any(kw in " ".join(header_texts) for kw in value_keywords):
            checks_passed += 1

        cepea_indicators = soup.find_all(
            string=re.compile(r"cepea|esalq|indicador", re.I)
        )
        if cepea_indicators:
            checks_passed += 1

        confidence = checks_passed / total_checks

        can_parse = confidence >= 0.4
        logger.debug(
            "can_parse_check",
            parser_version=self.version,
            confidence=confidence,
            checks_passed=checks_passed,
            total_checks=total_checks,
        )

        return can_parse, confidence

    def parse(self, html: str, produto: str) -> list[Indicador]:
        """Parseia HTML e retorna lista de indicadores."""
        soup = BeautifulSoup(html, "lxml")
        indicadores: list[Indicador] = []

        tables = soup.find_all("table")
        if not tables:
            raise ParseError(
                source=self.source,
                parser_version=self.version,
                reason="No tables found in HTML",
                html_snippet=html[:500],
            )

        data_table = self._find_data_table(soup)
        if not data_table:
            raise ParseError(
                source=self.source,
                parser_version=self.version,
                reason="Could not identify data table",
                html_snippet=html[:500],
            )

        headers = self._extract_headers(data_table)
        rows = data_table.find_all("tr")[1:]

        for row in rows:
            cells = row.find_all(["td", "th"])
            if len(cells) < 2:
                continue

            try:
                indicador = self._parse_row(cells, headers, produto)
                if indicador:
                    indicadores.append(indicador)
            except (ValueError, InvalidOperation) as e:
                logger.debug(
                    "row_parse_failed",
                    error=str(e),
                    cells=[c.get_text(strip=True) for c in cells],
                )
                continue

        if not indicadores:
            raise ParseError(
                source=self.source,
                parser_version=self.version,
                reason="No valid indicators extracted",
                html_snippet=html[:500],
            )

        logger.info(
            "parse_success",
            source=self.source,
            parser_version=self.version,
            records_count=len(indicadores),
        )

        return indicadores

    def extract_fingerprint(self, html: str) -> dict[str, Any]:
        """Extrai assinatura estrutural do HTML."""
        fp = extract_fingerprint(html, Fonte.CEPEA, "internal")
        return fp.model_dump()

    def _find_data_table(self, soup: BeautifulSoup) -> Any | None:
        """Encontra a tabela com dados de indicadores."""
        table = soup.find("table", id=re.compile(r"indicador|preco|cotacao|dados", re.I))
        if table:
            return table

        table = soup.find(
            "table", class_=re.compile(r"indicador|preco|cotacao|dados|table", re.I)
        )
        if table:
            return table

        tables = soup.find_all("table")
        for table in tables:
            headers = table.find_all("th")
            header_text = " ".join(th.get_text(strip=True).lower() for th in headers)
            if "data" in header_text and ("valor" in header_text or "r$" in header_text):
                return table

        if tables:
            largest_table = max(tables, key=lambda t: len(t.find_all("tr")))
            if len(largest_table.find_all("tr")) >= 3:
                return largest_table

        return None

    def _extract_headers(self, table: Any) -> list[str]:
        """Extrai headers da tabela."""
        headers: list[str] = []
        header_row = table.find("tr")

        if header_row:
            for cell in header_row.find_all(["th", "td"]):
                text = cell.get_text(strip=True).lower()
                text = re.sub(r"\s+", " ", text)
                headers.append(text)

        return headers

    def _parse_row(
        self, cells: list[Any], headers: list[str], produto: str
    ) -> Indicador | None:
        """Parseia uma linha da tabela."""
        cell_texts = [c.get_text(strip=True) for c in cells]

        data_value = None
        valor_value = None
        variacao_value = None

        for i, (header, cell_text) in enumerate(zip(headers, cell_texts)):
            header_lower = header.lower()

            if any(kw in header_lower for kw in ["data", "dia", "date"]):
                data_value = self._parse_date(cell_text)
            elif any(kw in header_lower for kw in ["valor", "preço", "preco", "r$", "price"]):
                valor_value = self._parse_decimal(cell_text)
            elif "var" in header_lower or "%" in header_lower:
                variacao_value = cell_text

        if not data_value and cell_texts:
            data_value = self._parse_date(cell_texts[0])

        if not valor_value and len(cell_texts) > 1:
            for text in cell_texts[1:]:
                parsed = self._parse_decimal(text)
                if parsed and parsed > 0:
                    valor_value = parsed
                    break

        if not data_value or not valor_value:
            return None

        unidade = self._detect_unidade(produto, headers)

        return Indicador(
            fonte=Fonte.CEPEA,
            produto=produto,
            praca=None,
            data=data_value,
            valor=valor_value,
            unidade=unidade,
            metodologia="indicador_esalq",
            revisao=0,
            meta={"variacao": variacao_value} if variacao_value else {},
            parser_version=self.version,
        )

    def _parse_date(self, text: str) -> date | None:
        """Parseia data de diferentes formatos."""
        text = text.strip()

        patterns = [
            (r"(\d{2})/(\d{2})/(\d{4})", "%d/%m/%Y"),
            (r"(\d{2})-(\d{2})-(\d{4})", "%d-%m-%Y"),
            (r"(\d{4})-(\d{2})-(\d{2})", "%Y-%m-%d"),
            (r"(\d{2})/(\d{2})/(\d{2})", "%d/%m/%y"),
        ]

        for pattern, date_format in patterns:
            match = re.search(pattern, text)
            if match:
                try:
                    return datetime.strptime(match.group(), date_format).date()
                except ValueError:
                    continue

        return None

    def _parse_decimal(self, text: str) -> Decimal | None:
        """Parseia valor decimal."""
        text = text.strip()

        text = re.sub(r"[R$\s]", "", text)

        if "," in text and "." in text:
            text = text.replace(".", "").replace(",", ".")
        elif "," in text:
            text = text.replace(",", ".")

        text = re.sub(r"[^\d.\-]", "", text)

        if not text or text == "." or text == "-":
            return None

        try:
            value = Decimal(text)
            return value if value > 0 else None
        except InvalidOperation:
            return None

    def _detect_unidade(self, produto: str, headers: list[str]) -> str:
        """Detecta unidade baseado no produto e headers."""
        produto_lower = produto.lower()

        unidades_produto = {
            "soja": "BRL/sc60kg",
            "milho": "BRL/sc60kg",
            "cafe": "BRL/sc60kg",
            "trigo": "BRL/sc60kg",
            "arroz": "BRL/sc50kg",
            "boi": "BRL/@",
            "boi_gordo": "BRL/@",
            "boi-gordo": "BRL/@",
            "algodao": "BRL/@",
            "frango": "BRL/kg",
            "suino": "BRL/kg",
            "acucar": "BRL/sc50kg",
            "etanol": "BRL/L",
        }

        for key, unidade in unidades_produto.items():
            if key in produto_lower:
                return unidade

        header_text = " ".join(headers).lower()
        if "sc" in header_text or "saca" in header_text:
            if "50" in header_text:
                return "BRL/sc50kg"
            return "BRL/sc60kg"
        if "@" in header_text or "arroba" in header_text:
            return "BRL/@"
        if "kg" in header_text:
            return "BRL/kg"
        if "litro" in header_text or "/l" in header_text:
            return "BRL/L"

        return "BRL/sc60kg"
