from __future__ import annotations

import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

import structlog
from bs4 import BeautifulSoup

from agrobr.constants import CEPEA_PARSER_VERSION, CEPEA_TABELAS_POR_PRACA, CEPEA_TITULOS, Fonte
from agrobr.exceptions import ParseError
from agrobr.models import Indicador
from agrobr.normalize import dates

from .base import BaseParser
from .fingerprint import extract_fingerprint

logger = structlog.get_logger()

PRACAS: dict[str, str] = {
    "soja": "Paranaguá/PR",
    "soja_parana": "Paraná",
    "milho": "Campinas/SP",
    "cafe": "São Paulo/SP",
    "cafe_arabica": "São Paulo/SP",
    "cafe_robusta": "Espírito Santo",
    "bezerro": "Mato Grosso do Sul",
    "boi": "São Paulo/SP",
    "boi_gordo": "São Paulo/SP",
    "boi-gordo": "São Paulo/SP",
    "trigo": "Paraná",
    "algodao": "São Paulo/SP",
    "arroz": "Rio Grande do Sul",
    "acucar": "São Paulo/SP",
    "acucar_refinado": "São Paulo/SP",
    "frango_congelado": "São Paulo/SP",
    "frango_resfriado": "São Paulo/SP",
    "etanol_hidratado": "São Paulo/SP",
    "etanol_anidro": "São Paulo/SP",
    "laranja_industria": "São Paulo/SP",
    "laranja_in_natura": "São Paulo/SP",
}


def _chave_da_variacao(cabecalho: str) -> str:
    if "mês" in cabecalho or "mes" in cabecalho:
        return "variacao_mes"
    if "semana" in cabecalho:
        return "variacao_semana"
    return "variacao"


def _papel_da_coluna(cabecalho: str) -> str | None:
    """O que a coluna traz, pelo cabeçalho; ``valor`` é só o preço em reais.

    "Preço médio" (leite) e "A prazo" (laranja) não dizem a moeda, que o título ou a nota da tabela
    dão em reais.
    """
    if cabecalho == "estado":
        return "praca"
    if "var" in cabecalho or "%" in cabecalho:
        return "variacao"
    if any(chave in cabecalho for chave in ("data", "dia", "date")):
        return "data"
    if "us$" in cabecalho or "usd" in cabecalho:
        return "usd"
    if any(chave in cabecalho for chave in ("valor", "preço", "preco", "r$", "price", "a prazo")):
        return "valor"
    return None


def _is_robusta(text: str) -> bool:
    t = text.lower()
    return "robust" in t or "conil" in t


class CepeaParserV1(BaseParser):
    version = CEPEA_PARSER_VERSION
    source = "cepea"
    valid_from = date(2024, 1, 1)
    valid_until = None

    def can_parse(self, html: str) -> tuple[bool, float]:
        soup = BeautifulSoup(html, "lxml")

        confidence = 0.0
        checks_passed = 0
        total_checks = 5

        tables = soup.find_all("table")
        if tables:
            checks_passed += 1

        indicador_table = soup.find("table", id=re.compile(r"indicador|preco|cotacao", re.I))
        if not indicador_table:
            indicador_table = soup.find(
                "table", class_=re.compile(r"indicador|preco|cotacao", re.I)
            )
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

        cepea_indicators = soup.find_all(string=re.compile(r"cepea|esalq|indicador", re.I))
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

        tabelas = self._data_tables(soup, produto)
        if not tabelas:
            raise ParseError(
                source=self.source,
                parser_version=self.version,
                reason="Could not identify data table",
                html_snippet=html[:500],
            )

        for data_table, praca_da_tabela in tabelas:
            headers = self._extract_headers(data_table)
            if "valor" not in map(_papel_da_coluna, headers):
                raise ParseError(
                    source=self.source,
                    parser_version=self.version,
                    reason="layout do CEPEA mudou: coluna de valor em R$ não encontrada",
                    html_snippet=html[:500],
                )
            for row in data_table.find_all("tr")[1:]:
                cells = row.find_all(["td", "th"])
                if len(cells) < 2:
                    continue

                try:
                    indicador = self._parse_row(cells, headers, produto, praca_da_tabela)
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

        pesos = self._extract_peso_medio(soup)
        for indicador in indicadores:
            peso = pesos.get(indicador.data)
            if peso is not None:
                indicador.meta["peso_medio_kg"] = peso

        logger.info(
            "parse_success",
            source=self.source,
            parser_version=self.version,
            records_count=len(indicadores),
        )

        return indicadores

    def extract_fingerprint(self, html: str) -> dict[str, Any]:
        fp = extract_fingerprint(html, Fonte.CEPEA, "internal")
        return fp.model_dump()

    def _data_tables(self, soup: BeautifulSoup, produto: str) -> list[tuple[Any, str | None]]:
        """Uma tabela por praça quando a página publica cada praça em tabela própria (trigo:
        PR e RS); nos demais produtos, a tabela do indicador, com a praça da linha."""
        por_praca = CEPEA_TABELAS_POR_PRACA.get(produto)
        if not por_praca:
            tabela = self._find_data_table(soup, produto)
            return [(tabela, None)] if tabela else []
        titulos = [
            (" ".join(titulo.get_text(" ", strip=True).split()), titulo)
            for titulo in soup.find_all("div", class_="imagenet-table-titulo")
        ]
        return [
            (titulo.find_next("table"), praca)
            for praca, padrao in por_praca.items()
            for texto, titulo in titulos
            if re.search(padrao, texto, re.I) and titulo.find_next("table") is not None
        ]

    def _find_data_table(self, soup: BeautifulSoup, produto: str | None = None) -> Any | None:
        titles = soup.find_all("div", class_="imagenet-table-titulo")
        pattern = CEPEA_TITULOS.get(produto or "")
        if pattern and titles:
            for title in titles:
                text = " ".join(title.get_text(" ", strip=True).split())
                if re.search(pattern, text, re.I):
                    return title.find_next("table")
            return None

        tables = soup.find_all("table")
        if len(tables) != 1 or (produto and _is_robusta(produto)):
            return None

        table = soup.find("table", id=re.compile(r"indicador|preco|cotacao|dados", re.I))
        if table:
            return table

        table = soup.find("table", class_=re.compile(r"indicador|preco|cotacao|dados|table", re.I))
        if table:
            return table

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
        headers: list[str] = []
        header_row = table.find("tr")

        if header_row:
            for cell in header_row.find_all(["th", "td"]):
                text = cell.get_text(strip=True).lower()
                text = re.sub(r"\s+", " ", text)
                headers.append(text)

        return headers

    def _parse_row(
        self, cells: list[Any], headers: list[str], produto: str, praca_da_tabela: str | None = None
    ) -> Indicador | None:
        cell_texts = [c.get_text(strip=True) for c in cells]

        data_value = None
        valor_value = None
        variacoes: dict[str, str] = {}
        valor_usd = None
        praca = praca_da_tabela or PRACAS.get(produto.lower())

        for header, cell_text in zip(headers, cell_texts):
            header_lower = header.lower()
            papel = _papel_da_coluna(header_lower)

            if papel == "praca":
                praca = cell_text
            elif papel == "variacao":
                variacoes[_chave_da_variacao(header_lower)] = cell_text
            elif papel == "data":
                data_value = self._parse_date(cell_text)
            elif papel == "usd":
                valor_usd = self._parse_decimal(cell_text)
            elif papel == "valor":
                valor_value = self._parse_decimal(cell_text)

        if not data_value and cell_texts:
            data_value = (
                self._parse_month(cell_texts[0])
                if produto == "leite"
                else self._parse_date(cell_texts[0])
            )

        if not data_value or not valor_value:
            return None

        unidade = self._detect_unidade(produto, headers)
        meta: dict[str, Any] = {chave: texto for chave, texto in variacoes.items() if texto}
        if valor_usd:
            meta["valor_usd"] = float(valor_usd)

        return Indicador(
            fonte=Fonte.CEPEA,
            produto=produto,
            praca=praca,
            data=data_value,
            valor=valor_value,
            unidade=unidade,
            metodologia="indicador_esalq",
            revisao=0,
            meta=meta,
            parser_version=self.version,
        )

    def _extract_peso_medio(self, soup: BeautifulSoup) -> dict[date, float]:
        pesos: dict[date, float] = {}
        for title in soup.find_all("div", class_="imagenet-table-titulo"):
            if not re.search(r"peso\s+m[ée]dio", title.get_text(" ", strip=True), re.I):
                continue
            table = title.find_next("table")
            if table is None:
                continue
            for row in table.find_all("tr")[1:]:
                cells = [cell.get_text(strip=True) for cell in row.find_all(["td", "th"])]
                if len(cells) < 2:
                    continue
                dia = self._parse_date(cells[0])
                peso = self._parse_decimal(cells[1])
                if dia and peso:
                    pesos[dia] = float(peso)
        return pesos

    def _parse_month(self, text: str) -> date | None:
        match = re.fullmatch(r"([a-zç]+)/(\d{2}|\d{4})", text.strip(), re.I)
        if not match:
            return None
        month = dates.month_to_number(match.group(1))
        if month is None:
            return None
        year = int(match.group(2))
        return date(year + 2000 if year < 100 else year, month, 1)

    def _parse_date(self, text: str) -> date | None:
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
        produto_lower = produto.lower()

        unidades_produto = {
            "acucar_refinado": "BRL/kg",
            "leite": "BRL/L",
            "laranja": "BRL/cx40.8kg",
            "soja": "BRL/sc60kg",
            "milho": "BRL/sc60kg",
            "cafe": "BRL/sc60kg",
            "trigo": "BRL/ton",
            "arroz": "BRL/sc50kg",
            "bezerro": "BRL/cabeca",
            "boi": "BRL/@",
            "boi_gordo": "BRL/@",
            "boi-gordo": "BRL/@",
            "algodao": "cBRL/lb",
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
