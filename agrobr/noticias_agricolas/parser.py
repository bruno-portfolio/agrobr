"""Parser para indicadores CEPEA via Notícias Agrícolas."""

from __future__ import annotations

import re
from datetime import datetime
from decimal import Decimal, InvalidOperation

import structlog
from bs4 import BeautifulSoup

from agrobr.constants import Fonte
from agrobr.models import Indicador

logger = structlog.get_logger()

# Mapeamento de produtos para unidades
UNIDADES = {
    "soja": "BRL/sc60kg",
    "soja_parana": "BRL/sc60kg",
    "milho": "BRL/sc60kg",
    "boi": "BRL/@",
    "boi_gordo": "BRL/@",
    "cafe": "BRL/sc60kg",
    "cafe_arabica": "BRL/sc60kg",
    "algodao": "BRL/@",
    "trigo": "BRL/ton",
}

# Mapeamento de produtos para praça
PRACAS = {
    "soja": "Paranaguá/PR",
    "soja_parana": "Paraná",
    "milho": "Campinas/SP",
    "boi": "São Paulo/SP",
    "boi_gordo": "São Paulo/SP",
    "cafe": "São Paulo/SP",
    "cafe_arabica": "São Paulo/SP",
    "algodao": "São Paulo/SP",
    "trigo": "Paraná",
}


def _parse_date(date_str: str) -> datetime | None:
    """Converte string de data para datetime."""
    date_str = date_str.strip()

    # Formato: DD/MM/YYYY
    match = re.match(r"(\d{2})/(\d{2})/(\d{4})", date_str)
    if match:
        day, month, year = match.groups()
        try:
            return datetime(int(year), int(month), int(day))
        except ValueError:
            return None

    return None


def _parse_valor(valor_str: str) -> Decimal | None:
    """Converte string de valor para Decimal."""
    valor_str = valor_str.strip()

    # Remove "R$" e espaços
    valor_str = re.sub(r"R\$\s*", "", valor_str)

    # Substitui vírgula por ponto
    valor_str = valor_str.replace(".", "").replace(",", ".")

    try:
        return Decimal(valor_str)
    except InvalidOperation:
        return None


def _parse_variacao(var_str: str) -> Decimal | None:
    """Converte string de variação para Decimal."""
    var_str = var_str.strip()

    # Remove % e espaços
    var_str = re.sub(r"[%\s]", "", var_str)

    # Substitui vírgula por ponto
    var_str = var_str.replace(",", ".")

    try:
        return Decimal(var_str)
    except InvalidOperation:
        return None


def parse_indicador(html: str, produto: str) -> list[Indicador]:
    """
    Faz parse do HTML do Notícias Agrícolas e extrai indicadores CEPEA.

    Args:
        html: HTML da página
        produto: Nome do produto (soja, milho, boi, etc)

    Returns:
        Lista de objetos Indicador
    """
    soup = BeautifulSoup(html, "lxml")
    indicadores: list[Indicador] = []

    produto_lower = produto.lower()
    unidade = UNIDADES.get(produto_lower, "BRL/unidade")
    praca = PRACAS.get(produto_lower)

    # Estrutura do Notícias Agrícolas:
    # Tabela com classe "cot-fisicas" ou tabelas genéricas
    # Headers: Data | Valor R$ | Variação (%)

    # Primeiro tenta tabela específica de cotações
    tables = soup.find_all("table", class_="cot-fisicas")

    # Se não encontrar, tenta todas as tabelas
    if not tables:
        tables = soup.find_all("table")

    for table in tables:
        # Verifica se é tabela de cotação
        headers = table.find_all("th")
        header_text = " ".join(h.get_text(strip=True).lower() for h in headers)

        if "data" not in header_text or "valor" not in header_text:
            continue

        # Extrai todas as linhas de dados (tbody > tr)
        tbody = table.find("tbody")
        if tbody:
            rows = tbody.find_all("tr")
        else:
            # Fallback: pega todas as tr exceto header
            rows = table.find_all("tr")[1:]

        for row in rows:
            cells = row.find_all("td")

            if len(cells) < 2:
                continue

            # Extrai data e valor
            data_str = cells[0].get_text(strip=True)
            valor_str = cells[1].get_text(strip=True)

            data = _parse_date(data_str)
            valor = _parse_valor(valor_str)

            if data is None or valor is None:
                logger.warning(
                    "parse_row_failed",
                    source="noticias_agricolas",
                    data_str=data_str,
                    valor_str=valor_str,
                )
                continue

            # Extrai variação se disponível
            meta: dict = {}
            if len(cells) >= 3:
                var_str = cells[2].get_text(strip=True)
                variacao = _parse_variacao(var_str)
                if variacao is not None:
                    meta["variacao_percentual"] = float(variacao)

            meta["fonte_original"] = "CEPEA/ESALQ"
            meta["via"] = "Notícias Agrícolas"

            indicador = Indicador(
                fonte=Fonte.NOTICIAS_AGRICOLAS,
                produto=produto_lower,
                praca=praca,
                data=data.date(),
                valor=valor,
                unidade=unidade,
                metodologia="CEPEA/ESALQ via Notícias Agrícolas",
                meta=meta,
                parser_version=1,
            )

            indicadores.append(indicador)

    logger.info(
        "parse_complete",
        source="noticias_agricolas",
        produto=produto,
        count=len(indicadores),
    )

    return indicadores
