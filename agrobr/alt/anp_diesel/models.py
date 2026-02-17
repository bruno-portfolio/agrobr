"""Constantes e modelos Pydantic para ANP Diesel."""

from __future__ import annotations

from datetime import date
from typing import Any

from pydantic import BaseModel, Field, field_validator

SHLP_BASE = (
    "https://www.gov.br/anp/pt-br/assuntos/precos-e-defesa-da-concorrencia"
    "/precos/precos-revenda-e-de-distribuicao-combustiveis/shlp"
)

VENDAS_M3_URL = (
    "https://www.gov.br/anp/pt-br/centrais-de-conteudo"
    "/dados-estatisticos/de/vdpb/vendas-combustiveis-m3.xls"
)

PRECOS_MUNICIPIOS_URLS: dict[str, str] = {
    "2022-2023": f"{SHLP_BASE}/semanal/semanal-municipios-2022_a_2023.xlsx",
    "2024-2025": f"{SHLP_BASE}/semanal/semanal-municipio-2024-2025.xlsx",
    "2026": f"{SHLP_BASE}/semanal/semanal-municipios-2026.xlsx",
}

PRECOS_ESTADOS_URL = f"{SHLP_BASE}/semanal/semanal-estados-desde-2013.xlsx"

PRECOS_BRASIL_URL = f"{SHLP_BASE}/semanal/semanal-brasil-desde-2013.xlsx"

MENSAL_ESTADOS_URL = f"{SHLP_BASE}/mensal/mensal-estados-desde-jan2013.xlsx"

MENSAL_BRASIL_URL = f"{SHLP_BASE}/mensal/mensal-brasil-desde-jan2013.xlsx"

PRODUTOS_DIESEL = frozenset(
    {
        "DIESEL",
        "DIESEL S10",
        "OLEO DIESEL",
        "OLEO DIESEL S10",
    }
)

NIVEL_MUNICIPIO = "municipio"
NIVEL_UF = "uf"
NIVEL_BRASIL = "brasil"
NIVEIS_VALIDOS = frozenset({NIVEL_MUNICIPIO, NIVEL_UF, NIVEL_BRASIL})

AGREGACAO_SEMANAL = "semanal"
AGREGACAO_MENSAL = "mensal"
AGREGACOES_VALIDAS = frozenset({AGREGACAO_SEMANAL, AGREGACAO_MENSAL})

COLUNAS_XLSX_PRECOS: dict[str, str] = {
    "ESTADO - SIGLA": "uf",
    "MUNICÍPIO": "municipio",
    "PRODUTO": "produto",
    "DATA INICIAL": "data_inicial",
    "DATA FINAL": "data_final",
    "PREÇO MÉDIO REVENDA": "preco_venda",
    "PREÇO MÉDIO DISTRIBUIÇÃO": "preco_compra",
    "NÚMERO DE POSTOS PESQUISADOS": "n_postos",
}

COLUNAS_XLSX_PRECOS_ALT: dict[str, str] = {
    "Estado - Sigla": "uf",
    "Município": "municipio",
    "Produto": "produto",
    "Data Inicial": "data_inicial",
    "Data Final": "data_final",
    "Preço Médio Revenda": "preco_venda",
    "Preço Médio Distribuição": "preco_compra",
    "Número de Postos Pesquisados": "n_postos",
}

UFS_VALIDAS = frozenset(
    {
        "AC",
        "AL",
        "AM",
        "AP",
        "BA",
        "CE",
        "DF",
        "ES",
        "GO",
        "MA",
        "MG",
        "MS",
        "MT",
        "PA",
        "PB",
        "PE",
        "PI",
        "PR",
        "RJ",
        "RN",
        "RO",
        "RR",
        "RS",
        "SC",
        "SE",
        "SP",
        "TO",
    }
)


def _resolve_periodo_municipio(ano: int) -> str | None:
    """Resolve ano para chave do dict PRECOS_MUNICIPIOS_URLS."""
    for periodo, _url in PRECOS_MUNICIPIOS_URLS.items():
        partes = periodo.split("-")
        if len(partes) == 2:
            inicio, fim = int(partes[0]), int(partes[1])
            if inicio <= ano <= fim:
                return periodo
        elif len(partes) == 1:
            if ano == int(partes[0]):
                return periodo
    return None


class PrecoDiesel(BaseModel):
    """Observacao de preco de diesel (semanal, por municipio/UF/Brasil)."""

    data: date
    uf: str = Field("", max_length=2)
    municipio: str = ""
    produto: str
    preco_venda: float | None = None
    preco_compra: float | None = None
    margem: float | None = None
    n_postos: int | None = None

    @field_validator("preco_venda", "preco_compra", "margem", mode="before")
    @classmethod
    def convert_numeric(cls, v: Any) -> float | None:
        if v is None or v == "" or v == "-":
            return None
        try:
            return float(v)
        except (ValueError, TypeError):
            return None

    @field_validator("n_postos", mode="before")
    @classmethod
    def convert_int(cls, v: Any) -> int | None:
        if v is None or v == "" or v == "-":
            return None
        try:
            return int(float(v))
        except (ValueError, TypeError):
            return None


class VendaDiesel(BaseModel):
    """Volume de venda de diesel (mensal, por UF)."""

    data: date
    uf: str = Field("", max_length=2)
    regiao: str = ""
    produto: str = ""
    volume_m3: float | None = None

    @field_validator("volume_m3", mode="before")
    @classmethod
    def convert_volume(cls, v: Any) -> float | None:
        if v is None or v == "" or v == "-":
            return None
        try:
            return float(v)
        except (ValueError, TypeError):
            return None
