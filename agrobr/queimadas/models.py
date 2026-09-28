from __future__ import annotations

from agrobr.normalize.regions import BIOMAS as BIOMAS  # noqa: F401
from agrobr.normalize.regions import BIOMAS_VALIDOS as BIOMAS_VALIDOS  # noqa: F401
from agrobr.normalize.regions import normalizar_bioma as normalizar_bioma  # noqa: F401

COLUNAS_CSV = [
    "id",
    "lat",
    "lon",
    "data_hora_gmt",
    "satelite",
    "municipio",
    "estado",
    "pais",
    "municipio_id",
    "estado_id",
    "pais_id",
    "numero_dias_sem_chuva",
    "precipitacao",
    "risco_fogo",
    "bioma",
    "frp",
]

COLUNAS_SAIDA = [
    "data",
    "hora_gmt",
    "lat",
    "lon",
    "satelite",
    "municipio",
    "municipio_id",
    "estado",
    "bioma",
    "numero_dias_sem_chuva",
    "precipitacao",
    "risco_fogo",
    "frp",
]

UF_ESTADO: dict[str, str] = {
    "ACRE": "AC",
    "ALAGOAS": "AL",
    "AMAPÁ": "AP",
    "AMAZONAS": "AM",
    "BAHIA": "BA",
    "CEARÁ": "CE",
    "DISTRITO FEDERAL": "DF",
    "ESPÍRITO SANTO": "ES",
    "GOIÁS": "GO",
    "MARANHÃO": "MA",
    "MATO GROSSO": "MT",
    "MATO GROSSO DO SUL": "MS",
    "MINAS GERAIS": "MG",
    "PARÁ": "PA",
    "PARAÍBA": "PB",
    "PARANÁ": "PR",
    "PERNAMBUCO": "PE",
    "PIAUÍ": "PI",
    "RIO DE JANEIRO": "RJ",
    "RIO GRANDE DO NORTE": "RN",
    "RIO GRANDE DO SUL": "RS",
    "RONDÔNIA": "RO",
    "RORAIMA": "RR",
    "SANTA CATARINA": "SC",
    "SÃO PAULO": "SP",
    "SERGIPE": "SE",
    "TOCANTINS": "TO",
}


COLUNAS_SAIDA_GEO = COLUNAS_SAIDA + ["geometry"]

CHAVE = ["data", "hora_gmt", "lat", "lon", "satelite"]


def estado_para_uf(estado: str) -> str:
    return UF_ESTADO.get(estado.strip().upper(), estado.strip())
