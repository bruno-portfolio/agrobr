from __future__ import annotations

BIOMAS = {
    "amazonia": "Amazônia",
    "amazônia": "Amazônia",
    "cerrado": "Cerrado",
    "mata atlantica": "Mata Atlântica",
    "mata atlântica": "Mata Atlântica",
    "caatinga": "Caatinga",
    "pampa": "Pampa",
    "pantanal": "Pantanal",
}

BIOMAS_VALIDOS = {"Amazônia", "Cerrado", "Mata Atlântica", "Caatinga", "Pampa", "Pantanal"}

SATELITES = {
    "AQUA_M-T",
    "AQUA_M-M",
    "TERRA_M-T",
    "TERRA_M-M",
    "NOAA-20",
    "NOAA-21",
    "GOES-16",
    "GOES-19",
    "METOP-B",
    "METOP-C",
    "MSG-03",
    "NPP-375",
    "NPP-375D",
}

SATELITE_REFERENCIA = "AQUA_M-T"

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


def normalizar_bioma(bioma: str) -> str:
    key = bioma.strip().lower()
    return BIOMAS.get(key, bioma.strip())


def estado_para_uf(estado: str) -> str:
    return UF_ESTADO.get(estado.strip().upper(), estado.strip())
