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

CLASSES_DETER = {
    "DESMATAMENTO_CR",
    "DESMATAMENTO_VEG",
    "DEGRADACAO",
    "MINERACAO",
    "CICATRIZ_DE_QUEIMADA",
    "CS_DESORDENADO",
    "CS_GEOMETRICO",
}

PRODES_WORKSPACES: dict[str, str] = {
    "Amazônia": "prodes-cerrado-nb",
    "Cerrado": "prodes-cerrado-nb",
    "Caatinga": "prodes-caatinga-nb",
    "Mata Atlântica": "prodes-mata-atlantica-nb",
    "Pantanal": "prodes-pantanal-nb",
    "Pampa": "prodes-pampa-nb",
}

PRODES_LAYER = "yearly_deforestation"

DETER_WORKSPACES: dict[str, str] = {
    "Amazônia": "deter-amz",
    "Cerrado": "deter-cerrado-nb",
}

DETER_LAYERS: dict[str, str] = {
    "Amazônia": "deter_amz",
    "Cerrado": "deter_cerrado",
}

PRODES_COLUNAS_WFS = [
    "uid",
    "state",
    "main_class",
    "class_name",
    "year",
    "area_km",
    "source",
    "satellite",
    "sensor",
    "path_row",
    "publish_year",
]

DETER_COLUNAS_WFS_AMZ = [
    "classname",
    "quadrant",
    "path_row",
    "view_date",
    "sensor",
    "satellite",
    "areauckm",
    "areamunkm",
    "municipality",
    "mun_geocod",
    "uf",
    "publish_month",
]

DETER_COLUNAS_WFS_CERRADO = [
    "classname",
    "quadrant",
    "path_row",
    "view_date",
    "sensor",
    "satellite",
    "areatotalkm",
    "areauckm",
    "areamunkm",
    "municipality",
    "uf",
    "publish_month",
]

COLUNAS_SAIDA_PRODES = [
    "ano",
    "uf",
    "classe",
    "area_km2",
    "satelite",
    "sensor",
    "bioma",
]

COLUNAS_SAIDA_DETER = [
    "data",
    "classe",
    "uf",
    "municipio",
    "municipio_id",
    "area_km2",
    "satelite",
    "sensor",
    "bioma",
]


def normalizar_bioma(bioma: str) -> str:
    key = bioma.strip().lower()
    return BIOMAS.get(key, bioma.strip())


def estado_para_uf(estado: str) -> str:
    return UF_ESTADO.get(estado.strip().upper(), estado.strip())
