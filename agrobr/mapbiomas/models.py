from __future__ import annotations

COLECAO_ATUAL = 10

ANO_INICIO = 1985
ANO_FIM = 2024

BIOMAS_VALIDOS = {"Amazônia", "Caatinga", "Cerrado", "Mata Atlântica", "Pampa", "Pantanal"}

BIOMAS: dict[str, str] = {
    "amazonia": "Amazônia",
    "amazônia": "Amazônia",
    "cerrado": "Cerrado",
    "mata atlantica": "Mata Atlântica",
    "mata atlântica": "Mata Atlântica",
    "caatinga": "Caatinga",
    "pampa": "Pampa",
    "pantanal": "Pantanal",
}

CLASSES_LEGENDA: dict[int, str] = {
    1: "Floresta",
    3: "Formação Florestal",
    4: "Formação Savânica",
    5: "Mangue",
    6: "Floresta Alagável",
    9: "Silvicultura",
    10: "Vegetação Herbácea e Arbustiva",
    11: "Campo Alagado e Área Pantanosa",
    12: "Formação Campestre",
    14: "Agropecuária",
    15: "Pastagem",
    18: "Agricultura",
    19: "Lavoura Temporária",
    20: "Cana",
    21: "Mosaico de Usos",
    22: "Área não Vegetada",
    23: "Praia, Duna e Areal",
    24: "Área Urbanizada",
    25: "Outras Áreas não Vegetadas",
    26: "Corpo D'Água",
    27: "Não observado",
    29: "Afloramento Rochoso",
    30: "Mineração",
    31: "Aquicultura",
    32: "Apicum",
    33: "Rio, Lago e Oceano",
    35: "Dendê",
    36: "Lavoura Perene",
    39: "Soja",
    40: "Arroz",
    41: "Outras Lavouras Temporárias",
    46: "Café",
    47: "Citrus",
    48: "Outras Lavouras Perenes",
    49: "Restinga Arbórea",
    50: "Restinga Herbácea",
    62: "Algodão",
    75: "Não definido",
}

CLASSES_LEVEL_0: dict[str, str] = {
    "Natural": "Natural",
    "Antropic": "Antrópico",
    "Natural/Antropic": "Natural/Antrópico",
    "Undefined": "Indefinido",
}

NIVEIS_COLUNA: dict[int, str] = {
    0: "class_level_0",
    1: "class_level_1",
    2: "class_level_2",
    3: "class_level_3",
    4: "class_level_4",
}

SHEET_COBERTURA = "COVERAGE_10"
SHEET_TRANSICAO = "TRANSITION_10"
SHEET_LEGENDA = "LEGEND_CODE"

COLUNAS_SAIDA_COBERTURA = [
    "bioma",
    "estado",
    "classe_id",
    "classe",
    "nivel_0",
    "ano",
    "area_ha",
]

COLUNAS_SAIDA_TRANSICAO = [
    "bioma",
    "estado",
    "classe_de_id",
    "classe_de",
    "classe_para_id",
    "classe_para",
    "periodo",
    "area_ha",
]

ESTADOS_MAPBIOMAS: dict[str, str] = {
    "Acre": "AC",
    "Alagoas": "AL",
    "Amapá": "AP",
    "Amazonas": "AM",
    "Bahia": "BA",
    "Ceará": "CE",
    "Distrito Federal": "DF",
    "Espírito Santo": "ES",
    "Goiás": "GO",
    "Maranhão": "MA",
    "Mato Grosso": "MT",
    "Mato Grosso do Sul": "MS",
    "Minas Gerais": "MG",
    "Pará": "PA",
    "Paraíba": "PB",
    "Paraná": "PR",
    "Pernambuco": "PE",
    "Piauí": "PI",
    "Rio de Janeiro": "RJ",
    "Rio Grande do Norte": "RN",
    "Rio Grande do Sul": "RS",
    "Rondônia": "RO",
    "Roraima": "RR",
    "Santa Catarina": "SC",
    "São Paulo": "SP",
    "Sergipe": "SE",
    "Tocantins": "TO",
}


def normalizar_bioma(bioma: str) -> str:
    key = bioma.strip().lower()
    return BIOMAS.get(key, bioma.strip())


def estado_para_uf(estado: str) -> str:
    normalized = estado.strip().rstrip("\n")
    return ESTADOS_MAPBIOMAS.get(normalized, normalized)


def classe_para_nome(classe_id: int) -> str:
    return CLASSES_LEGENDA.get(classe_id, f"Classe {classe_id}")
