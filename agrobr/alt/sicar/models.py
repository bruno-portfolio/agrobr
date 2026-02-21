from __future__ import annotations

WFS_BASE = "https://geoserver.car.gov.br/geoserver/sicar/wfs"
WFS_VERSION = "2.0.0"

PAGE_SIZE = 10_000
MAX_FEATURES_WARNING = 100_000


def layer_name(uf: str) -> str:
    return f"sicar_imoveis_{uf.lower()}"


PROPERTY_NAMES = [
    "cod_imovel",
    "status_imovel",
    "dat_criacao",
    "data_atualizacao",
    "area",
    "condicao",
    "uf",
    "municipio",
    "cod_municipio_ibge",
    "m_fiscal",
    "tipo_imovel",
]

RENAME_MAP = {
    "status_imovel": "status",
    "dat_criacao": "data_criacao",
    "area": "area_ha",
    "m_fiscal": "modulos_fiscais",
    "tipo_imovel": "tipo",
}

COLUNAS_IMOVEIS = [
    "cod_imovel",
    "status",
    "data_criacao",
    "data_atualizacao",
    "area_ha",
    "condicao",
    "uf",
    "municipio",
    "cod_municipio_ibge",
    "modulos_fiscais",
    "tipo",
]

STATUS_VALIDOS = frozenset({"AT", "PE", "SU", "CA"})
STATUS_LABELS = {
    "AT": "Ativo",
    "PE": "Pendente",
    "SU": "Suspenso",
    "CA": "Cancelado",
}

TIPO_VALIDOS = frozenset({"IRU", "AST", "PCT"})
TIPO_LABELS = {
    "IRU": "Rural",
    "AST": "Assentamento",
    "PCT": "Terra Indigena",
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
