"""Constantes e modelos para ANTT Pedagio (fluxo de veiculos em pracas)."""

from __future__ import annotations

from datetime import date

CKAN_BASE = "https://dados.antt.gov.br"
CKAN_API = f"{CKAN_BASE}/api/3/action"

DATASET_TRAFEGO_SLUG = "fluxo-de-veiculos-nas-pracas-de-pedagio"
DATASET_PRACAS_SLUG = "cadastro-de-pracas-de-pedagio"

ANO_INICIO = 2010
ANO_FIM_V1 = 2023
ANO_INICIO_V2 = 2024

# Mapeamento Categoria V1 (2010-2023) -> (n_eixos, tipo_veiculo)
CATEGORIA_MAP: dict[str, tuple[int, str]] = {
    "Categoria 1": (2, "Passeio"),
    "Categoria 2": (2, "Comercial"),
    "Categoria 3": (3, "Passeio"),
    "Categoria 4": (3, "Comercial"),
    "Categoria 5": (4, "Passeio"),
    "Categoria 6": (4, "Comercial"),
    "Categoria 7": (5, "Comercial"),
    "Categoria 8": (6, "Comercial"),
    "Categoria 9": (2, "Moto"),
}

# Mapeamento numerico V2 (2024+): n_eixos -> tipo_veiculo
# V2 ja vem com contagem de eixos como inteiro; tipo inferido pelo numero
EIXOS_TIPO_MAP: dict[int, str] = {
    2: "Passeio",  # default 2 eixos = passeio (pode ser comercial leve)
    3: "Comercial",
    4: "Comercial",
    5: "Comercial",
    6: "Comercial",
    7: "Comercial",
    8: "Comercial",
    9: "Comercial",
    10: "Comercial",
    11: "Comercial",
    12: "Comercial",
    13: "Comercial",
    14: "Comercial",
    15: "Comercial",
    16: "Comercial",
    17: "Comercial",
    18: "Comercial",
}

# Colunas esperadas no output final de fluxo
COLUNAS_FLUXO = [
    "data",
    "concessionaria",
    "praca",
    "sentido",
    "n_eixos",
    "tipo_veiculo",
    "volume",
    "rodovia",
    "uf",
    "municipio",
]

# Colunas V1 (2010-2023) — CSV com header
COLUNAS_V1 = [
    "concessionaria",
    "praca",
    "mes_ano",
    "categoria",
    "tipo_cobranca",
    "sentido",
    "quantidade",
]

# Colunas V2 (2024+) — CSV SEM header
COLUNAS_V2 = [
    "concessionaria",
    "praca",
    "mes_ano",
    "categoria_eixo",
    "tipo_cobranca",
    "sentido",
    "quantidade",
]

# Colunas do cadastro de pracas
COLUNAS_PRACAS = [
    "concessionaria",
    "praca_de_pedagio",
    "rodovia",
    "uf",
    "km_m",
    "municipio",
    "lat",
    "lon",
    "situacao",
]

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


def _resolve_anos(
    ano: int | None = None,
    ano_inicio: int | None = None,
    ano_fim: int | None = None,
) -> list[int]:
    """Resolve parametros de ano para lista de anos a baixar.

    Default: ano atual + anterior (evita download de 16 CSVs).
    """
    if ano is not None:
        return [ano]

    if ano_inicio is not None or ano_fim is not None:
        start = ano_inicio or ANO_INICIO
        end = ano_fim or date.today().year
        return list(range(start, end + 1))

    # Default: ultimos 2 anos
    current = date.today().year
    return [current - 1, current]


def schema_version(ano: int) -> int:
    """Retorna versao do schema (1 ou 2) baseado no ano."""
    if ano >= ANO_INICIO_V2:
        return 2
    return 1


def build_ckan_package_url(slug: str) -> str:
    """URL da API CKAN para obter metadados do dataset."""
    return f"{CKAN_API}/package_show?id={slug}"


def build_ckan_resource_url(resource_id: str) -> str:
    """URL de download de um resource CKAN."""
    return f"{CKAN_BASE}/dataset/file/{resource_id}"
