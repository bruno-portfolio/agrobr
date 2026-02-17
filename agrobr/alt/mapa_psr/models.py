"""Constantes e modelos Pydantic para MAPA PSR (seguro rural)."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, field_validator

DATASET_ID = "baefdc68-9bad-4204-83e8-f2888b79ab48"

_BASE_URL = (
    "https://dados.agricultura.gov.br/dataset"
    f"/{DATASET_ID}/resource/{{resource_id}}/download/{{filename}}"
)

CSV_RESOURCES: dict[str, dict[str, str]] = {
    "2006-2015": {
        "resource_id": "97f29a77-4e7e-44bf-99b3-a2d75911b6bf",
        "filename": "dados_abertos_psr_2006a2015csv.csv",
    },
    "2016-2024": {
        "resource_id": "54e04a6b-15b3-4bda-a330-b8e805deabe4",
        "filename": "dados_abertos_psr_2016a2024csv.csv",
    },
    "2025": {
        "resource_id": "ac7e4351-974f-4958-9294-627c5cbf289a",
        "filename": "dados_abertos_psr_2025csv.csv",
    },
}

PERIODOS_VALIDOS = frozenset(CSV_RESOURCES.keys())

COLUNAS_PII = frozenset({"NM_SEGURADO", "NR_DOCUMENTO_SEGURADO"})

COLUNAS_GEO = frozenset(
    {
        "LATITUDE",
        "LONGITUDE",
        "NR_GRAU_LAT",
        "NR_MIN_LAT",
        "NR_SEG_LAT",
        "NR_GRAU_LONG",
        "NR_MIN_LONG",
        "NR_SEG_LONG",
    }
)

COLUNAS_DROP = COLUNAS_PII | COLUNAS_GEO

COLUNAS_CSV: dict[str, str] = {
    "ANO_APOLICE": "ano_apolice",
    "NR_APOLICE": "nr_apolice",
    "SG_UF_PROPRIEDADE": "uf",
    "NM_MUNICIPIO_PROPRIEDADE": "municipio",
    "CD_GEOCMU": "cd_ibge",
    "NM_CULTURA_GLOBAL": "cultura",
    "NM_CLASSIF_PRODUTO": "classificacao",
    "NR_AREA_TOTAL": "area_total",
    "VL_PREMIO_LIQUIDO": "valor_premio",
    "VL_SUBVENCAO_FEDERAL": "valor_subvencao",
    "VL_LIMITE_GARANTIA": "valor_limite_garantia",
    "VALOR_INDENIZACAO": "valor_indenizacao",
    "EVENTO_PREPONDERANTE": "evento",
    "NR_PRODUTIVIDADE_ESTIMADA": "produtividade_estimada",
    "NR_PRODUTIVIDADE_SEGURADA": "produtividade_segurada",
    "NivelDeCobertura": "nivel_cobertura",
    "PE_TAXA": "taxa",
    "NM_RAZAO_SOCIAL": "seguradora",
}

COLUNAS_FLOAT = frozenset(
    {
        "area_total",
        "valor_premio",
        "valor_subvencao",
        "valor_limite_garantia",
        "valor_indenizacao",
        "produtividade_estimada",
        "produtividade_segurada",
        "nivel_cobertura",
        "taxa",
    }
)

COLUNAS_SINISTROS = [
    "nr_apolice",
    "ano_apolice",
    "uf",
    "municipio",
    "cd_ibge",
    "cultura",
    "classificacao",
    "evento",
    "area_total",
    "valor_indenizacao",
    "valor_premio",
    "valor_subvencao",
    "valor_limite_garantia",
    "produtividade_estimada",
    "produtividade_segurada",
    "nivel_cobertura",
    "seguradora",
]

COLUNAS_APOLICES = [
    "nr_apolice",
    "ano_apolice",
    "uf",
    "municipio",
    "cd_ibge",
    "cultura",
    "classificacao",
    "area_total",
    "valor_premio",
    "valor_subvencao",
    "valor_limite_garantia",
    "valor_indenizacao",
    "evento",
    "produtividade_estimada",
    "produtividade_segurada",
    "nivel_cobertura",
    "taxa",
    "seguradora",
]

CLASSIFICACOES_VALIDAS = frozenset(
    {
        "AGRICOLA",
        "PECUARIO",
        "AQUICOLA",
        "FLORESTAL",
        "PENHOR RURAL",
        "BENFEITORIAS",
    }
)

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

ANO_INICIO_PSR = 2006


def _resolve_periodos(
    ano_inicio: int | None = None,
    ano_fim: int | None = None,
) -> list[str]:
    """Resolve range de anos para lista de chaves de CSV_RESOURCES."""
    if ano_inicio is None and ano_fim is None:
        return list(CSV_RESOURCES.keys())

    resultado: list[str] = []
    for periodo in CSV_RESOURCES:
        partes = periodo.split("-")
        if len(partes) == 2:
            p_inicio, p_fim = int(partes[0]), int(partes[1])
        else:
            p_inicio = p_fim = int(partes[0])

        if ano_inicio is not None and ano_fim is not None:
            if p_fim >= ano_inicio and p_inicio <= ano_fim:
                resultado.append(periodo)
        elif ano_inicio is not None:
            if p_fim >= ano_inicio:
                resultado.append(periodo)
        elif ano_fim is not None and p_inicio <= ano_fim:
            resultado.append(periodo)

    return resultado


def get_csv_url(periodo: str) -> str:
    """Retorna URL completa do CSV para o periodo."""
    if periodo not in CSV_RESOURCES:
        raise ValueError(f"Periodo '{periodo}' invalido. Opcoes: {sorted(CSV_RESOURCES.keys())}")
    info = CSV_RESOURCES[periodo]
    return _BASE_URL.format(
        resource_id=info["resource_id"],
        filename=info["filename"],
    )


class ApoliceSeguroRural(BaseModel):
    """Apolice de seguro rural com subvencao federal."""

    nr_apolice: str = ""
    ano_apolice: int
    uf: str = Field("", max_length=2)
    municipio: str = ""
    cd_ibge: str = ""
    cultura: str = ""
    classificacao: str = ""
    area_total: float | None = None
    valor_premio: float | None = None
    valor_subvencao: float | None = None
    valor_limite_garantia: float | None = None
    valor_indenizacao: float | None = None
    evento: str = ""
    produtividade_estimada: float | None = None
    produtividade_segurada: float | None = None
    nivel_cobertura: float | None = None
    taxa: float | None = None
    seguradora: str = ""

    @field_validator(
        "area_total",
        "valor_premio",
        "valor_subvencao",
        "valor_limite_garantia",
        "valor_indenizacao",
        "produtividade_estimada",
        "produtividade_segurada",
        "nivel_cobertura",
        "taxa",
        mode="before",
    )
    @classmethod
    def convert_numeric(cls, v: Any) -> float | None:
        if v is None or v == "" or v == "-":
            return None
        try:
            return float(v)
        except (ValueError, TypeError):
            return None

    @field_validator("ano_apolice", mode="before")
    @classmethod
    def convert_ano(cls, v: Any) -> int:
        if v is None or v == "":
            raise ValueError("ano_apolice is required")
        return int(float(v))
