from __future__ import annotations

from pydantic import BaseModel, Field

SAFRAS_URLS: dict[str, str] = {
    "2023/2024": "https://fundacaorioverde.com.br/wp-content/uploads/2025/07/Competicao-de-Cultivares-de-Soja-Safra-202324-.pdf",
    "2024/2025": "https://www.fundacaorioverde.com.br/wp-content/uploads/2025/07/Competicao-de-Cultivares-de-Soja-Safra-2024-25.pdf",
    "2025/2026": "https://fundacaorioverde.com.br/wp-content/uploads/2026/03/Competicao-de-Cultivares-de-Soja-Safra-25-26.pdf",
}

SAFRAS_FORA_DO_LAYOUT: dict[str, str] = {
    "2022/2023": "3 épocas de semeio e sem produtividade média",
}

COLUNAS_SAIDA: list[str] = [
    "safra",
    "empresa",
    "cultivar",
    "grupo_maturacao",
    "ciclo_dias",
    "produtividade_1_epoca_sc_ha",
    "produtividade_2_epoca_sc_ha",
    "produtividade_3_epoca_sc_ha",
    "produtividade_4_epoca_sc_ha",
    "produtividade_media_sc_ha",
]

MIN_PDF_SIZE = 50_000


class EnsaioSoja(BaseModel):
    safra: str
    empresa: str = Field(min_length=2)
    cultivar: str = Field(min_length=2)
    grupo_maturacao: str = Field(pattern=r"^\d{1,2}[.,]\d+$")
    ciclo_dias: int = Field(ge=1, le=365)
    produtividade_1_epoca_sc_ha: float | None = Field(ge=0)
    produtividade_2_epoca_sc_ha: float | None = Field(ge=0)
    produtividade_3_epoca_sc_ha: float | None = Field(ge=0)
    produtividade_4_epoca_sc_ha: float | None = Field(ge=0)
    produtividade_media_sc_ha: float = Field(ge=0)
