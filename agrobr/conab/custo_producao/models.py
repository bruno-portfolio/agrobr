"""Modelos Pydantic para custo de produção CONAB."""

from __future__ import annotations

from pydantic import BaseModel, Field, field_validator


class ItemCusto(BaseModel):
    """Item individual de custo de produção por hectare."""

    cultura: str = Field(..., min_length=2)
    uf: str = Field(..., min_length=2, max_length=2)
    safra: str = Field(..., pattern=r"^\d{4}/\d{2}$")
    tecnologia: str = Field(default="alta")

    categoria: str = Field(
        ...,
        description="Categoria do custo: insumos, operacoes, mao_de_obra, custos_fixos, outros",
    )
    item: str = Field(..., min_length=1)
    unidade: str | None = None
    quantidade_ha: float | None = Field(None, ge=0)
    preco_unitario: float | None = Field(None, ge=0)
    valor_ha: float = Field(..., ge=0)
    participacao_pct: float | None = Field(None, ge=0, le=100)

    @field_validator("cultura", mode="before")
    @classmethod
    def normalize_cultura(cls, v: str) -> str:
        return v.lower().strip()

    @field_validator("uf", mode="before")
    @classmethod
    def normalize_uf(cls, v: str) -> str:
        return v.upper().strip()

    @field_validator("tecnologia", mode="before")
    @classmethod
    def normalize_tecnologia(cls, v: str) -> str:
        return v.lower().strip()


class CustoTotal(BaseModel):
    """Agregado de custo por hectare (COE/COT/CT)."""

    cultura: str = Field(..., min_length=2)
    uf: str = Field(..., min_length=2, max_length=2)
    safra: str = Field(..., pattern=r"^\d{4}/\d{2}$")
    tecnologia: str = Field(default="alta")

    coe_ha: float = Field(..., ge=0, description="Custo Operacional Efetivo por hectare (R$)")
    cot_ha: float | None = Field(None, ge=0, description="Custo Operacional Total por hectare (R$)")
    ct_ha: float | None = Field(None, ge=0, description="Custo Total por hectare (R$)")

    @field_validator("cultura", mode="before")
    @classmethod
    def normalize_cultura(cls, v: str) -> str:
        return v.lower().strip()

    @field_validator("uf", mode="before")
    @classmethod
    def normalize_uf(cls, v: str) -> str:
        return v.upper().strip()

    @field_validator("tecnologia", mode="before")
    @classmethod
    def normalize_tecnologia(cls, v: str) -> str:
        return v.lower().strip()


# Mapeamento de nomes de cultura CONAB para nomes normalizados
CULTURAS_MAP: dict[str, str] = {
    "soja": "soja",
    "milho": "milho",
    "milho verao": "milho_verao",
    "milho verão": "milho_verao",
    "milho safrinha": "milho_safrinha",
    "milho 2a safra": "milho_safrinha",
    "arroz": "arroz",
    "arroz irrigado": "arroz_irrigado",
    "arroz sequeiro": "arroz_sequeiro",
    "feijao": "feijao",
    "feijão": "feijao",
    "algodao": "algodao",
    "algodão": "algodao",
    "trigo": "trigo",
    "cafe": "cafe",
    "café": "cafe",
    "cafe arabica": "cafe_arabica",
    "café arábica": "cafe_arabica",
    "cafe conilon": "cafe_conilon",
    "café conilon": "cafe_conilon",
    "mandioca": "mandioca",
    "cana": "cana",
    "cana de acucar": "cana",
    "cana-de-açúcar": "cana",
    "sorgo": "sorgo",
}

# Mapeamento de categorias brutas do Excel para nomes normalizados
CATEGORIAS_MAP: dict[str, str] = {
    # Insumos
    "sementes": "insumos",
    "fertilizantes": "insumos",
    "adubação de base": "insumos",
    "adubação de cobertura": "insumos",
    "corretivos": "insumos",
    "defensivos": "insumos",
    "herbicidas": "insumos",
    "inseticidas": "insumos",
    "fungicidas": "insumos",
    "adjuvantes": "insumos",
    "tratamento de sementes": "insumos",
    "inoculante": "insumos",
    # Operações mecânicas
    "operações com máquinas": "operacoes",
    "operações mecânicas": "operacoes",
    "preparo do solo": "operacoes",
    "plantio": "operacoes",
    "semeadura": "operacoes",
    "pulverização": "operacoes",
    "pulverizações": "operacoes",
    "colheita": "operacoes",
    "colheita mecânica": "operacoes",
    "transporte interno": "operacoes",
    # Mão de obra
    "mão de obra": "mao_de_obra",
    "mao de obra": "mao_de_obra",
    "mão de obra temporária": "mao_de_obra",
    "empreita": "mao_de_obra",
    # Custos fixos
    "depreciação": "custos_fixos",
    "depreciação de máquinas": "custos_fixos",
    "depreciação de benfeitorias": "custos_fixos",
    "manutenção periódica": "custos_fixos",
    "manutenção": "custos_fixos",
    "seguros": "custos_fixos",
    "juros sobre capital fixo": "custos_fixos",
    # Outros
    "assistência técnica": "outros",
    "arrendamento": "outros",
    "terra": "outros",
    "cessr": "outros",
    "funrural": "outros",
    "transporte externo": "outros",
    "armazenagem": "outros",
    "juros sobre capital de giro": "outros",
}


def classify_categoria(item_name: str) -> str:
    """Classifica um item de custo em sua categoria.

    Args:
        item_name: Nome do item de custo vindo da planilha.

    Returns:
        Categoria normalizada (insumos, operacoes, mao_de_obra, custos_fixos, outros).
    """
    lower = item_name.lower().strip()
    for key, cat in CATEGORIAS_MAP.items():
        if key in lower:
            return cat
    return "outros"


def normalize_cultura(nome: str) -> str:
    """Normaliza nome de cultura para chave padronizada.

    Args:
        nome: Nome bruto da cultura (pode conter acentos, maiúsculas).

    Returns:
        Nome normalizado (ex: "soja", "milho_safrinha").
    """
    lower = nome.lower().strip()
    return CULTURAS_MAP.get(lower, lower.replace(" ", "_"))
