from __future__ import annotations

from pydantic import BaseModel, Field, field_validator


class ExportRecord(BaseModel):
    ano: int = Field(..., ge=1997)
    mes: int = Field(..., ge=1, le=12)
    ncm: str = Field(..., min_length=8, max_length=8)
    uf: str = Field(..., min_length=2, max_length=2)
    pais_destino: str | None = None
    porto: str | None = None
    kg_liquido: float = Field(..., ge=0)
    valor_fob_usd: float = Field(..., ge=0)

    @field_validator("uf", mode="before")
    @classmethod
    def normalize_uf(cls, v: str) -> str:
        return v.upper().strip()


NCM_PRODUTOS: dict[str, str] = {
    "soja": "12019000",
    "soja_grao": "12019000",
    "soja_semeadura": "12011000",
    "oleo_soja_bruto": "15071000",
    "farelo_soja": "23040010",
    "milho": "10059010",
    "arroz": "10063021",
    "trigo": "10019900",
    "algodao": "520100",
    "algodao_cardado": "520300",
    "cafe": "09011110",
    "cafe_arabica": "09011110",
    "cafe_conilon": "09011190",
    "acucar": "17011400",
    "etanol": "22071000",
    "carne_bovina": "02023000",
    "carne_frango": "02071400",
    "carne_suina": "02032900",
}


def resolve_ncm(produto: str) -> str:
    lower = produto.lower().strip()
    ncm = NCM_PRODUTOS.get(lower)
    if ncm is None:
        raise ValueError(
            f"Produto '{produto}' sem mapeamento NCM. "
            f"Produtos disponíveis: {list(NCM_PRODUTOS.keys())}"
        )
    return ncm
