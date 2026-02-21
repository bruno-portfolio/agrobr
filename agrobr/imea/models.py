from __future__ import annotations

from pydantic import BaseModel, field_validator

IMEA_CADEIAS: dict[str, int] = {
    "soja": 4,
    "soybeans": 4,
    "milho": 3,
    "corn": 3,
    "algodao": 1,
    "cotton": 1,
    "bovinocultura": 2,
    "cattle": 2,
    "suinocultura": 7,
    "pork": 7,
    "leite": 8,
    "dairy": 8,
}

_CADEIA_NAMES: dict[int, str] = {
    1: "algodao",
    2: "bovinocultura",
    3: "milho",
    4: "soja",
    5: "conjuntura",
    7: "suinocultura",
    8: "leite",
}

IMEA_MACRORREGIOES: list[str] = [
    "Centro-Sul",
    "Médio-Norte",
    "Nordeste",
    "Noroeste",
    "Norte",
    "Oeste",
    "Sudeste",
]

IMEA_COLUMNS_MAP: dict[str, str] = {
    "Localidade": "localidade",
    "Valor": "valor",
    "Variacao": "variacao",
    "Safra": "safra",
    "IndicadorFinalId": "indicador_id",
    "CadeiaId": "cadeia_id",
    "DataPublicacao": "data_publicacao",
    "TipoLocalidadeId": "tipo_localidade_id",
    "UnidadeSigla": "unidade",
    "UnidadeDescricao": "unidade_descricao",
}


def resolve_cadeia_id(nome: str) -> int:
    key = nome.strip().lower()
    if key in IMEA_CADEIAS:
        return IMEA_CADEIAS[key]
    try:
        cadeia_id = int(key)
        if cadeia_id in _CADEIA_NAMES:
            return cadeia_id
    except ValueError:
        pass
    raise ValueError(
        f"Cadeia desconhecida: '{nome}'. Opções: {list(dict.fromkeys(IMEA_CADEIAS.keys()))}"
    )


def cadeia_name(cadeia_id: int) -> str:
    return _CADEIA_NAMES.get(cadeia_id, str(cadeia_id))


class CotacaoIMEA(BaseModel):
    cadeia: str
    localidade: str
    valor: float | None = None
    variacao: float | None = None
    safra: str = ""
    unidade: str = ""
    data_publicacao: str = ""

    @field_validator("cadeia", mode="before")
    @classmethod
    def normalize_cadeia(cls, v: str) -> str:
        return v.strip().lower()

    @field_validator("localidade", mode="before")
    @classmethod
    def normalize_localidade(cls, v: str) -> str:
        return v.strip()
