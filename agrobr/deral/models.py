"""Modelos e constantes para dados DERAL (Depto. Economia Rural — PR).

Fonte: SEAB/DERAL (agricultura.pr.gov.br/deral).
Dados semanais de condição das lavouras e progresso de safra no Paraná.
"""

from __future__ import annotations

from pydantic import BaseModel, field_validator

# Produtos monitorados pelo DERAL (nomes canônicos)
DERAL_PRODUTOS: dict[str, str] = {
    "soja": "Soja",
    "milho": "Milho",
    "milho_1": "Milho 1ª safra",
    "milho_2": "Milho 2ª safra",
    "trigo": "Trigo",
    "feijao": "Feijão",
    "feijao_1": "Feijão 1ª safra",
    "feijao_2": "Feijão 2ª safra",
    "mandioca": "Mandioca",
    "cana": "Cana-de-açúcar",
    "cafe": "Café",
    "aveia": "Aveia",
    "cevada": "Cevada",
    "canola": "Canola",
}

# Condições possíveis (rating de 1 a 3)
CONDICOES = ["boa", "media", "ruim"]

# Estágios fenológicos comuns
ESTAGIOS = [
    "germinacao",
    "vegetativo",
    "florescimento",
    "frutificacao",
    "maturacao",
]

# Mapeamento de nomes no XLS -> canônicos
_PRODUTO_ALIASES: dict[str, str] = {
    "soja": "soja",
    "milho": "milho",
    "milho 1ª safra": "milho_1",
    "milho 2ª safra": "milho_2",
    "milho 1a safra": "milho_1",
    "milho 2a safra": "milho_2",
    "milho verão": "milho_1",
    "milho safrinha": "milho_2",
    "trigo": "trigo",
    "feijão": "feijao",
    "feijao": "feijao",
    "feijão 1ª safra": "feijao_1",
    "feijão 2ª safra": "feijao_2",
    "mandioca": "mandioca",
    "cana-de-açúcar": "cana",
    "cana": "cana",
    "café": "cafe",
    "cafe": "cafe",
    "aveia": "aveia",
    "cevada": "cevada",
    "canola": "canola",
}

# Mapeamento condição XLS -> canônico
_CONDICAO_ALIASES: dict[str, str] = {
    "boa": "boa",
    "bom": "boa",
    "média": "media",
    "media": "media",
    "regular": "media",
    "ruim": "ruim",
    "má": "ruim",
    "ma": "ruim",
}


def normalize_produto(nome: str) -> str:
    """Normaliza nome de produto para chave canônica."""
    key = nome.strip().lower()
    return _PRODUTO_ALIASES.get(key, key)


def normalize_condicao(cond: str) -> str:
    """Normaliza condição para chave canônica (boa/media/ruim)."""
    key = cond.strip().lower()
    return _CONDICAO_ALIASES.get(key, key)


class CondicaoLavoura(BaseModel):
    """Registro de condição de lavoura DERAL."""

    produto: str
    data: str = ""
    estagio: str = ""
    condicao: str = ""
    pct: float | None = None
    plantio_pct: float | None = None
    colheita_pct: float | None = None

    @field_validator("produto", mode="before")
    @classmethod
    def normalize_prod(cls, v: str) -> str:
        return normalize_produto(v)

    @field_validator("condicao", mode="before")
    @classmethod
    def normalize_cond(cls, v: str) -> str:
        if not v:
            return v
        return normalize_condicao(v)
