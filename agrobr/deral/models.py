from __future__ import annotations

from agrobr.exceptions import InvalidParameterError
from agrobr.normalize import crops

DERAL_PRODUTOS_PUBLICADOS: tuple[str, ...] = (
    "cafe",
    "cevada",
    "feijao_1",
    "feijao_2",
    "milho_1",
    "milho_2",
    "soja",
    "trigo",
)
"""Culturas observadas no PC.xls entre fevereiro e setembro de 2026."""

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
    "feijão 1a safra": "feijao_1",
    "feijão 2a safra": "feijao_2",
    "feijao 1ª safra": "feijao_1",
    "feijao 2ª safra": "feijao_2",
    "feijao 1a safra": "feijao_1",
    "feijao 2a safra": "feijao_2",
    "mandioca": "mandioca",
    "cana-de-açúcar": "cana",
    "cana": "cana",
    "café": "cafe",
    "cafe": "cafe",
    "aveia": "aveia",
    "cevada": "cevada",
    "canola": "canola",
}


PRODUTOS_ACEITOS: tuple[str, ...] = (*DERAL_PRODUTOS_PUBLICADOS, "feijao", "milho")
"""`feijao` e `milho` juntam a 1ª e a 2ª safra."""


def normalize_produto(nome: str) -> str:
    key = nome.strip().lower()
    return _PRODUTO_ALIASES.get(key, key)


def validate_produto(produto: str | None) -> str | None:
    if produto is None:
        return None
    chave = normalize_produto(produto) if isinstance(produto, str) else None
    if chave not in PRODUTOS_ACEITOS and isinstance(produto, str):
        chave = crops.normalizar_cultura(produto)
    if chave not in PRODUTOS_ACEITOS:
        raise InvalidParameterError(
            f"produto inválido: {produto!r}. Valores válidos: {sorted(PRODUTOS_ACEITOS)}"
        )
    return chave
