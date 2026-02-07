"""Aliases de produto — resolução unificada cross-source.

Permite usar nomes variantes/informais (ex: "soy", "café", "boi gordo")
que são resolvidos para o nome canônico do agrobr.
"""

from __future__ import annotations

# Mapeamento de aliases → nome canônico agrobr.
# Nomes canônicos são as chaves já usadas nos dicts de cada source
# (CEPEA_PRODUTOS, CONAB_PRODUTOS, NCM_PRODUTOS, etc.)
PRODUCT_ALIASES: dict[str, str] = {
    # ── Soja ──────────────────────────────
    "soy": "soja",
    "soybean": "soja",
    "soybeans": "soja",
    "soja_grao": "soja",
    "soja_grão": "soja",
    # ── Milho ─────────────────────────────
    "corn": "milho",
    "maize": "milho",
    "milho_total": "milho",
    # ── Café ──────────────────────────────
    "café": "cafe",
    "coffee": "cafe",
    "cafe_arabica": "cafe",
    "café_arábica": "cafe",
    "arabica": "cafe",
    "arábica": "cafe",
    # ── Algodão ───────────────────────────
    "algodão": "algodao",
    "cotton": "algodao",
    "algodao_herbaceo": "algodao",
    "algodão_herbáceo": "algodao",
    # ── Boi gordo ─────────────────────────
    "boi_gordo": "boi",
    "boi gordo": "boi",
    "cattle": "boi",
    "beef": "boi",
    # ── Trigo ─────────────────────────────
    "wheat": "trigo",
    # ── Arroz ─────────────────────────────
    "rice": "arroz",
    "arroz_casca": "arroz",
    # ── Açúcar ────────────────────────────
    "açúcar": "acucar",
    "açucar": "acucar",
    "sugar": "acucar",
    "acucar_cristal": "acucar",
    "açúcar_cristal": "acucar",
    # ── Etanol ────────────────────────────
    "etanol": "etanol_hidratado",
    "ethanol": "etanol_hidratado",
    # ── Frango ────────────────────────────
    "frango": "frango_congelado",
    "chicken": "frango_congelado",
    # ── Suíno ─────────────────────────────
    "suíno": "suino",
    "porco": "suino",
    "pork": "suino",
    # ── Feijão ────────────────────────────
    "feijão": "feijao",
    "feijao_total": "feijao",
    "bean": "feijao",
    "beans": "feijao",
    # ── Leite ─────────────────────────────
    "milk": "leite",
    # ── Laranja ───────────────────────────
    "laranja": "laranja_industria",
    "orange": "laranja_industria",
    # ── Cana ──────────────────────────────
    "cana_de_acucar": "cana",
    "cana_de_açúcar": "cana",
    "sugarcane": "cana",
    # ── Mandioca ──────────────────────────
    "cassava": "mandioca",
}


def resolve_alias(produto: str) -> str:
    """Resolve alias de produto para nome canônico agrobr.

    Se o nome já é canônico ou não tem alias, retorna como está.

    Args:
        produto: Nome do produto (qualquer variante).

    Returns:
        Nome canônico do produto em minúsculas.

    Example:
        >>> resolve_alias("soy")
        'soja'
        >>> resolve_alias("café")
        'cafe'
        >>> resolve_alias("soja")
        'soja'
    """
    key = produto.strip().lower()
    return PRODUCT_ALIASES.get(key, key)


def list_aliases(produto: str | None = None) -> dict[str, str] | list[str]:
    """Lista aliases disponíveis.

    Args:
        produto: Se fornecido, retorna apenas aliases para esse produto.
                 Se None, retorna todo o mapeamento.

    Returns:
        Dict de alias → canônico, ou lista de aliases para um produto.
    """
    if produto is None:
        return dict(PRODUCT_ALIASES)

    canonical = resolve_alias(produto)
    return [k for k, v in PRODUCT_ALIASES.items() if v == canonical]
