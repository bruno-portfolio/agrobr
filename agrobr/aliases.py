from __future__ import annotations

PRODUCT_ALIASES: dict[str, str] = {
    "soy": "soja",
    "soybean": "soja",
    "soybeans": "soja",
    "soja_grao": "soja",
    "soja_grão": "soja",
    "corn": "milho",
    "maize": "milho",
    "milho_total": "milho",
    "café": "cafe",
    "coffee": "cafe",
    "cafe_arabica": "cafe",
    "café_arábica": "cafe",
    "arabica": "cafe",
    "arábica": "cafe",
    "algodão": "algodao",
    "cotton": "algodao",
    "algodao_herbaceo": "algodao",
    "algodão_herbáceo": "algodao",
    "boi_gordo": "boi",
    "boi gordo": "boi",
    "cattle": "boi",
    "beef": "boi",
    "wheat": "trigo",
    "rice": "arroz",
    "arroz_casca": "arroz",
    "açúcar": "acucar",
    "açucar": "acucar",
    "sugar": "acucar",
    "acucar_cristal": "acucar",
    "açúcar_cristal": "acucar",
    "etanol": "etanol_hidratado",
    "ethanol": "etanol_hidratado",
    "frango": "frango_congelado",
    "chicken": "frango_congelado",
    "suíno": "suino",
    "porco": "suino",
    "pork": "suino",
    "feijão": "feijao",
    "feijao_total": "feijao",
    "bean": "feijao",
    "beans": "feijao",
    "milk": "leite",
    "laranja": "laranja_industria",
    "orange": "laranja_industria",
    "cana_de_acucar": "cana",
    "cana_de_açúcar": "cana",
    "sugarcane": "cana",
    "cassava": "mandioca",
}


def resolve_alias(produto: str) -> str:
    key = produto.strip().lower()
    return PRODUCT_ALIASES.get(key, key)


def list_aliases(produto: str | None = None) -> dict[str, str] | list[str]:
    if produto is None:
        return dict(PRODUCT_ALIASES)

    canonical = resolve_alias(produto)
    return [k for k, v in PRODUCT_ALIASES.items() if v == canonical]
