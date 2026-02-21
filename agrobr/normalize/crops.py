from __future__ import annotations

import unicodedata


def _remover_acentos(texto: str) -> str:
    nfkd = unicodedata.normalize("NFKD", texto)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


CULTURAS: dict[str, str] = {
    "soja": "soja",
    "soja em grao": "soja",
    "soja em grão": "soja",
    "soja_grao": "soja",
    "soja_grão": "soja",
    "soybean": "soja",
    "soybeans": "soja",
    "soy": "soja",
    "milho": "milho",
    "milho total": "milho",
    "milho_total": "milho",
    "corn": "milho",
    "maize": "milho",
    "milho 1a safra": "milho_1",
    "milho 1ª safra": "milho_1",
    "milho_1": "milho_1",
    "milho 1a": "milho_1",
    "milho 1ª": "milho_1",
    "milho 2a safra": "milho_2",
    "milho 2ª safra": "milho_2",
    "milho_2": "milho_2",
    "milho 2a": "milho_2",
    "milho 2ª": "milho_2",
    "milho 3a safra": "milho_3",
    "milho_3": "milho_3",
    "cafe": "cafe",
    "café": "cafe",
    "coffee": "cafe",
    "cafe arabica": "cafe_arabica",
    "café arábica": "cafe_arabica",
    "cafe_arabica": "cafe_arabica",
    "café_arábica": "cafe_arabica",
    "arabica": "cafe_arabica",
    "arábica": "cafe_arabica",
    "cafe robusta": "cafe_robusta",
    "café robusta": "cafe_robusta",
    "cafe_robusta": "cafe_robusta",
    "conilon": "cafe_robusta",
    "cafe conilon": "cafe_robusta",
    "café conilon": "cafe_robusta",
    "algodao": "algodao",
    "algodão": "algodao",
    "cotton": "algodao",
    "algodao herbaceo": "algodao",
    "algodão herbáceo": "algodao",
    "algodao_herbaceo": "algodao",
    "algodão_herbáceo": "algodao",
    "algodao em pluma": "algodao_pluma",
    "algodão em pluma": "algodao_pluma",
    "algodao_pluma": "algodao_pluma",
    "trigo": "trigo",
    "wheat": "trigo",
    "arroz": "arroz",
    "rice": "arroz",
    "arroz casca": "arroz",
    "arroz_casca": "arroz",
    "arroz em casca": "arroz",
    "feijao": "feijao",
    "feijão": "feijao",
    "bean": "feijao",
    "beans": "feijao",
    "feijao total": "feijao",
    "feijao_total": "feijao",
    "feijao 1a safra": "feijao_1",
    "feijão 1ª safra": "feijao_1",
    "feijao_1": "feijao_1",
    "feijao 2a safra": "feijao_2",
    "feijão 2ª safra": "feijao_2",
    "feijao_2": "feijao_2",
    "feijao 3a safra": "feijao_3",
    "feijão 3ª safra": "feijao_3",
    "feijao_3": "feijao_3",
    "boi": "boi",
    "boi gordo": "boi",
    "boi_gordo": "boi",
    "cattle": "boi",
    "beef": "boi",
    "acucar": "acucar",
    "açúcar": "acucar",
    "açucar": "acucar",
    "sugar": "acucar",
    "acucar cristal": "acucar_cristal",
    "açúcar cristal": "acucar_cristal",
    "acucar_cristal": "acucar_cristal",
    "acucar refinado": "acucar_refinado",
    "açúcar refinado": "acucar_refinado",
    "acucar_refinado": "acucar_refinado",
    "cana": "cana",
    "cana de acucar": "cana",
    "cana de açúcar": "cana",
    "cana_de_acucar": "cana",
    "cana_de_açúcar": "cana",
    "sugarcane": "cana",
    "etanol hidratado": "etanol_hidratado",
    "etanol_hidratado": "etanol_hidratado",
    "etanol": "etanol_hidratado",
    "ethanol": "etanol_hidratado",
    "etanol anidro": "etanol_anidro",
    "etanol_anidro": "etanol_anidro",
    "frango congelado": "frango_congelado",
    "frango_congelado": "frango_congelado",
    "frango": "frango_congelado",
    "chicken": "frango_congelado",
    "frango resfriado": "frango_resfriado",
    "frango_resfriado": "frango_resfriado",
    "suino": "suino",
    "suíno": "suino",
    "porco": "suino",
    "pork": "suino",
    "leite": "leite",
    "milk": "leite",
    "laranja": "laranja",
    "orange": "laranja",
    "laranja industria": "laranja_industria",
    "laranja_industria": "laranja_industria",
    "laranja in natura": "laranja_in_natura",
    "laranja_in_natura": "laranja_in_natura",
    "mandioca": "mandioca",
    "cassava": "mandioca",
    "farelo soja": "farelo_soja",
    "farelo_soja": "farelo_soja",
    "farelo de soja": "farelo_soja",
    "soybean meal": "farelo_soja",
    "oleo soja": "oleo_soja",
    "oleo_soja": "oleo_soja",
    "oleo de soja": "oleo_soja",
    "óleo de soja": "oleo_soja",
    "soybean oil": "oleo_soja",
    "sorgo": "sorgo",
    "sorghum": "sorgo",
    "aveia": "aveia",
    "oats": "aveia",
    "centeio": "centeio",
    "rye": "centeio",
    "cevada": "cevada",
    "barley": "cevada",
    "amendoim": "amendoim",
    "peanut": "amendoim",
    "batata": "batata",
    "potato": "batata",
    "tomate": "tomate",
    "tomato": "tomate",
    "cebola": "cebola",
    "onion": "cebola",
}

CANONICAL_CROPS: set[str] = set(CULTURAS.values())


def normalizar_cultura(nome: str) -> str:
    key = nome.strip().lower()
    if key in CULTURAS:
        return CULTURAS[key]

    key_sem_acento = _remover_acentos(key)
    if key_sem_acento in CULTURAS:
        return CULTURAS[key_sem_acento]

    for variante, canonico in CULTURAS.items():
        if _remover_acentos(variante) == key_sem_acento:
            return canonico

    return key.replace(" ", "_")


def listar_culturas() -> list[str]:
    return sorted(CANONICAL_CROPS)


def is_cultura_valida(nome: str) -> bool:
    return normalizar_cultura(nome) in CANONICAL_CROPS


__all__ = [
    "CANONICAL_CROPS",
    "CULTURAS",
    "is_cultura_valida",
    "listar_culturas",
    "normalizar_cultura",
]
