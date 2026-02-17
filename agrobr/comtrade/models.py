from __future__ import annotations

COMTRADE_PAISES: dict[str, int] = {
    "br": 76,
    "bra": 76,
    "brasil": 76,
    "brazil": 76,
    "cn": 156,
    "chn": 156,
    "china": 156,
    "us": 842,
    "usa": 842,
    "eua": 842,
    "ar": 32,
    "arg": 32,
    "argentina": 32,
    "eu": 97,
    "ue": 97,
    "jp": 392,
    "jpn": 392,
    "japao": 392,
    "kr": 410,
    "kor": 410,
    "coreia": 410,
    "in": 356,
    "ind": 356,
    "india": 356,
    "mx": 484,
    "mex": 484,
    "mexico": 484,
    "cl": 152,
    "chl": 152,
    "chile": 152,
    "co": 170,
    "col": 170,
    "colombia": 170,
    "pe": 604,
    "per": 604,
    "peru": 604,
    "py": 600,
    "pry": 600,
    "paraguai": 600,
    "uy": 858,
    "ury": 858,
    "uruguai": 858,
    "world": 0,
    "mundo": 0,
    "eg": 818,
    "egy": 818,
    "egito": 818,
    "id": 360,
    "idn": 360,
    "indonesia": 360,
    "th": 764,
    "tha": 764,
    "tailandia": 764,
    "ir": 364,
    "irn": 364,
    "ira": 364,
    "sa": 682,
    "sau": 682,
    "arabia_saudita": 682,
    "tr": 792,
    "tur": 792,
    "turquia": 792,
    "ru": 643,
    "rus": 643,
    "russia": 643,
}

COMTRADE_PAISES_INV: dict[int, str] = {
    76: "BRA",
    156: "CHN",
    842: "USA",
    32: "ARG",
    97: "EU",
    392: "JPN",
    410: "KOR",
    356: "IND",
    484: "MEX",
    152: "CHL",
    170: "COL",
    604: "PER",
    600: "PRY",
    858: "URY",
    0: "WLD",
    818: "EGY",
    360: "IDN",
    764: "THA",
    364: "IRN",
    682: "SAU",
    792: "TUR",
    643: "RUS",
}

HS_PRODUTOS_AGRO: dict[str, list[str]] = {
    "soja": ["1201"],
    "complexo_soja": ["1201", "1507", "2304"],
    "farelo_soja": ["2304"],
    "oleo_soja": ["1507"],
    "milho": ["1005"],
    "arroz": ["1006"],
    "trigo": ["1001"],
    "cafe": ["0901"],
    "acucar": ["1701"],
    "etanol": ["2207"],
    "algodao": ["5201"],
    "carne_bovina": ["0201", "0202"],
    "carne_frango": ["0207"],
    "carne_suina": ["0203"],
    "celulose": ["4703"],
    "tabaco": ["2401"],
    "suco_laranja": ["2009"],
}

COLUNAS_SAIDA: list[str] = [
    "periodo",
    "ano",
    "mes",
    "reporter_code",
    "reporter_iso",
    "reporter",
    "partner_code",
    "partner_iso",
    "partner",
    "fluxo_code",
    "fluxo",
    "hs_code",
    "produto_desc",
    "nivel_hs",
    "peso_liquido_kg",
    "peso_bruto_kg",
    "volume_ton",
    "valor_fob_usd",
    "valor_cif_usd",
    "valor_primario_usd",
    "quantidade",
    "unidade_qtd",
]

COLUNAS_MIRROR: list[str] = [
    "periodo",
    "ano",
    "mes",
    "hs_code",
    "produto_desc",
    "reporter_iso",
    "partner_iso",
    "peso_liquido_kg_reporter",
    "valor_fob_usd_reporter",
    "volume_ton_reporter",
    "peso_liquido_kg_partner",
    "valor_fob_usd_partner",
    "valor_cif_usd_partner",
    "volume_ton_partner",
    "diff_peso_kg",
    "diff_valor_fob_usd",
    "ratio_valor",
    "ratio_peso",
]


def resolve_pais(nome: str) -> int:
    key = nome.strip().lower()
    if key in COMTRADE_PAISES:
        return COMTRADE_PAISES[key]
    if key.isdigit():
        return int(key)
    raise ValueError(
        f"Pais desconhecido: '{nome}'. Opcoes: {sorted(set(COMTRADE_PAISES_INV.values()))}"
    )


def resolve_hs(produto: str) -> list[str]:
    key = produto.strip().lower()
    if key in HS_PRODUTOS_AGRO:
        return HS_PRODUTOS_AGRO[key]
    if key.isdigit() and 2 <= len(key) <= 6:
        return [key]
    raise ValueError(f"Produto desconhecido: '{produto}'. Opcoes: {list(HS_PRODUTOS_AGRO.keys())}")
