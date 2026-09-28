from __future__ import annotations

_CLASSIFICACOES_CENSO_AGRO: dict[tuple[str, str], dict[str, str]] = {
    ("efetivo_rebanho", "1995"): {"224": "all", "220": "0"},
    ("uso_terra", "1995"): {"222": "all", "220": "0"},
    ("lavoura_temporaria", "1995"): {"226": "all", "220": "0"},
    ("lavoura_permanente", "1995"): {"227": "all", "220": "0"},
    ("efetivo_rebanho", "2017"): {
        "829": "46302",
        "12443": "all",
        "218": "46502",
    },
    ("uso_terra", "2017"): {
        "829": "46302",
        "222": "all",
        "218": "46502",
        "12517": "113601",
        "12567": "41151",
    },
    ("lavoura_temporaria", "2017"): {
        "829": "46302",
        "226": "all",
        "218": "46502",
        "12517": "113601",
    },
    ("lavoura_permanente", "2017"): {
        "829": "46302",
        "227": "all",
        "220": "110085",
    },
    ("preparo_solo", "2006"): {
        "12585": "all",
    },
    ("preparo_solo", "2017"): {
        "829": "46302",
        "12564": "41145",
        "12771": "45951",
        "218": "46502",
    },
    ("adubacao", "2006"): {
        "12586": "all",
    },
    ("adubacao", "2017"): {
        "12522": "all",
    },
    ("calagem", "2006"): {
        "12549": "all",
    },
    ("calagem", "2017"): {
        "12549": "all",
    },
    ("agrotoxicos", "2006"): {
        "12521": "all",
    },
    ("agrotoxicos", "2017"): {
        "12521": "all",
    },
    ("praticas_agricolas", "2006"): {
        "12568": "all",
    },
    ("praticas_agricolas", "2017"): {
        "12568": "all",
    },
    ("irrigacao", "2006"): {
        "12604": "all",
    },
    ("irrigacao", "2017"): {
        "12604": "all",
    },
    ("despesa_adubos", "2017"): {
        "829": "46302",
        "210": "45953",
        "218": "46502",
        "12517": "113601",
    },
}

_CENSO_VAR_NOME: dict[str, str] = {
    "105": "cabecas",
    "151": "informantes",
    "214": "producao",
    "216": "area_colhida",
    "10010": "estabelecimentos",
    "2209": "cabecas",
    "9587": "estabelecimentos",
    "184": "area",
    "183": "estabelecimentos",
    "10084": "estabelecimentos",
    "10085": "producao",
    "10089": "area_colhida",
    "9504": "estabelecimentos",
    "9506": "producao",
    "10078": "area_colhida",
    "2372": "estabelecimentos",
    "2373": "area",
}

_CENSO_VAR_UNIDADE: dict[str, str] = {
    "105": "cabeças",
    "151": "unidades",
    "214": "",
    "216": "hectares",
    "10010": "unidades",
    "2209": "cabeças",
    "9587": "unidades",
    "184": "hectares",
    "183": "unidades",
    "10084": "unidades",
    "10085": "",
    "10089": "hectares",
    "9504": "unidades",
    "9506": "",
    "10078": "hectares",
    "2372": "unidades",
    "2373": "hectares",
}

_CENSO_ALL_VAR_IDS: set[str] = set(_CENSO_VAR_NOME.keys())

_CENSO_CATEGORIA_COL_INDEX: dict[tuple[str, str], int] = {
    ("efetivo_rebanho", "1995"): 3,
    ("uso_terra", "1995"): 3,
    ("lavoura_temporaria", "1995"): 3,
    ("lavoura_permanente", "1995"): 3,
    ("efetivo_rebanho", "2017"): 5,
    ("uso_terra", "2017"): 5,
    ("lavoura_temporaria", "2017"): 5,
    ("lavoura_permanente", "2017"): 5,
    ("preparo_solo", "2006"): 3,
    ("adubacao", "2006"): 3,
    ("adubacao", "2017"): 3,
    ("calagem", "2006"): 3,
    ("calagem", "2017"): 3,
    ("agrotoxicos", "2006"): 3,
    ("agrotoxicos", "2017"): 3,
    ("praticas_agricolas", "2006"): 3,
    ("praticas_agricolas", "2017"): 3,
    ("irrigacao", "2006"): 3,
    ("irrigacao", "2017"): 3,
    ("despesa_adubos", "2017"): 3,
}

_VAR_AS_CATEGORIA: dict[tuple[str, str], dict[str, tuple[str, str, str]]] = {
    ("preparo_solo", "2017"): {
        "9562": ("Não utiliza preparo", "estabelecimentos", "unidades"),
        "9563": ("Utiliza preparo", "estabelecimentos", "unidades"),
        "9564": ("Cultivo convencional", "estabelecimentos", "unidades"),
        "9565": ("Cultivo mínimo", "estabelecimentos", "unidades"),
        "2016": ("Plantio direto na palha", "estabelecimentos", "unidades"),
        "2018": ("Plantio direto na palha", "area", "hectares"),
    },
    ("despesa_adubos", "2017"): {
        "2": ("Estabelecimentos com despesa", "estabelecimentos", "unidades"),
        "1996": ("Valor da despesa", "valor_mil_reais", "mil reais"),
    },
}

_CENSO_MULTI_TABLE: dict[tuple[str, str], list[tuple[str, dict[str, str]]]] = {
    ("uso_terra", "1995"): [
        ("316", {"area": "184"}),
        ("311", {"estabelecimentos": "183"}),
    ],
    ("lavoura_temporaria", "1995"): [
        ("497", {"producao": "214"}),
        ("492", {"informantes": "151"}),
        ("503", {"area_colhida": "216"}),
    ],
    ("lavoura_permanente", "1995"): [
        ("509", {"producao": "214"}),
        ("504", {"informantes": "151"}),
        ("510", {"area_colhida": "216"}),
    ],
}
