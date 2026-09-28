from __future__ import annotations

IMEA_CADEIAS: dict[str, int] = {
    "soja": 4,
    "soybeans": 4,
    "milho": 3,
    "corn": 3,
    "algodao": 1,
    "cotton": 1,
    "bovinocultura": 2,
    "boi": 2,
    "boi_gordo": 2,
    "bovinos": 2,
    "cattle": 2,
    "suinocultura": 7,
    "pork": 7,
    "leite": 8,
    "dairy": 8,
    "conjuntura": 5,
    "custo_producao": 10,
}

_CADEIA_NAMES: dict[int, str] = {
    1: "algodao",
    2: "bovinocultura",
    3: "milho",
    4: "soja",
    5: "conjuntura",
    7: "suinocultura",
    8: "leite",
    10: "custo_producao",
}

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
