from __future__ import annotations

from agrobr.exceptions import InvalidParameterError
from agrobr.utils import validation

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
    key = nome.strip().lower() if isinstance(nome, str) else ""
    if key in IMEA_CADEIAS:
        return IMEA_CADEIAS[key]
    if key.isdecimal() and int(key) in _CADEIA_NAMES:
        return int(key)
    raise InvalidParameterError(
        f"Cadeia desconhecida: {nome!r}. Opções: {list(dict.fromkeys(IMEA_CADEIAS.keys()))}"
    )


def validate_safra(safra: str | None) -> str | None:
    """Safra no formato que o IMEA publica (`AA/AA`), aceitando os do `validate_safra`."""
    normalizada = validation.validate_safra(safra)
    if normalizada is None:
        return None
    return f"{normalizada[2:4]}/{normalizada[-2:]}"


def cadeia_name(cadeia_id: int) -> str:
    return _CADEIA_NAMES.get(cadeia_id, str(cadeia_id))
