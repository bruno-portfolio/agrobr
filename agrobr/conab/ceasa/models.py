from __future__ import annotations

import re

PENTAHO_BASE = "https://pentahoportaldeinformacoes.conab.gov.br/pentaho/plugin/cda/api/doQuery"

PENTAHO_AUTH = {"userid": "pentaho", "password": "password"}

CDA_PROHORT = "/home/PROHORT/precoDia.cda"

QUERY_PRECOS = "MDXProdutoPreco"
QUERY_PRODUTOS = "MDXproduto"
QUERY_CEASAS = "MDXceasa"

FRUTAS: list[str] = [
    "ABACATE",
    "ABACAXI",
    "BANANA NANICA",
    "BANANA PRATA",
    "GOIABA",
    "LARANJA PERA",
    "LIMAO TAHITI",
    "MACA",
    "MAMAO FORMOSA",
    "MAMAO HAWAY",
    "MANGA",
    "MARACUJA AZEDO",
    "MELANCIA",
    "MELAO AMARELO",
    "MORANGO",
    "PERA IMPORTADA",
    "TANGERINA",
    "UVA ITALIA",
    "UVA NIAGARA",
    "UVA RUBI",
]

HORTALICAS: list[str] = [
    "ABOBORA",
    "ABOBRINHA",
    "ALFACE",
    "ALHO",
    "BATATA",
    "BATATA DOCE",
    "BERINJELA",
    "BETERRABA",
    "BROCOLO",
    "CARA",
    "CEBOLA",
    "CENOURA",
    "CHUCHU",
    "COCO VERDE",
    "COUVE",
    "COUVE-FLOR",
    "INHAME",
    "JILO",
    "MANDIOCA",
    "MANDIOQUINHA",
    "MILHO VERDE",
    "OVOS",
    "PEPINO",
    "PIMENTAO VERDE",
    "QUIABO",
    "REPOLHO",
    "TOMATE",
    "VAGEM",
]

CATEGORIAS: dict[str, list[str]] = {
    "FRUTAS": FRUTAS,
    "HORTALICAS": HORTALICAS,
}

PRODUTO_PARA_CATEGORIA: dict[str, str] = {}
for _cat, _prods in CATEGORIAS.items():
    for _p in _prods:
        PRODUTO_PARA_CATEGORIA[_p] = _cat

PRODUTOS_PROHORT: set[str] = set(PRODUTO_PARA_CATEGORIA.keys())

UNIDADE_ESPECIAL: dict[str, str] = {
    "ABACAXI": "UN",
    "COCO VERDE": "UN",
    "COUVE-FLOR": "UN",
    "ALFACE": "DZ",
    "OVOS": "DZ",
}

COLUNAS_SAIDA: list[str] = [
    "data",
    "produto",
    "categoria",
    "unidade",
    "ceasa",
    "ceasa_uf",
    "preco",
]

CEASA_UF_MAP: dict[str, str] = {
    "AMA/BA - JUAZEIRO": "BA",
    "CEAGESP - ARACATUBA": "SP",
    "CEAGESP - ARARAQUARA": "SP",
    "CEAGESP - BAURU": "SP",
    "CEAGESP - FRANCA": "SP",
    "CEAGESP - MARILIA": "SP",
    "CEAGESP - PIRACICABA": "SP",
    "CEAGESP - PRES. PRUDENTE": "SP",
    "CEAGESP - RIBEIRAO PRETO": "SP",
    "CEAGESP - S J DOS CAMPOS": "SP",
    "CEAGESP - SAO JOSE RIO PRETO": "SP",
    "CEAGESP - SAO PAULO": "SP",
    "CEAGESP - SOROCABA": "SP",
    "CEASA/AL - MACEIO": "AL",
    "CEASA/BA - PAULO AFONSO": "BA",
    "CEASA/BA - SALVADOR": "BA",
    "CEASA/CE - FORTALEZA": "CE",
    "CEASA/CE - TIANGUA": "CE",
    "CEASA/DF - BRASILIA": "DF",
    "CEASA/ES - VITORIA": "ES",
    "CEASA/GO - GOIANIA": "GO",
    "CEASA/MA - SAO LUIZ": "MA",
    "CEASA/MS - CAMPO GRANDE": "MS",
    "CEASA/MT - CUIABA": "MT",
    "CEASA/PA - BELEM": "PA",
    "CEASA/PB - JOAO PESSOA": "PB",
    "CEASA/PB - PATOS": "PB",
    "CEASA/PE - CARUARU": "PE",
    "CEASA/PE - RECIFE": "PE",
    "CEASA/PR - CASCAVEL": "PR",
    "CEASA/PR - CURITIBA": "PR",
    "CEASA/PR - FOZ DO IGUACU": "PR",
    "CEASA/PR - MARINGA": "PR",
    "CEASA/RJ - RIO DE JANEIRO": "RJ",
    "CEASA/RN - NATAL": "RN",
    "CEASA/RS - CAXIAS DO SUL": "RS",
    "CEASA/RS - PORTO ALEGRE": "RS",
    "CEASA/SC - FLORIANOPOLIS": "SC",
    "CEASA/SP - CAMPINAS": "SP",
    "CEASA/TO - PALMAS": "TO",
    "CEASAMINAS - BARBACENA": "MG",
    "CEASAMINAS - BELO HORIZONTE": "MG",
    "CEASAMINAS - UBERABA": "MG",
}

_RE_PRODUTO_UNIDADE = re.compile(r"^(.+?)\s*\((\w+)\)$")
_RE_UF_SLASH = re.compile(r"/([A-Z]{2})\s*-")


def parse_produto_unidade(text: str) -> tuple[str, str]:
    m = _RE_PRODUTO_UNIDADE.match(text.strip())
    if m:
        return m.group(1).strip(), m.group(2).strip()
    return text.strip(), "KG"


def parse_ceasa_uf(name: str) -> str | None:
    if name in CEASA_UF_MAP:
        return CEASA_UF_MAP[name]
    m = _RE_UF_SLASH.search(name)
    return m.group(1) if m else None
