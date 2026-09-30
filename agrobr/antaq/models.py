from __future__ import annotations

from agrobr.exceptions import InvalidParameterError
from agrobr.normalize import regions

TIPO_NAVEGACAO = {
    "longo_curso": "Longo Curso",
    "cabotagem": "Cabotagem",
    "interior": "Interior",
    "apoio_maritimo": "Apoio Marítimo",
    "apoio_portuario": "Apoio Portuário",
}

NATUREZA_CARGA = {
    "granel_solido": "Granel Sólido",
    "granel_liquido": "Granel Líquido e Gasoso",
    "carga_geral": "Carga Geral",
    "conteiner": "Carga Conteinerizada",
}

SENTIDO = {"embarque": "Embarcados", "desembarque": "Desembarcados"}

COLUNAS_ATRACACAO = [
    "IDAtracacao",
    "Porto Atracação",
    "Complexo Portuário",
    "Tipo da Autoridade Portuária",
    "Data Atracação",
    "Data Desatracação",
    "Ano",
    "Mes",
    "Tipo de Navegação da Atracação",
    "Terminal",
    "Município",
    "UF",
    "SGUF",
    "Região Geográfica",
]

COLUNAS_CARGA = [
    "IDCarga",
    "IDAtracacao",
    "Origem",
    "Destino",
    "CDMercadoria",
    "Tipo Operação da Carga",
    "Tipo Navegação",
    "Natureza da Carga",
    "Sentido",
    "TEU",
    "QTCarga",
    "VLPesoCargaBruta",
]

COLUNAS_MERCADORIA = [
    "CDMercadoria",
    "Grupo de Mercadoria",
    "Mercadoria",
    "Nomenclatura Simplificada Mercadoria",
]

RENAME_FINAL: dict[str, str] = {
    "Ano": "ano",
    "Mes": "mes",
    "Data Atracação": "data_atracacao",
    "Porto Atracação": "porto",
    "Complexo Portuário": "complexo_portuario",
    "Terminal": "terminal",
    "Município": "municipio",
    "SGUF": "uf",
    "Região Geográfica": "regiao",
    "Tipo Navegação": "tipo_navegacao",
    "Natureza da Carga": "natureza_carga",
    "Sentido": "sentido",
    "Tipo Operação da Carga": "tipo_operacao",
    "CDMercadoria": "cd_mercadoria",
    "Nomenclatura Simplificada Mercadoria": "mercadoria",
    "Grupo de Mercadoria": "grupo_mercadoria",
    "Origem": "origem",
    "Destino": "destino",
    "VLPesoCargaBruta": "peso_bruto_ton",
    "QTCarga": "qt_carga",
    "TEU": "teu",
}

PARSER_VERSION = 2

MIN_ANO = 2010


def resolve_tipo_navegacao(valor: str | None) -> str | None:
    return _resolve_enum(valor, TIPO_NAVEGACAO, "tipo_navegacao")


def resolve_natureza_carga(valor: str | None) -> str | None:
    return _resolve_enum(valor, NATUREZA_CARGA, "natureza_carga")


def resolve_sentido(valor: str | None) -> str | None:
    return _resolve_enum(valor, SENTIDO, "sentido")


def _resolve_enum(valor: str | None, dominio: dict[str, str], nome: str) -> str | None:
    if valor is None:
        return None
    if isinstance(valor, str):
        key = regions.remover_acentos(valor).strip().lower().replace(" ", "_")
        for alias, rotulo in dominio.items():
            publicado = regions.remover_acentos(rotulo).lower().replace(" ", "_")
            if key in (alias, publicado):
                return rotulo
    raise InvalidParameterError(
        f"{nome} desconhecido: {valor!r}. Valores válidos: {list(dominio)} "
        f"ou os rótulos publicados {list(dominio.values())}"
    )
