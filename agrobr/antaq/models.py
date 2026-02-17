"""Constantes e mapeamentos para dados ANTAQ."""

from __future__ import annotations

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

SENTIDO = {
    "embarque": "Embarcados",
    "desembarque": "Desembarcados",
}

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

PARSER_VERSION = 1

MIN_ANO = 2010
MAX_ANO_DEFAULT = 2025


def resolve_tipo_navegacao(valor: str | None) -> str | None:
    """Resolve alias de tipo de navegação para valor do dataset."""
    if valor is None:
        return None
    key = valor.strip().lower().replace(" ", "_")
    if key in TIPO_NAVEGACAO:
        return TIPO_NAVEGACAO[key]
    for v in TIPO_NAVEGACAO.values():
        if v.lower() == valor.strip().lower():
            return v
    raise ValueError(
        f"Tipo de navegação desconhecido: {valor!r}. Valores válidos: {list(TIPO_NAVEGACAO.keys())}"
    )


def resolve_natureza_carga(valor: str | None) -> str | None:
    """Resolve alias de natureza da carga para valor do dataset."""
    if valor is None:
        return None
    key = valor.strip().lower().replace(" ", "_")
    if key in NATUREZA_CARGA:
        return NATUREZA_CARGA[key]
    for v in NATUREZA_CARGA.values():
        if v.lower() == valor.strip().lower():
            return v
    raise ValueError(
        f"Natureza da carga desconhecida: {valor!r}. Valores válidos: {list(NATUREZA_CARGA.keys())}"
    )
