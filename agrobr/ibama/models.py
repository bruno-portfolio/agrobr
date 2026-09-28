from agrobr.constants import URLS, Fonte

CSV_URL: str = URLS[Fonte.IBAMA]["termo_embargo_csv"]

MIN_CSV_BYTES = 10_000_000

CSV_COLUMN_MAP: dict[str, str] = {
    "SEQ_TAD": "seq_tad",
    "NUM_TAD": "numero_tad",
    "DAT_EMBARGO": "data_embargo",
    "NUM_PROCESSO": "num_processo",
    "DES_TAD": "descricao",
    "COD_MUNICIPIO": "codigo_municipio",
    "MUNICIPIO": "municipio",
    "UF": "uf",
    "NUM_LATITUDE_TAD": "latitude",
    "NUM_LONGITUDE_TAD": "longitude",
    "QTD_AREA_EMBARGADA": "area_embargada_ha",
    "NOME_IMOVEL": "nome_imovel",
    "DES_STATUS_FORMULARIO": "status",
    "SIT_CANCELADO": "cancelado",
    "DAT_DESEMBARGO": "data_desembargo",
}

GEOM_COLUMN_CSV = "GEOM_AREA_EMBARGADA"

EDICAO_COLUMN_CSV = "ULTIMA_ATUALIZACAO_RELATORIO"

COLUNAS_SAIDA: list[str] = list(CSV_COLUMN_MAP.values())

COLUNAS_SAIDA_GEO: list[str] = [*COLUNAS_SAIDA, "geometry"]
