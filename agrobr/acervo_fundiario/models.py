from __future__ import annotations

from agrobr.constants import URLS, Fonte

BASE_URL: str = URLS[Fonte.ACERVO_FUNDIARIO]["download"]

DBF_ENCODING = "latin1"


FILENAME_PATTERNS: dict[str, str] = {
    "sigef_publico": "Sigef Público_{uf}.zip",
    "sigef_privado": "Sigef Privado_{uf}.zip",
    "snci": "Imóvel certificado SNCI Brasil_{uf}.zip",
    "snci_publico": "Imóvel certificado SNCI Público_{uf}.zip",
    "snci_privado": "Imóvel certificado SNCI Privado_{uf}.zip",
    "assentamentos": "Assentamento Brasil.zip",
}

SIGEF_NATUREZAS: tuple[str, ...] = ("publico", "privado")

SIGEF_SCHEMA_VERSION = "1.1"


SIGEF_RENAME_MAP: dict[str, str] = {
    "parcela_co": "codigo_parcela",
    "rt": "rt",
    "art": "art",
    "situacao_i": "situacao",
    "codigo_imo": "codigo_imovel",
    "data_submi": "data_submissao",
    "data_aprov": "data_aprovacao",
    "status": "status",
    "nome_area": "nome_area",
    "registro_m": "registro_matricula",
    "registro_d": "registro_data",
    "municipio_": "cod_municipio",
}

SIGEF_REQUIRED_COLS: frozenset[str] = frozenset({"parcela_co", "codigo_imo", "status", "uf_id"})

SIGEF_COLUNAS_SAIDA: list[str] = [
    "codigo_parcela",
    "rt",
    "art",
    "situacao",
    "codigo_imovel",
    "data_submissao",
    "data_aprovacao",
    "status",
    "nome_area",
    "registro_matricula",
    "registro_data",
    "cod_municipio",
    "uf",
    "natureza",
]

SIGEF_COLUNAS_SAIDA_GEO: list[str] = [*SIGEF_COLUNAS_SAIDA, "geometry"]

SIGEF_DATE_COLS: tuple[str, ...] = ("data_submissao", "data_aprovacao", "registro_data")


SNCI_RENAME_MAP: dict[str, str] = {
    "num_proces": "num_processo",
    "sr": "sr",
    "num_certif": "num_certificacao",
    "data_certi": "data_certificacao",
    "qtd_area_p": "area_peca_tecnica",
    "cod_profis": "cod_profissional",
    "cod_imovel": "cod_imovel_rural",
    "nome_imove": "nome_imovel",
    "uf_municip": "uf",
}

SNCI_REQUIRED_COLS: frozenset[str] = frozenset({"num_proces", "cod_imovel", "uf_municip"})

SNCI_COLUNAS_SAIDA: list[str] = [
    "num_processo",
    "sr",
    "num_certificacao",
    "data_certificacao",
    "area_peca_tecnica",
    "cod_profissional",
    "cod_imovel_rural",
    "nome_imovel",
    "uf",
]

SNCI_COLUNAS_SAIDA_GEO: list[str] = [*SNCI_COLUNAS_SAIDA, "geometry"]

SNCI_NATUREZA_SCHEMA_VERSION = "1.1"

SNCI_COLUNAS_SAIDA_NATUREZA: list[str] = [*SNCI_COLUNAS_SAIDA, "natureza"]

SNCI_COLUNAS_SAIDA_NATUREZA_GEO: list[str] = [*SNCI_COLUNAS_SAIDA_NATUREZA, "geometry"]

SNCI_DATE_COLS: tuple[str, ...] = ("data_certificacao",)

SNCI_NUMERIC_COLS: tuple[str, ...] = ("area_peca_tecnica",)


ASSENTAMENTOS_RENAME_MAP: dict[str, str] = {
    "cd_sipra": "codigo_sipra",
    "uf": "uf",
    "nome_proje": "nome_projeto",
    "municipio": "municipio",
    "area_hecta": "area_ha",
    "capacidade": "capacidade",
    "num_famili": "num_familias",
    "fase": "fase",
    "data_de_cr": "data_criacao",
    "forma_obte": "forma_obtencao",
    "data_obten": "data_obtencao",
    "area_calc_": "area_calc_ha",
    "sr": "sr",
    "descricao_": "descricao_fase",
}

ASSENTAMENTOS_REQUIRED_COLS: frozenset[str] = frozenset(
    {"cd_sipra", "uf", "nome_proje", "municipio"}
)

ASSENTAMENTOS_COLUNAS_SAIDA: list[str] = [
    "codigo_sipra",
    "nome_projeto",
    "municipio",
    "uf",
    "area_ha",
    "capacidade",
    "num_familias",
    "fase",
    "data_criacao",
    "forma_obtencao",
    "data_obtencao",
    "area_calc_ha",
    "sr",
    "descricao_fase",
]

ASSENTAMENTOS_COLUNAS_SAIDA_GEO: list[str] = [*ASSENTAMENTOS_COLUNAS_SAIDA, "geometry"]

ASSENTAMENTOS_DATE_COLS: tuple[str, ...] = ("data_criacao", "data_obtencao")

ASSENTAMENTOS_NUMERIC_COLS: tuple[str, ...] = (
    "area_ha",
    "area_calc_ha",
    "capacidade",
    "num_familias",
    "fase",
)
