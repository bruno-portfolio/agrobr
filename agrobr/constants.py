from __future__ import annotations

import math
import os
import warnings
from datetime import date
from enum import StrEnum
from pathlib import Path
from typing import Any

from pydantic import (
    AliasChoices,
    Field,
    TypeAdapter,
    ValidationError,
    ValidationInfo,
    field_validator,
    model_validator,
)
from pydantic_settings import BaseSettings, SettingsConfigDict

from agrobr.exceptions import InvalidParameterError

CONAB_SOCIOBIODIVERSIDADE_PRODUTOS = (
    "acai",
    "andiroba",
    "babacu",
    "baru",
    "borracha",
    "buriti",
    "cacau",
    "carnauba",
    "castanha_do_brasil",
    "fava_danta",
    "jucara",
    "licuri",
    "macauba",
    "mangaba",
    "murumuru",
    "pequi",
    "piacava",
    "pirarucu",
    "pinhao",
    "umbu",
)

JSON_NUMBER_PATTERN = r"-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?"
JSON_COUNT_PATTERN = r"0|[1-9][0-9]*"

__all__ = [
    "CEPEA_PRODUTOS",
    "CONAB_PRODUTOS",
    "CONAB_REGIOES",
    "CONAB_UFS",
    "CacheSettings",
    "Fonte",
    "HTTPSettings",
    "NOTICIAS_AGRICOLAS_PRODUTOS",
    "URLS",
]


class Fonte(StrEnum):
    ABIOVE = "abiove"
    ACERVO_FUNDIARIO = "acervo_fundiario"
    ANA = "ana"
    ANDA = "anda"
    ANEC = "anec"
    ANP_DIESEL = "anp_diesel"
    ANTAQ = "antaq"
    ANTT_PEDAGIO = "antt_pedagio"
    B3 = "b3"
    BCB = "bcb"
    CEPEA = "cepea"
    CFTC = "cftc"
    CNUC = "cnuc"
    COMEXSTAT = "comexstat"
    COMTRADE = "comtrade"
    CONAB = "conab"
    DEFENSIVOS = "defensivos"
    DERAL = "deral"
    EMBRAPA_SOLOS = "embrapa_solos"
    FUNAI = "funai"
    IBAMA = "ibama"
    IBGE = "ibge"
    ICMBIO = "icmbio"
    IMEA = "imea"
    INCRA = "incra"
    INMET = "inmet"
    LISTA_SUJA = "lista_suja"
    MAPA_PSR = "mapa_psr"
    MAPBIOMAS_ALERTA = "mapbiomas_alerta"
    NASA_POWER = "nasa_power"
    NOTICIAS_AGRICOLAS = "noticias_agricolas"
    DESMATAMENTO = "desmatamento"
    MAPBIOMAS = "mapbiomas"
    QUEIMADAS = "queimadas"
    SFB = "sfb"
    SICAR = "sicar"
    UNICA = "unica"
    USDA = "usda"
    RNC = "rnc"
    RIO_VERDE = "rio_verde"
    ZARC = "zarc"


LICENCAS: dict[str, str] = {
    Fonte.ABIOVE: "zona_cinza",
    Fonte.ACERVO_FUNDIARIO: "livre",
    Fonte.ANA: "livre",
    Fonte.ANDA: "zona_cinza",
    Fonte.ANEC: "zona_cinza",
    Fonte.ANP_DIESEL: "livre",
    Fonte.ANTAQ: "livre",
    Fonte.ANTT_PEDAGIO: "livre",
    Fonte.B3: "zona_cinza",
    Fonte.BCB: "livre",
    Fonte.CEPEA: "nc",
    Fonte.CFTC: "livre",
    Fonte.CNUC: "livre",
    Fonte.COMEXSTAT: "livre",
    Fonte.COMTRADE: "restrito",
    Fonte.CONAB: "livre",
    Fonte.DEFENSIVOS: "livre",
    Fonte.DERAL: "livre",
    Fonte.DESMATAMENTO: "livre",
    Fonte.EMBRAPA_SOLOS: "nc",
    Fonte.FUNAI: "livre",
    Fonte.IBAMA: "livre",
    Fonte.IBGE: "livre",
    Fonte.ICMBIO: "livre",
    Fonte.IMEA: "zona_cinza",
    Fonte.INCRA: "livre",
    Fonte.INMET: "livre",
    Fonte.LISTA_SUJA: "livre",
    Fonte.MAPA_PSR: "livre",
    Fonte.MAPBIOMAS: "livre",
    Fonte.MAPBIOMAS_ALERTA: "livre",
    Fonte.NASA_POWER: "livre",
    Fonte.NOTICIAS_AGRICOLAS: "zona_cinza",
    Fonte.QUEIMADAS: "livre",
    Fonte.RIO_VERDE: "zona_cinza",
    Fonte.RNC: "livre",
    Fonte.SFB: "livre",
    Fonte.SICAR: "livre",
    Fonte.UNICA: "zona_cinza",
    Fonte.USDA: "livre",
    Fonte.ZARC: "livre",
    "conab_ceasa": "zona_cinza",
    "conab_prohort": "zona_cinza",
    "inpe": "livre",
}
LICENCAS_EM_ORDEM = ("livre", "zona_cinza", "nc", "restrito")


def licenca_da_fonte(nome: str) -> str | None:
    """Classificação da ``docs/licenses.md`` pela fonte ou pelo prefixo mais longo (``ibge_lspa`` → ``ibge``)."""
    candidatos = [chave for chave in LICENCAS if nome == chave or nome.startswith(f"{chave}_")]
    return LICENCAS[max(candidatos, key=len)] if candidatos else None


URLS = {
    Fonte.ABIOVE: {
        "base": "https://abiove.org.br",
        "estatisticas": "https://abiove.org.br/estatisticas",
        "exportacao": "https://abiove.org.br/abiove_content/Abiove",
    },
    Fonte.ACERVO_FUNDIARIO: {
        "base": "https://certificacao.incra.gov.br",
        "download": "https://certificacao.incra.gov.br/csv_shp/zip/",
    },
    Fonte.ANA: {
        "base": "https://portal1.snirh.gov.br",
        "arcgis": "https://portal1.snirh.gov.br/server/rest/services/dados_abertos",
        "arcgis_spr": "https://www.snirh.gov.br/arcgis/rest/services/SPR",
    },
    Fonte.ANDA: {
        "base": "https://anda.org.br",
        "estatisticas": "https://anda.org.br/recursos/",
    },
    Fonte.ANEC: {
        "base": "https://www.anec.com.br",
        "search": "https://www.anec.com.br/search",
        "article": "https://www.anec.com.br/article",
    },
    Fonte.ANP_DIESEL: {
        "precos_catalogo": "https://www.gov.br/anp/pt-br/assuntos/precos-e-defesa-da-concorrencia/precos/precos-revenda-e-de-distribuicao-combustiveis/serie-historica-do-levantamento-de-precos",
        "base": "https://www.gov.br/anp/pt-br",
        "shlp": "https://www.gov.br/anp/pt-br/assuntos/precos-e-defesa-da-concorrencia/precos/precos-revenda-e-de-distribuicao-combustiveis/shlp",
        "vendas_diesel_csv": "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/vdpb/vct/vendas-oleo-diesel-tipo-m3-2013-2025.csv",
    },
    Fonte.ANTAQ: {
        "base": "https://estatistica.antaq.gov.br/ea/sense/download.html",
        "bulk_txt": "https://estatistica.antaq.gov.br/ea/txt",
    },
    Fonte.ANTT_PEDAGIO: {
        "base": "https://dados.antt.gov.br",
        "trafego": "https://dados.antt.gov.br/dataset/volume-trafego-praca-pedagio",
        "pracas": "https://dados.antt.gov.br/dataset/praca-de-pedagio",
    },
    Fonte.BCB: {
        "base": "https://olinda.bcb.gov.br/olinda/servico/SICOR/versao/v2/odata",
        "ptax": "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata",
        "sgs": "https://api.bcb.gov.br/dados/serie/bcdata.sgs",
        "focus": "https://olinda.bcb.gov.br/olinda/servico/Expectativas/versao/v1/odata",
    },
    Fonte.CEPEA: {
        "base": "https://www.cepea.org.br",
        "indicadores": "https://www.cepea.org.br/br/indicador",
    },
    Fonte.CFTC: {
        "base": "https://publicreporting.cftc.gov",
        "disaggregated_futures": "https://publicreporting.cftc.gov/resource/72hh-3qpy.json",
        "disaggregated_combined": "https://publicreporting.cftc.gov/resource/kh3c-gbw2.json",
    },
    Fonte.CNUC: {
        "base": "https://cnuc.mma.gov.br",
        "mapserver": "https://cnuc-mapserv.mma.gov.br/cgi-bin/mapserv",
        "dados_abertos": "https://dados.mma.gov.br/dataset/unidadesdeconservacao",
        "ckan_package": "https://dados.mma.gov.br/api/3/action/package_show?id=unidadesdeconservacao",
        "cadastro_csv": "https://dados.mma.gov.br/dataset/44b6dc8a-dc82-4a84-8d95-1b0da7c85dac/resource/72dd3d2d-3cca-4b97-b382-a1c90531e379/download/cnuc_2026_07.csv",
    },
    Fonte.COMEXSTAT: {
        "base": "https://comexstat.mdic.gov.br",
        "bulk_csv": "https://balanca.mdic.gov.br/balanca/bd/comexstat-bd/ncm",
    },
    Fonte.COMTRADE: {
        "base": "https://comtradeapi.un.org",
        "auth": "https://comtradeapi.un.org/data/v1/get",
        "guest": "https://comtradeapi.un.org/public/v1/preview",
    },
    Fonte.CONAB: {
        "base": "https://www.gov.br/conab",
        "boletim_graos": "https://www.gov.br/conab/pt-br/atuacao/informacoes-agropecuarias/safras/safra-de-graos/boletim-da-safra-de-graos",
        "ceasa_prohort": "https://pentahoportaldeinformacoes.conab.gov.br/pentaho/plugin/cda/api/doQuery",
    },
    Fonte.DEFENSIVOS: {
        "base": "https://dados.agricultura.gov.br",
        "formulados": "https://dados.agricultura.gov.br/dataset/6c913699-e82e-4da3-a0a1-fb6c431e367f/resource/d30b30d7-e256-484e-9ab8-cd40974e1238/download/agrofitprodutosformulados.csv",
        "tecnicos": "https://dados.agricultura.gov.br/dataset/6c913699-e82e-4da3-a0a1-fb6c431e367f/resource/a200c70b-e025-4a9a-be1b-ec7275d7921f/download/agrofitprodutostecnicos.csv",
    },
    Fonte.DERAL: {
        "base": "https://www.agricultura.pr.gov.br/deral",
        "downloads": "https://www.agricultura.pr.gov.br/system/files/publico/Safras",
    },
    Fonte.EMBRAPA_SOLOS: {
        "base": "https://geoinfo.dados.embrapa.br",
        "geoserver": "https://geoinfo.dados.embrapa.br/geoserver/ows",
    },
    Fonte.IBGE: {
        "base": "https://sidra.ibge.gov.br",
        "api": "https://apisidra.ibge.gov.br",
        "agregados": "https://servicodados.ibge.gov.br/api/v3/agregados",
        "ftp_censo_agro_1996": "https://ftp.ibge.gov.br/Censo_Agropecuario/Censo_Agropecuario_1995_96",
        "wfs_malha_municipal": "https://geoservicos.ibge.gov.br/geoserverIBGE/wfs",
        "wfs_areas_urbanizadas": "https://geoservicos.ibge.gov.br/geoserverCGEO/wfs",
        "zip_malha_municipal": "https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_municipais/municipio_2025/Brasil/BR_Municipios_2025.zip",
        "zip_areas_urbanizadas": "https://geoftp.ibge.gov.br/organizacao_do_territorio/tipologias_do_territorio/areas_urbanizadas_do_brasil/2022/Shapefile/AreasUrbanizadas2022_Brasil.zip",
    },
    Fonte.LISTA_SUJA: {
        "base": "https://www.gov.br/trabalho-e-emprego",
        "page": "https://www.gov.br/trabalho-e-emprego/pt-br/assuntos/inspecao-do-trabalho/areas-de-atuacao/combate-ao-trabalho-escravo-e-analogo-ao-de-escravo",
        "download": "https://www.gov.br/trabalho-e-emprego/pt-br/assuntos/inspecao-do-trabalho/areas-de-atuacao/cadastro_de_empregadores.pdf",
    },
    Fonte.MAPA_PSR: {
        "base": "https://dados.agricultura.gov.br",
        "dataset": "https://dados.agricultura.gov.br/dataset/baefdc68-9bad-4204-83e8-f2888b79ab48",
    },
    Fonte.IMEA: {
        "base": "https://api1.imea.com.br/api",
        "cotacoes": "https://api1.imea.com.br/api/v2/mobile/cadeias",
    },
    Fonte.FUNAI: {
        "base": "https://geoserver.funai.gov.br",
        "geoserver": "https://geoserver.funai.gov.br/geoserver/Funai/ows",
    },
    Fonte.IBAMA: {
        "base": "https://www.ibama.gov.br",
        "termo_embargo_csv": "https://stibamadadosabertosprd.blob.core.windows.net/dados-abertos/dados/TERMOS_DE_EMBARGO/TERMO_EMBARGO/termo_de_embargo.csv",
        "dados_abertos": "https://dadosabertos.ibama.gov.br",
    },
    Fonte.ICMBIO: {
        "base": "https://geoservicos.inde.gov.br",
        "geoserver": "https://geoservicos.inde.gov.br/geoserver/ICMBio/ows",
    },
    Fonte.INCRA: {
        "base": "https://cmr.funai.gov.br",
        "geoserver": "https://cmr.funai.gov.br/geoserver/ows",
    },
    Fonte.INMET: {
        "base": "https://apitempo.inmet.gov.br",
        "estacoes": "https://apitempo.inmet.gov.br/estacoes",
        "dados": "https://apitempo.inmet.gov.br/estacao",
        "dadoshistoricos": "https://portal.inmet.gov.br/uploads/dadoshistoricos",
    },
    Fonte.NASA_POWER: {
        "base": "https://power.larc.nasa.gov",
        "daily": "https://power.larc.nasa.gov/api/temporal/daily/point",
    },
    Fonte.UNICA: {
        "base": "https://unicadata.com.br",
        "quinzenal_page": "https://unicadata.com.br/listagem.php?idMn=63",
        "historico_xls": "https://unicadata.com.br/xlsHPM.php",
    },
    Fonte.USDA: {
        "base": "https://api.fas.usda.gov/api/psd",
    },
    Fonte.NOTICIAS_AGRICOLAS: {
        "base": "https://www.noticiasagricolas.com.br",
        "cotacoes": "https://www.noticiasagricolas.com.br/cotacoes",
    },
    Fonte.DESMATAMENTO: {
        "base": "https://terrabrasilis.dpi.inpe.br",
        "geoserver": "https://terrabrasilis.dpi.inpe.br/geoserver",
    },
    Fonte.MAPBIOMAS: {
        "base": "https://brasil.mapbiomas.org",
        "dataverse": "https://data.mapbiomas.org/api/access/datafile",
        "biome_state_file_id": "457",
        "biome_state_municipality_file_id": "254",
        "biome_state_collection_11": "https://brasil.mapbiomas.org/wp-content/uploads/sites/3/2026/08/MAPBIOMAS_BRAZIL-COL.11-BIOME_STATE.xlsx",
        "biome_state_municipality_collection_11": "https://drive.google.com/uc?export=download&id=1otOqymHuixvkRGVl65zTTNyfaHo46Gqk",
    },
    Fonte.MAPBIOMAS_ALERTA: {
        "base": "https://plataforma.alerta.mapbiomas.org",
        "graphql": "https://plataforma.alerta.mapbiomas.org/api/v2/graphql",
    },
    Fonte.QUEIMADAS: {
        "base": "https://terrabrasilis.dpi.inpe.br/queimadas/portal/",
        "dados_abertos": "https://dataserver-coids.inpe.br/queimadas/queimadas/focos/csv",
    },
    Fonte.SFB: {
        "base": "https://mapas.florestal.gov.br",
        "arcgis": "https://mapas.florestal.gov.br/server/rest/services",
    },
    Fonte.SICAR: {
        "base": "https://www.car.gov.br",
        "geoserver": "https://geoserver.car.gov.br/geoserver/sicar/wfs",
    },
    Fonte.B3: {
        "base": "https://www.b3.com.br",
        "ajustes_zip": "https://www.b3.com.br/pesquisapregao/download",
        "arquivos": "https://arquivos.b3.com.br/api/download",
    },
    Fonte.RNC: {
        "base": "https://sistemas.agricultura.gov.br/snpc",
        "cultivarweb": "https://sistemas.agricultura.gov.br/snpc/cultivarweb",
    },
    Fonte.RIO_VERDE: {
        "base": "https://fundacaorioverde.com.br",
        "publicacoes": "https://fundacaorioverde.com.br/publicacoes/",
    },
    Fonte.ZARC: {
        "base": "https://dados.agricultura.gov.br",
        "ckan_api": "https://dados.agricultura.gov.br/api/3/action",
    },
}

CEPEA_PARSER_VERSION = 2
NOTICIAS_AGRICOLAS_PARSER_VERSION = 3

NOTICIAS_AGRICOLAS_PRODUTOS = {
    "soja": "soja/soja-indicador-cepea-esalq-porto-paranagua",
    "soja_parana": "soja/indicador-cepea-esalq-soja-parana",
    "milho": "milho/indicador-cepea-esalq-milho",
    "bezerro": "boi-gordo/indicador-bezerro-esalq-bmf-bovespa-ms",
    "boi": "boi-gordo/boi-gordo-indicador-esalq-bmf",
    "boi_gordo": "boi-gordo/boi-gordo-indicador-esalq-bmf",
    "cafe": "cafe/indicador-cepea-esalq-cafe-arabica",
    "cafe_arabica": "cafe/indicador-cepea-esalq-cafe-arabica",
    "cafe_robusta": "cafe/indicador-cepea-esalq-cafe-conillon",
    "algodao": "algodao/algodao-indicador-cepea-esalq-a-prazo",
    "trigo": "trigo/preco-medio-do-trigo-cepea-esalq",
    "arroz": "arroz/arroz-em-casca-esalq-bbm",
    "acucar": "sucroenergetico/acucar-cristal-cepea",
    "acucar_refinado": "sucroenergetico/acucar-refinado-amorfo",
    "etanol_hidratado": "sucroenergetico/indicador-semanal-etanol-hidratado-cepea-esalq",
    "etanol_anidro": "sucroenergetico/indicador-semanal-etanol-anidro-cepea-esalq",
    "frango_congelado": "frango/precos-do-frango-congelado-cepea-esalq",
    "frango_resfriado": "frango/precos-do-frango-resfriado-cepea-esalq",
    "suino": "suinos/indicador-do-suino-vivo-cepea-esalq",
    "leite": "leite/leite-precos-ao-produtor-cepea-rs-litro",
    "laranja_industria": "laranja/laranja-industria",
    "laranja_in_natura": "laranja/laranja-pera-in-natura",
}

CEPEA_PRODUTOS = {
    "soja": "soja",
    "soja_parana": "soja",
    "milho": "milho",
    "bezerro": "bezerro",
    "cafe": "cafe",
    "cafe_arabica": "cafe",
    "cafe_robusta": "cafe",
    "boi": "boi-gordo",
    "boi_gordo": "boi-gordo",
    "trigo": "trigo",
    "algodao": "algodao",
    "arroz": "arroz",
    "acucar": "acucar",
    "acucar_refinado": "acucar-refinado-amorfo-sp",
    "frango_congelado": "frango",
    "frango_resfriado": "frango",
    "suino": "suino",
    "etanol_hidratado": "etanol",
    "etanol_anidro": "etanol",
    "leite": "leite",
    "laranja_industria": "citros",
    "laranja_in_natura": "citros",
}

CEPEA_TITULOS = {
    "soja": r"SOJA.*PARANAGUÁ",
    "soja_parana": r"SOJA.*PARANÁ$",
    "milho": r"INDICADOR DO MILHO",
    "bezerro": r"INDICADOR DO BEZERRO",
    "cafe": r"CAFÉ ARÁBICA",
    "cafe_arabica": r"CAFÉ ARÁBICA",
    "cafe_robusta": r"CAFÉ ROBUSTA",
    "boi": r"^INDICADOR DO BOI GORDO",
    "boi_gordo": r"^INDICADOR DO BOI GORDO",
    "trigo": r"TRIGO.*PARANÁ",
    "algodao": r"ALGODÃO EM PLUMA.*8 DIAS",
    "arroz": r"58%",
    "acucar": r"CRISTAL BRANCO",
    "acucar_refinado": r"AÇÚCAR REFINADO AMORFO",
    "frango_congelado": r"FRANGO CONGELADO",
    "frango_resfriado": r"FRANGO RESFRIADO",
    "suino": r"^INDICADOR DO SUÍNO VIVO(?!.*MENSAL)",
    "etanol_hidratado": r"ETANOL HIDRATADO COMBUSTÍVEL",
    "etanol_anidro": r"ETANOL ANIDRO",
    "leite": r"LEITE AO PRODUTOR",
    "laranja_industria": r"^LARANJA INDÚSTRIA$",
    "laranja_in_natura": r"^LARANJA PERA IN NATURA$",
}

CEPEA_SERIE_PARSER_VERSION = 101
CEPEA_SERIE_MAX_BYTES = 2 * 1024**2
CEPEA_SERIE_TITULO_PESO = r"PESO MÉDIO DO BEZERRO"
CEPEA_SERIES: dict[str, tuple[tuple[str, str, str], ...]] = {
    "soja": (("soja", "92", ""),),
    "soja_parana": (("soja", "12", ""),),
    "milho": (("milho", "77", ""),),
    "bezerro": (("bezerro", "8", ""), ("bezerro", "174", "peso")),
    "cafe": (("cafe", "23", ""),),
    "cafe_arabica": (("cafe", "23", ""),),
    "cafe_robusta": (("cafe", "24", ""),),
    "boi": (("boi-gordo", "2", ""),),
    "boi_gordo": (("boi-gordo", "2", ""),),
    "trigo": (("trigo", "178", "Paraná"), ("trigo", "179", "Rio Grande do Sul")),
    "algodao": (("algodao", "54", ""),),
    "arroz": (("arroz", "91", ""),),
    "acucar": (("acucar", "53", ""),),
    "acucar_refinado": (("acucar-refinado-amorfo-sp", "114", ""),),
    "frango_congelado": (("frango", "181", ""),),
    "frango_resfriado": (("frango", "130", ""),),
    "suino": (("suino", "129", ""),),
    "etanol_hidratado": (("etanol", "103", ""),),
    "etanol_anidro": (("etanol", "104", ""),),
    "leite": (("leite", "leitep", ""),),
}

CEPEA_SERIE_CANONICA: dict[str, str] = {
    produto: next(nome for nome, outra in CEPEA_SERIES.items() if outra == serie)
    for produto, serie in CEPEA_SERIES.items()
}

CEPEA_VALOR_MANTIDO: dict[str, tuple[str, date]] = {"soja": ("Paranaguá/PR", date(2015, 5, 4))}

CEPEA_TABELAS_POR_PRACA = {
    "trigo": {"Paraná": r"TRIGO.*PARANÁ", "Rio Grande do Sul": r"TRIGO.*RIO GRANDE DO SUL"},
}

CEPEA_PRACAS_REGIONAIS = {
    "suino": ("MG - posto", "PR - a retirar", "RS - a retirar", "SC - a retirar", "SP - posto"),
    "leite": ("RS", "SC", "PR", "SP", "MG", "GO", "BA", "BRASIL", "ES", "RJ"),
    **{produto: tuple(tabelas) for produto, tabelas in CEPEA_TABELAS_POR_PRACA.items()},
}

CONAB_PRODUTOS = {
    "soja": "Soja",
    "milho": "Milho Total",
    "milho_1": "Milho 1a",
    "milho_2": "Milho 2a",
    "milho_3": "Milho 3a",
    "arroz": "Arroz Total",
    "arroz_irrigado": "Arroz Irrigado",
    "arroz_sequeiro": "Arroz Sequeiro",
    "feijao": "Feijão Total",
    "feijao_1": "Feijão 1a Total",
    "feijao_2": "Feijão 2a Total",
    "feijao_3": "Feijão 3a Total",
    "algodao": "Algodao Total",
    "algodao_pluma": "Algodao em Pluma",
    "trigo": "Trigo",
    "sorgo": "Sorgo",
    "aveia": "Aveia",
    "cevada": "Cevada",
    "canola": "Canola",
    "girassol": "Girassol",
    "mamona": "Mamona",
    "amendoim": "Amendoim Total",
    "centeio": "Centeio",
    "triticale": "Triticale",
    "gergelim": "Gergelim",
}

CONAB_SAFRA_METRICS = {
    "area": ("area", "mil ha"),
    "produtividade": ("produtividade", "kg/ha"),
    "producao": ("producao", "mil t"),
}

CONAB_INVERNO = "CULTURAS DE INVERNO"

CONAB_BRASIL_TOTAL_SERIES: tuple[tuple[str, str | None, str, str | None], ...] = (
    ("ALGODÃO - CAROÇO (1)", None, "algodao_caroco", "verao"),
    ("ALGODÃO - PLUMA", None, "algodao_pluma", None),
    ("AMENDOIM TOTAL", None, "amendoim", "verao"),
    ("Amendoim 1ª Safra", "AMENDOIM TOTAL", "amendoim_1", None),
    ("Amendoim 2ª Safra", "AMENDOIM TOTAL", "amendoim_2", None),
    ("ARROZ", None, "arroz", "verao"),
    ("Arroz sequeiro", "ARROZ", "arroz_sequeiro", None),
    ("Arroz irrigado", "ARROZ", "arroz_irrigado", None),
    ("FEIJÃO TOTAL", None, "feijao", "verao"),
    *(
        linha
        for safra in ("1", "2", "3")
        for linha in (
            (f"FEIJÃO {safra}ª SAFRA", None, f"feijao_{safra}", None),
            ("Cores", f"FEIJÃO {safra}ª SAFRA", f"feijao_cores_{safra}", None),
            ("Preto", f"FEIJÃO {safra}ª SAFRA", f"feijao_preto_{safra}", None),
            ("Caupi", f"FEIJÃO {safra}ª SAFRA", f"feijao_caupi_{safra}", None),
        )
    ),
    ("GERGELIM", None, "gergelim", "verao"),
    ("GIRASSOL", None, "girassol", "verao"),
    ("MAMONA", None, "mamona", "verao"),
    ("MILHO TOTAL", None, "milho", "verao"),
    ("Milho 1ª Safra", "MILHO TOTAL", "milho_1", None),
    ("Milho 2ª Safra", "MILHO TOTAL", "milho_2", None),
    ("Milho 3ª Safra", "MILHO TOTAL", "milho_3", None),
    ("SOJA", None, "soja", "verao"),
    ("SORGO", None, "sorgo", "verao"),
    ("AVEIA", CONAB_INVERNO, "aveia", "inverno"),
    ("CANOLA", CONAB_INVERNO, "canola", "inverno"),
    ("CENTEIO", CONAB_INVERNO, "centeio", "inverno"),
    ("CEVADA", CONAB_INVERNO, "cevada", "inverno"),
    ("TRIGO", CONAB_INVERNO, "trigo", "inverno"),
    ("TRITICALE", CONAB_INVERNO, "triticale", "inverno"),
)

CONAB_BRASIL_TOTAL_PARTES: tuple[
    tuple[tuple[str, str | None], tuple[tuple[str, str | None], ...]], ...
] = (
    (("FEIJÃO TOTAL", None), tuple((f"FEIJÃO {n}ª SAFRA", None) for n in "123")),
    *(
        (
            (f"FEIJÃO {n}ª SAFRA", None),
            tuple((tipo, f"FEIJÃO {n}ª SAFRA") for tipo in ("Cores", "Preto", "Caupi")),
        )
        for n in "123"
    ),
    (("AMENDOIM TOTAL", None), tuple((f"Amendoim {n}ª Safra", "AMENDOIM TOTAL") for n in "12")),
    (("ARROZ", None), (("Arroz sequeiro", "ARROZ"), ("Arroz irrigado", "ARROZ"))),
    (("MILHO TOTAL", None), tuple((f"Milho {n}ª Safra", "MILHO TOTAL") for n in "123")),
)

CONAB_BALANCO_PRODUTOS = ("soja", "milho", "arroz", "feijao", "trigo", "algodao")

CONAB_BALANCO_DTYPES = {
    "estoque_inicial": "float64",
    "producao": "float64",
    "importacao": "float64",
    "suprimento": "float64",
    "consumo": "float64",
    "exportacao": "float64",
    "demanda_total": "float64",
    "estoque_final": "float64",
}

CONAB_BALANCO_IDENTIDADES: tuple[tuple[str, tuple[str, ...], tuple[str, ...]], ...] = (
    (
        "estoque inicial + produção + importação − suprimento",
        ("estoque_inicial", "producao", "importacao"),
        ("suprimento_total",),
    ),
    ("consumo + exportação − demanda total", ("consumo", "exportacao"), ("demanda_total",)),
    (
        "suprimento − consumo − exportação − estoque final",
        ("suprimento_total",),
        ("consumo", "exportacao", "estoque_final"),
    ),
)
CONAB_ARREDONDAMENTO = 0.05

CONAB_SOJA_COMPONENTES = {
    "estoque inicial": "estoque_inicial",
    "producao": "producao",
    "importacao": "importacao",
    "exportacao": "exportacao",
    "estoque final": "estoque_final",
    "sementes/outros": "sementes_outros",
    "processamento": "processamento",
}

CONAB_SUPRIMENTO_HEADERS = {
    "estoque inicial": "estoque_inicial",
    "producao": "producao",
    "importacao": "importacao",
    "suprimento": "suprimento_total",
    "consumo": "consumo",
    "exportacao": "exportacao",
    "demanda total": "demanda_total",
    "estoque final": "estoque_final",
}

CONAB_UFS = [
    "AC",
    "AL",
    "AM",
    "AP",
    "BA",
    "CE",
    "DF",
    "ES",
    "GO",
    "MA",
    "MG",
    "MS",
    "MT",
    "PA",
    "PB",
    "PE",
    "PI",
    "PR",
    "RJ",
    "RN",
    "RO",
    "RR",
    "RS",
    "SC",
    "SE",
    "SP",
    "TO",
]

CONAB_REGIOES = ["NORTE", "NORDESTE", "CENTRO-OESTE", "SUDESTE", "SUL"]


_CACHE_DIR_PADRAO = Path.home() / ".agrobr" / "cache"
_BOOLEANO = TypeAdapter(bool)


def env_flag(nome: str) -> bool:
    """Lê a variável de ambiente ``nome`` como booleano, na hora da chamada.

    Aceita os valores do pydantic, sem caixa: ``1``/``true``/``yes``/``on`` ligam e ``0``/``false``/``no``/``off``
    desligam. Ausente ou vazia é ``False``; outro valor levanta ``InvalidParameterError``.
    """
    valor = os.environ.get(nome, "").strip()
    if not valor:
        return False
    try:
        return _BOOLEANO.validate_python(valor)
    except ValidationError:
        raise InvalidParameterError(
            f"{nome}={valor!r} não é booleano. Valores válidos: 1, true, yes, on (liga); "
            "0, false, no, off (desliga)"
        ) from None


_NOMES_DA_PASTA_DO_CACHE = ("AGROBR_CACHE_DIR", "AGROBR_CACHE_CACHE_DIR")


class CacheSettings(BaseSettings):
    """``AGROBR_CACHE_DIR`` é o nome da pasta; ``AGROBR_CACHE_CACHE_DIR`` segue como alias e perde para ele.

    Vazia, a variável vale o padrão (``~/.agrobr/cache``), e não a pasta corrente. O argumento
    ``cache_dir`` vence as 2 variáveis.
    """

    cache_dir: Path = Field(
        default=_CACHE_DIR_PADRAO,
        validation_alias=AliasChoices(*_NOMES_DA_PASTA_DO_CACHE),
    )
    db_name: str = "agrobr.duckdb"

    model_config = SettingsConfigDict(env_prefix="AGROBR_CACHE_", populate_by_name=True)

    @model_validator(mode="before")
    @classmethod
    def _argumento_vence_o_ambiente(cls, dados: Any) -> Any:
        """Descarta as chaves do alias quando o argumento ``cache_dir`` veio.

        Até a 2.11, o pydantic-settings entrega o argumento pelo nome do campo junto das chaves do
        alias lidas do ambiente, e o par daria ``extra_forbidden``. A partir da 2.12, o argumento já
        chega sozinho, e nada muda.
        """
        if isinstance(dados, dict) and "cache_dir" in dados:
            return {
                chave: valor
                for chave, valor in dados.items()
                if str(chave).upper() not in _NOMES_DA_PASTA_DO_CACHE
            }
        return dados

    @field_validator("cache_dir", mode="before")
    @classmethod
    def _vazio_vale_o_padrao(cls, valor: object) -> object:
        return _CACHE_DIR_PADRAO if isinstance(valor, str) and not valor.strip() else valor

    def __init__(self, **dados: Any) -> None:
        super().__init__(**dados)
        self._avisar_pastas_divergentes(argumento="cache_dir" in dados)

    def _avisar_pastas_divergentes(self, *, argumento: bool) -> None:
        nova = os.environ.get("AGROBR_CACHE_DIR", "").strip()
        antiga = os.environ.get("AGROBR_CACHE_CACHE_DIR", "").strip()
        if not (nova and antiga and Path(nova) != Path(antiga)):
            return
        vencedor = (
            f"vale o argumento cache_dir ({self.cache_dir}), e as duas ficam sem efeito"
            if argumento
            else f"vale AGROBR_CACHE_DIR ({self.cache_dir})"
        )
        warnings.warn(
            f"agrobr: AGROBR_CACHE_DIR ({nova}) e AGROBR_CACHE_CACHE_DIR ({antiga}) apontam para "
            f"pastas diferentes; {vencedor}. Deixe só uma das duas.",
            UserWarning,
            stacklevel=3,
        )


class HTTPSettings(BaseSettings):
    """``max_retries`` é o total de tentativas por pedido; ``0`` vale como ``1`` (uma tentativa, sem retry).

    Os ``max_concurrent_*`` recusam valor menor que 1 na validação.
    """

    timeout_connect: float = 10.0
    timeout_read: float = 30.0
    timeout_write: float = 10.0
    timeout_pool: float = 10.0

    max_retries: int = Field(default=3, ge=0)
    retry_base_delay: float = 1.0
    retry_max_delay: float = 30.0
    retry_exponential_base: int = 2

    rate_limit_abiove: float = 3.0
    rate_limit_acervo_fundiario: float = 3.0
    rate_limit_ana: float = 2.0
    rate_limit_anda: float = 3.0
    rate_limit_anec: float = 3.0
    rate_limit_anp_diesel: float = 2.0
    rate_limit_antaq: float = 1.0
    rate_limit_antt_pedagio: float = 2.0
    rate_limit_bcb: float = 1.0
    rate_limit_cepea: float = 5.0
    rate_limit_cftc: float = 2.0
    rate_limit_cnuc: float = 2.0
    rate_limit_comexstat: float = 2.0
    rate_limit_comtrade: float = 2.0
    rate_limit_conab: float = 3.0
    rate_limit_defensivos: float = 2.0
    rate_limit_deral: float = 3.0
    rate_limit_ibge: float = 1.0
    rate_limit_imea: float = 1.0
    rate_limit_lista_suja: float = 2.0
    rate_limit_funai: float = 2.0
    rate_limit_ibama: float = 2.0
    rate_limit_icmbio: float = 2.0
    rate_limit_incra: float = 2.0
    rate_limit_inmet: float = 0.5
    rate_limit_nasa_power: float = 1.0
    rate_limit_noticias_agricolas: float = 2.0
    rate_limit_desmatamento: float = 2.0
    rate_limit_mapbiomas: float = 2.0
    rate_limit_mapbiomas_alerta: float = 3.0
    rate_limit_queimadas: float = 1.0
    rate_limit_sfb: float = 2.0
    rate_limit_sicar: float = 2.0
    rate_limit_unica: float = 3.0
    rate_limit_usda: float = 1.0
    rate_limit_b3: float = 1.0
    rate_limit_b3_arquivos: float = 5.0
    rate_limit_embrapa_solos: float = 2.0
    rate_limit_rnc: float = 3.0
    rate_limit_rio_verde: float = 3.0
    rate_limit_zarc: float = 2.0
    rate_limit_conab_ceasa: float = 2.0
    rate_limit_default: float = 1.0

    timeout_download_comexstat: float = Field(
        default_factory=lambda: float(COMEXSTAT_DOWNLOAD_TIMEOUT_SECONDS), gt=0, allow_inf_nan=False
    )

    max_concurrent_default: int = Field(default=1, ge=1)
    max_concurrent_ana: int = Field(default=1, ge=1)
    max_concurrent_anp_diesel: int = Field(default=3, ge=1)
    max_concurrent_b3: int = Field(default=3, ge=1)
    max_concurrent_ibge: int = Field(default=3, ge=1)

    model_config = SettingsConfigDict(env_prefix="AGROBR_HTTP_")

    @field_validator("max_retries")
    @classmethod
    def _zero_vale_uma_tentativa(cls, valor: int) -> int:
        return max(valor, 1)

    @field_validator("*")
    @classmethod
    def _intervalo_finito_nao_negativo(cls, valor: Any, info: ValidationInfo) -> Any:
        if str(info.field_name).startswith("rate_limit_") and not (
            math.isfinite(valor) and valor >= 0
        ):
            raise ValueError("intervalo entre pedidos deve ser número finito maior ou igual a 0")
        return valor


class AlertSettings(BaseSettings):
    enabled: bool = True

    slack_webhook: str | None = None
    discord_webhook: str | None = None

    sendgrid_api_key: str | None = None
    email_from: str = "alerts@agrobr.dev"
    email_to: list[str] = []

    alert_on_parse_error: bool = True
    alert_on_layout_change: bool = True
    alert_on_source_down: bool = True
    alert_on_anomaly: bool = True
    alert_on_soft_block: bool = True

    consecutive_failures_warning: int = 2
    consecutive_failures_critical: int = 3
    alert_on_recovery: bool = True
    discord_embed_char_limit: int = 3900

    model_config = SettingsConfigDict(env_prefix="AGROBR_ALERT_")


ALERT_WEBHOOK_ATTEMPTS = 3
ALERT_RETRY_DELAY_SECONDS = 1.0
ALERT_RETRY_AFTER_MAX_SECONDS = 10.0


_CEPEA_ENDPOINTS: tuple[str, ...] = (
    "https://www.cepea.org.br",
    "https://cepea.org.br",
)

CONFIDENCE_HIGH: float = 0.85
CONFIDENCE_LOW: float = 0.50

RETRIABLE_STATUS_CODES: set[int] = {408, 429, 500, 502, 503, 504}

MIN_WFS_SIZE: int = 50
MIN_CSV_SIZE: int = 100
MIN_HTML_SIZE: int = 500
MIN_ZIP_SIZE: int = 500
ATOMIC_REPLACE_ATTEMPTS: int = 5
ATOMIC_REPLACE_RETRY_DELAY: float = 0.01
ACERVO_MAX_DOWNLOAD_BYTES: int = 4 * 1024**3
MAPA_PSR_MAX_DOWNLOAD_BYTES: int = 2 * 1024**3
MAPA_PSR_MAX_TRANSFER_BYTES: int = 4 * 1024**3
MAPA_PSR_MES_ANO_COMPLETO: int = 10
BRUTO_MAX_BYTES_RECURSO: int = 4 * 1024**3
BRUTO_MAX_BYTES_PAGINA: int = 8 * 1024**2
BRUTO_MAX_PAGINAS: int = 10_000
BRUTO_MAX_IDS: int = 500_000
BRUTO_MAX_BYTES_IDS: int = 64 * 1024**2
BRUTO_MAX_SEGUNDOS: float = 3600.0
BRUTO_MAX_BYTES_MANIFESTO: int = 64 * 1024**2
BRUTO_TETO_ARQUIVO_BYTES: int = 4 * 1024**3
BRUTO_TAMANHO_PAGINA_PADRAO: int = 100
BRUTO_TAMANHO_PAGINA_MAX: int = 1_000
MIN_XLSX_SIZE: int = 1_000
MIN_PDF_SIZE: int = 10_000
MIN_HTML_PAGE_SIZE: int = 5_000
MAX_EXPANDED_BYTES_DEFAULT: int = 256 * 1024**2
MAX_EXPANDED_BYTES: dict[str, int] = {
    "queimadas": 2 * 1024**3,
    "b3": 512 * 1024**2,
    "mapbiomas": 1024**3,
    "antaq": 4 * 1024**3,
    "ibge": 16 * 1024**2,
    "anp_diesel": 512 * 1024**2,
    "abiove": 64 * 1024**2,
    "unica": 64 * 1024**2,
    "conab": 64 * 1024**2,
    "conab_progresso": 64 * 1024**2,
}
MAX_XLSX_CELLS: int = 10_000_000
MAX_WKT_CHARS: int = 16 * 1024**2
MAX_WKT_DEPTH: int = 16

SICAR_MAX_VERSOES_DESCARTADAS = 1_000
SICAR_STATUS_VALIDOS: frozenset[str] = frozenset({"AT", "PE", "SU", "CA"})
SICAR_TIPO_VALIDOS: frozenset[str] = frozenset({"IRU", "AST", "PCT"})

MAPBIOMAS_GEOCODE_PATTERN = r"[0-9]{7}"
MAPBIOMAS_MUNICIPAL_IDENTITY_FIELDS = (
    "ID",
    "country",
    "biome",
    "region",
    "state",
    "geocode",
    "municipality",
    "municipality-state",
    "class",
    "class_level_0",
    "class_level_1",
    "class_level_2",
    "class_level_3",
    "class_level_4",
)
MAPBIOMAS_MUNICIPAL_MEMBER_11 = "MAPBIOMAS_BRAZIL-COL.11-BIOME_STATE_MUNICIPALITY.xlsx"
MAPBIOMAS_MUNICIPAL_IDENTITY_FIELDS_10 = (
    "ID",
    "country",
    "biome",
    "state",
    "municipality",
    "municipality - state",
    "geocode",
    "feature_id",
    "class",
    "class_level_0",
    "class_level_1",
    "class_level_2",
    "class_level_3",
    "class_level_4",
)

INMET_HISTORICO_MIN_ANO = 2000
INMET_HISTORICO_CACHE_MAX_BYTES = 268_435_456
INMET_HISTORICO_MAX_MEMBER_BYTES = 32 * 1024**2
INMET_HISTORICO_MAX_EXPANDED_BYTES = 512 * 1024**2
INMET_HISTORICO_CACHE_CURRENT_TTL = 3_600
INMET_HISTORICO_CACHE_CLOSED_TTL = 86_400
CLIMA_MIN_ANO = 1981
DEFENSIVOS_CACHE_FORMAT_VERSION = 2
DEFENSIVOS_CACHE_TTL_SECONDS = 86_400
DEFENSIVOS_REGISTRO_PATTERN = r"[A-Za-z0-9][\w./-]*"
RNC_DATE_PATTERN = r"[0-9]{2}/[0-9]{2}/[0-9]{4}"
SNPC_CONDITIONAL_END = "até a emissão do certificado definitivo"
RNC_CACHE_FORMAT_VERSION = 1
RNC_CACHE_TTL_SECONDS = 86_400
RNC_MIN_CSV_SIZE = 500_000
RNC_PUBLIC_URLS = {
    kind: f"{URLS[Fonte.RNC]['cultivarweb']}/cultivares_{kind}.php"
    for kind in ("registradas", "protegidas")
}
RNC_PUBLIC_RESPONSE_HEADERS = (
    "content-type",
    "content-length",
    "content-disposition",
    "date",
    "last-modified",
    "etag",
)
LISTA_SUJA_DATE_PATTERN = r"[0-9]{2}/[0-9]{2}/[0-9]{4}"
LISTA_SUJA_PUBLICATION_TITLE = (
    "Cadastro de Empregadores que tenham submetido trabalhadores a condições análogas à escravidão"
)
COMTRADE_GUEST_MAX_RECORDS = 500
COMTRADE_AUTH_MAX_RECORDS = 100_000
COMTRADE_GUEST_MAX_PERIODS = 1
COMTRADE_AUTH_MAX_PERIODS = 12
COMTRADE_HS_PATTERN = r"(?:[0-9]{2}|[0-9]{4}|[0-9]{6})"
COMTRADE_CLASSIFICATION_PATTERN = r"H[0-9]+"
BCB_FOCUS_ENTITIES: dict[str, str] = {
    "anual": "ExpectativasMercadoAnuais",
    "mensal": "ExpectativaMercadoMensais",
}
BCB_FOCUS_ORDER_BY: dict[str, str] = {
    "anual": "Data desc,DataReferencia asc,baseCalculo asc,IndicadorDetalhe asc",
    "mensal": "Data desc,DataReferencia asc,baseCalculo asc",
}
BCB_FOCUS_DATE_PATTERN = r"[0-9]{4}-[0-9]{2}-[0-9]{2}"
BCB_FOCUS_ANNUAL_REFERENCE_PATTERN = r"[0-9]{4}"
BCB_FOCUS_MONTHLY_REFERENCE_PATTERN = r"[0-9]{2}/[0-9]{4}"
BCB_FOCUS_MAX_REDIRECTS = 3

BCB_PTAX_PAGE_SIZE = 1000
BCB_PTAX_DEFAULT_DAYS = 30
BCB_PTAX_MAX_REDIRECTS = 3
BCB_PTAX_CURRENCY_PATTERN = r"[A-Z]{3}"
BCB_PTAX_INPUT_CURRENCY_PATTERN = r"[A-Za-z]{3}"
BCB_PTAX_TIMESTAMP_PATTERN = (
    r"[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,9})?"
)
BCB_PTAX_BULLETIN_LABELS: dict[str, str] = {
    "Abertura": "abertura",
    "Intermediário": "intermediario",
    "Fechamento PTAX": "fechamento",
    "Fechamento": "fechamento",
}
BCB_PTAX_QUOTES_ORDER_BY = "dataHoraCotacao asc,tipoBoletim asc"
BCB_PTAX_CATALOG_ORDER_BY = "simbolo asc"

BCB_SGS_MAX_WINDOW_YEARS = 10
BCB_SGS_DATE_PATTERN = r"[0-9]{2}/[0-9]{2}/[0-9]{4}"
BCB_SGS_VALUE_PATTERN = r"[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?"

LSPA_ESTIMATIVA_COMPONENTES: dict[str, tuple[str, ...]] = {
    "soja": ("soja",),
    "milho": ("milho_1", "milho_2"),
    "arroz": ("arroz",),
    "feijao": ("feijao_1", "feijao_2", "feijao_3"),
    "trigo": ("trigo",),
    "algodao": ("algodao",),
}

LSPA_ESTIMATIVA_UNIDADES: dict[str, str] = {
    "Área plantada": "Hectares",
    "Área colhida": "Hectares",
    "Produção": "Toneladas",
}

ZARC_RISK_COLUMNS: tuple[str, ...] = tuple(f"dec{i}" for i in range(1, 37))
ZARC_CSV_COLUMNS: tuple[str, ...] = (
    "Nome_cultura",
    "SafraIni",
    "SafraFin",
    "Cod_Cultura",
    "Cod_Ciclo",
    "Cod_Solo",
    "geocodigo",
    "UF",
    "municipio",
    "Cod_Clima",
    "Nome_Clima",
    "Cod_Outros_Manejos",
    "Nome_Outros_Manejos",
    "Produtividade",
    "Cod_NM",
    "Cod_Munic",
    "Cod_Meso",
    "Cod_Micro",
    "Portaria",
    *ZARC_RISK_COLUMNS,
)
ZARC_OUTPUT_COLUMNS: tuple[str, ...] = (
    "cultura",
    "safra",
    "geocodigo",
    "uf",
    "municipio",
    "solo_codigo",
    "ciclo_codigo",
    "clima",
    "manejo",
    "portaria",
    *ZARC_RISK_COLUMNS,
    "cultura_original",
    "safra_inicio",
    "safra_fim",
    "cultura_codigo",
    "clima_codigo",
    "manejo_codigo",
    "produtividade_texto",
    "nm_codigo",
    "municipio_sicor_codigo",
    "mesorregiao_codigo",
    "microrregiao_codigo",
    "registro_origem",
)
ZARC_CSV_TO_OUTPUT: dict[str, str] = {
    "Nome_cultura": "cultura_original",
    "SafraIni": "safra_inicio",
    "SafraFin": "safra_fim",
    "Cod_Cultura": "cultura_codigo",
    "Cod_Ciclo": "ciclo_codigo",
    "Cod_Solo": "solo_codigo",
    "geocodigo": "geocodigo",
    "UF": "uf",
    "municipio": "municipio",
    "Cod_Clima": "clima_codigo",
    "Nome_Clima": "clima",
    "Cod_Outros_Manejos": "manejo_codigo",
    "Nome_Outros_Manejos": "manejo",
    "Produtividade": "produtividade_texto",
    "Cod_NM": "nm_codigo",
    "Cod_Munic": "municipio_sicor_codigo",
    "Cod_Meso": "mesorregiao_codigo",
    "Cod_Micro": "microrregiao_codigo",
    "Portaria": "portaria",
    **{name: name for name in ZARC_RISK_COLUMNS},
}
ZARC_INTEGER_COLUMNS: tuple[str, ...] = (
    "solo_codigo",
    "ciclo_codigo",
    *ZARC_RISK_COLUMNS,
    "registro_origem",
)
ZARC_STRING_COLUMNS: tuple[str, ...] = tuple(
    name for name in ZARC_OUTPUT_COLUMNS if name not in ZARC_INTEGER_COLUMNS
)
ZARC_TEXT_CODE_COLUMNS: tuple[str, ...] = (
    "cultura_codigo",
    "clima_codigo",
    "manejo_codigo",
    "nm_codigo",
    "municipio_sicor_codigo",
    "mesorregiao_codigo",
    "microrregiao_codigo",
)
ZARC_RISK_VALUES: frozenset[int] = frozenset({0, 20, 30, 40, 50})
ZARC_SOIL_CODES: frozenset[int] = frozenset({1, 2, 3, 11, 12, 13, 14, 15, 16})
ZARC_CYCLE_CODES: frozenset[int] = frozenset({13, 19, 20, 21, 22, 24, 25, 26})
ZARC_NON_ANNUAL_SEASONS: dict[str, str] = {
    "PERENE": "perene",
    "OLERÍCOLA": "olericola",
    "SEM SAFRA": "sem_safra",
}
ZARC_CACHE_TTL_SECONDS = 86_400
ZARC_STORE_MAX_REVISIONS = 3
ZARC_STORE_BATCH_ROWS = 100_000
ZARC_STORE_FILENAME = "zarc_tabuas.duckdb"
ZARC_CATALOG_TTL_SECONDS = 3_600
ZARC_MAX_CATALOG_BYTES = 4 * 1024**2
ZARC_MAX_DOWNLOAD_BYTES = 768 * 1024**2
ZARC_MAX_TRANSFER_BYTES = 1536 * 1024**2
ZARC_PUBLIC_RESPONSE_HEADERS: tuple[str, ...] = (
    "content-type",
    "content-length",
    "content-encoding",
    "etag",
    "last-modified",
    "date",
    "location",
)

DESMATAMENTO_DEFAULT_PAGE_SIZE = 500
DESMATAMENTO_GEO_DEFAULT_PAGE_SIZE = 100
DESMATAMENTO_MAX_PAGE_SIZE = 2_000
DESMATAMENTO_GEO_MAX_PAGE_SIZE = 500
DESMATAMENTO_DEFAULT_MAX_RECORDS = 50_000
DESMATAMENTO_GEO_DEFAULT_MAX_RECORDS = 10_000
DESMATAMENTO_MAX_BODY_BYTES = 16 * 1024**2
DESMATAMENTO_MAX_TOTAL_BYTES = 512 * 1024**2
DESMATAMENTO_MAX_RETAINED_BYTES = 512 * 1024**2
DESMATAMENTO_MAX_PAGES = 5_000
DESMATAMENTO_DATE_PATTERN = r"[0-9]{4}-[0-9]{2}-[0-9]{2}"

DESMATAMENTO_PRODES_COLUMNS = (
    "ano",
    "uf",
    "classe",
    "area_km2",
    "satelite",
    "sensor",
    "bioma",
    "feature_id",
    "uuid",
    "fid",
    "estado_original",
    "path_row",
    "class_name",
    "def_cloud",
    "julian_day",
    "image_date",
    "scene_id",
    "publish_year",
    "source",
    "pub_date",
)
DESMATAMENTO_DETER_COLUMNS = (
    "data",
    "classe",
    "uf",
    "municipio",
    "municipio_id",
    "area_km2",
    "satelite",
    "sensor",
    "bioma",
    "feature_id",
    "gid",
    "uf_original",
    "quadrant",
    "path_row",
    "areauckm",
    "uc",
    "publish_month",
    "created_date",
    "areatotalkm",
)

EMBRAPA_SOLOS_WFS_VERSION = "2.0.0"

EMBRAPA_SOLOS_NC_WARNING = (
    "Embrapa Solos: as camadas PronaSolos 2020 e Mapa de Solos do Brasil usam CC BY-NC 3.0 "
    "BR; atribuição obrigatória e uso comercial sujeito a permissão do titular. Veja "
    "https://www.agrobr.dev/docs/licenses/."
)

EMBRAPA_SOLOS_INTEGER_BITS = {"fid": 32, "ogc_fid": 32, "codigo_pon": 64}

EMBRAPA_SOLOS_FLOAT_PROPERTIES = frozenset({"gcs_latitu", "gcs_longit", "area_km2"})

EMBRAPA_SOLOS_NAMESPACE = "geonode"

EMBRAPA_SOLOS_LAYERS = {"perfis": "perfis_pronasolos_2020", "mapa": "brasil_solos_5m_20201104"}

EMBRAPA_SOLOS_GEOMETRY_COLUMNS = {"perfis": "geom", "mapa": "geometry"}

EMBRAPA_SOLOS_SORT_FIELDS = {"perfis": "fid", "mapa": "ogc_fid"}

EMBRAPA_SOLOS_CRS = "EPSG:4326"

EMBRAPA_SOLOS_CRS_NAMES = ("EPSG:4326", "urn:ogc:def:crs:EPSG::4326")

EMBRAPA_SOLOS_DEFAULT_MAX_RECORDS = 50000

EMBRAPA_SOLOS_GEO_DEFAULT_MAX_RECORDS = {"perfis": 5000, "mapa": 3000}

EMBRAPA_SOLOS_DEFAULT_PAGE_SIZES = {"perfis": 250, "mapa": 500}

EMBRAPA_SOLOS_GEO_DEFAULT_PAGE_SIZES = {"perfis": 100, "mapa": 25}

EMBRAPA_SOLOS_MAX_PAGE_SIZE = 1000

EMBRAPA_SOLOS_GEO_MAX_PAGE_SIZE = 100

EMBRAPA_SOLOS_MAX_BODY_BYTES = 8388608

EMBRAPA_SOLOS_MAX_TOTAL_BODY_BYTES = 268435456

EMBRAPA_SOLOS_MAX_RETAINED_BYTES = 268435456

EMBRAPA_SOLOS_MAX_PAGES = 2000

EMBRAPA_SOLOS_MAX_DIAGNOSTIC_EXAMPLES = 10


EMBRAPA_SOLOS_PERFIS_PROPERTIES = (
    "fid",
    "sigla",
    "titulo",
    "ano",
    "referencia",
    "autor",
    "nivel_leva",
    "codigo_pon",
    "sigla_bd",
    "tipo_ponto",
    "responsave",
    "data_colet",
    "material_o",
    "uso_atual",
    "observacoe",
    "situacao_c",
    "gcs_latitu",
    "gcs_longit",
    "municipio",
    "uf",
    "simbolo_ho",
    "profundida",
    "profundi_1",
    "descricao_",
    "calhau",
    "cascalho",
    "terra_fina",
    "areia_gros",
    "areia_fina",
    "areia_tota",
    "silte",
    "argila",
    "argila_dis",
    "grau_flocu",
    "relacao_si",
    "densidade_",
    "densidad_1",
    "porosidade",
    "retencao_u",
    "retencao_1",
    "agua_dispo",
    "ph_h2o",
    "ph_kcl",
    "complexo_s",
    "complexo_1",
    "complexo_2",
    "complexo_3",
    "valor_s",
    "aluminio_t",
    "hidrogenio",
    "valor_t",
    "valor_v",
    "saturacao_",
    "fosforo_as",
    "carbono_or",
    "nitrogenio",
    "carbono_ni",
    "ataque_sul",
    "ataque_s_1",
    "ataque_s_2",
    "ataque_s_3",
    "ataque_s_4",
    "ataque_s_5",
    "ki",
    "kr",
    "al2o3_fe2o",
    "cdb_fe",
    "equivalent",
    "saturaca_1",
    "condutivid",
    "agua_pasta",
    "sais_sol_e",
    "sais_sol_1",
    "sais_sol_2",
    "sais_sol_3",
    "sais_sol_4",
    "sais_sol_5",
    "sais_sol_6",
    "classe_tex",
    "grau_consi",
    "grau_con_1",
    "pegajosida",
    "plasticida",
)

EMBRAPA_SOLOS_PERFIS_RENAME_MAP = {
    "gcs_latitu": "latitude",
    "gcs_longit": "longitude",
    "simbolo_ho": "horizonte",
    "profundida": "profundidade",
    "areia_tota": "areia_total",
    "carbono_or": "carbono_organico",
    "valor_t": "ctc",
    "valor_v": "saturacao_bases",
    "aluminio_t": "aluminio",
    "fosforo_as": "fosforo",
    "classe_tex": "classe_textural",
    "nivel_leva": "nivel_levantamento",
    "uf": "uf_original",
}

EMBRAPA_SOLOS_PERFIS_COLUMNS = (
    "fid",
    "uf",
    "municipio",
    "latitude",
    "longitude",
    "horizonte",
    "profundidade",
    "areia_total",
    "silte",
    "argila",
    "ph_h2o",
    "carbono_organico",
    "ctc",
    "saturacao_bases",
    "aluminio",
    "fosforo",
    "classe_textural",
    "nivel_levantamento",
    "uso_atual",
    "sigla",
    "titulo",
    "ano",
    "referencia",
    "autor",
    "codigo_pon",
    "sigla_bd",
    "tipo_ponto",
    "responsave",
    "data_colet",
    "material_o",
    "observacoe",
    "situacao_c",
    "profundi_1",
    "descricao_",
    "calhau",
    "cascalho",
    "terra_fina",
    "areia_gros",
    "areia_fina",
    "argila_dis",
    "grau_flocu",
    "relacao_si",
    "densidade_",
    "densidad_1",
    "porosidade",
    "retencao_u",
    "retencao_1",
    "agua_dispo",
    "ph_kcl",
    "complexo_s",
    "complexo_1",
    "complexo_2",
    "complexo_3",
    "valor_s",
    "hidrogenio",
    "saturacao_",
    "nitrogenio",
    "carbono_ni",
    "ataque_sul",
    "ataque_s_1",
    "ataque_s_2",
    "ataque_s_3",
    "ataque_s_4",
    "ataque_s_5",
    "ki",
    "kr",
    "al2o3_fe2o",
    "cdb_fe",
    "equivalent",
    "saturaca_1",
    "condutivid",
    "agua_pasta",
    "sais_sol_e",
    "sais_sol_1",
    "sais_sol_2",
    "sais_sol_3",
    "sais_sol_4",
    "sais_sol_5",
    "sais_sol_6",
    "grau_consi",
    "grau_con_1",
    "pegajosida",
    "plasticida",
    "uf_original",
    "feature_id",
)

FUNAI_WFS_VERSION = "2.0.0"
FUNAI_NAMESPACE = "Funai"
FUNAI_LAYER = "tis_poligonais"
FUNAI_GEOM_COLUMN = "the_geom"
FUNAI_DEFAULT_MAX_RECORDS = 10_000
FUNAI_GEO_DEFAULT_MAX_RECORDS = 1_000
FUNAI_DEFAULT_PAGE_SIZE = 250
FUNAI_GEO_DEFAULT_PAGE_SIZE = 10
FUNAI_MAX_PAGE_SIZE = 1_000
FUNAI_GEO_MAX_PAGE_SIZE = 100
FUNAI_MAX_BODY_BYTES = 8 * 1024**2
FUNAI_MAX_TOTAL_BODY_BYTES = 256 * 1024**2
FUNAI_SORT_FIELDS = ("terrai_codigo", "gid")
FUNAI_MAX_RETAINED_BYTES = 256 * 1024**2
FUNAI_MAX_PAGES = 2_000
FUNAI_MAX_DIAGNOSTIC_EXAMPLES = 10
FUNAI_CRS = "EPSG:4326"
FUNAI_CRS_NAMES = frozenset({"EPSG:4326", "urn:ogc:def:crs:EPSG::4326"})
FUNAI_INTERMEDIATE_SHA256 = "6542d176bed50f193c0ce297ae44ecd8a0a86bec2ede682769344059b4e78530"
FUNAI_TOLERANCIA_AREA = 0.05
FUNAI_DATA_FORMATO = "%d/%m/%Y"
ALBERS_BRASIL = "+proj=aea +lat_0=-12 +lon_0=-54 +lat_1=-2 +lat_2=-22 +x_0=0 +y_0=0 +ellps=GRS80 +units=m +no_defs"


FUNAI_INTEGER_BITS = {"gid": 32, "terrai_codigo": 32, "undadm_codigo": 64, "epsg": 32}
FUNAI_PROPERTIES = (
    "gid",
    "terrai_codigo",
    "terrai_nome",
    "etnia_nome",
    "municipio_nome",
    "uf_sigla",
    "superficie_perimetro_ha",
    "fase_ti",
    "modalidade_ti",
    "reestudo_ti",
    "cr",
    "faixa_fronteira",
    "undadm_codigo",
    "undadm_nome",
    "undadm_sigla",
    "dominio_uniao",
    "data_atualizacao",
    "epsg",
)
FUNAI_RENAME_MAP = {
    "terrai_codigo": "codigo",
    "terrai_nome": "nome",
    "etnia_nome": "etnia",
    "municipio_nome": "municipio",
    "uf_sigla": "uf",
    "superficie_perimetro_ha": "area_ha",
    "fase_ti": "fase",
    "modalidade_ti": "modalidade",
}
FUNAI_COLUMNS = (
    "codigo",
    "nome",
    "etnia",
    "municipio",
    "uf",
    "area_ha",
    "fase",
    "modalidade",
    "data_atualizacao",
    "feature_id",
    "gid",
    "reestudo_ti",
    "cr",
    "faixa_fronteira",
    "undadm_codigo",
    "undadm_nome",
    "undadm_sigla",
    "dominio_uniao",
    "epsg",
)
FUNAI_FASES_VALIDAS = frozenset(
    {
        "Regularizada",
        "Homologada",
        "Declarada",
        "Delimitada",
        "Em Estudo",
        "Encaminhada RI",
    }
)

EMBRAPA_SOLOS_MAPA_PROPERTIES = (
    "ogc_fid",
    "simbolos",
    "comp1",
    "comp2",
    "comp3",
    "leg_desc",
    "area_km2",
    "ordem1",
    "subordem1",
    "gdegrupo1",
    "ordem2",
    "subordem2",
    "gdegrupo2",
    "ordem3",
    "subordem3",
    "gdegrupo3",
    "leg_sinot",
    "classe_dom",
)

EMBRAPA_SOLOS_MAPA_RENAME_MAP = {
    "ogc_fid": "fid",
    "leg_desc": "legenda",
    "leg_sinot": "legenda_sinotica",
}

EMBRAPA_SOLOS_MAPA_COLUMNS = (
    "fid",
    "simbolos",
    "comp1",
    "comp2",
    "comp3",
    "legenda",
    "area_km2",
    "ordem1",
    "subordem1",
    "gdegrupo1",
    "ordem2",
    "subordem2",
    "gdegrupo2",
    "legenda_sinotica",
    "classe_dom",
    "ordem3",
    "subordem3",
    "gdegrupo3",
    "feature_id",
)

INCRA_WFS_VERSION = "2.0.0"
INCRA_NAMESPACE = "CMR-PUBLICO"
INCRA_LAYER = "lim_quilombolas_a"
INCRA_GEOM_COLUMN = "geom"
INCRA_DEFAULT_MAX_RECORDS = 1_500
INCRA_GEO_DEFAULT_MAX_RECORDS = 1_500
INCRA_DEFAULT_PAGE_SIZE = 250
INCRA_GEO_DEFAULT_PAGE_SIZE = 10
INCRA_MAX_PAGE_SIZE = 1_000
INCRA_GEO_MAX_PAGE_SIZE = 100
INCRA_MAX_BODY_BYTES = 8 * 1024**2
INCRA_MAX_TOTAL_BODY_BYTES = 256 * 1024**2
INCRA_MAX_RETAINED_BYTES = 256 * 1024**2
INCRA_MAX_PAGES = 2_000
INCRA_MAX_DIAGNOSTIC_EXAMPLES = 10
INCRA_SORT_FIELDS = ("cd_quilomb", "nu_processo", "no_comunidade")
INCRA_CRS = "EPSG:4326"
INCRA_CRS_NAMES = frozenset({"EPSG:4326", "urn:ogc:def:crs:EPSG::4326"})


INCRA_INTEGER_BITS = {"cd_quilomb": 32, "nu_familia": 32}
INCRA_TEMPORAL_XSD_NAMESPACE = "http://www.w3.org/2001/XMLSchema"
INCRA_DATE_PROPERTIES = frozenset({"dt_publica", "dt_public1", "dt_titulo", "dt_decreto"})
INCRA_DATETIME_PROPERTIES = frozenset({"dt_cadastro"})
INCRA_PROPERTIES = (
    "co_sr",
    "nu_processo",
    "no_comunidade",
    "no_municipio",
    "sg_uf",
    "dt_publica",
    "dt_public1",
    "nu_familia",
    "dt_titulo",
    "nu_area_ha",
    "no_responsavel",
    "no_esfera",
    "dt_cadastro",
    "cd_quilomb",
    "cd_sipra",
    "ds_descricao",
    "st_titulad",
    "dt_decreto",
    "tp_levanta",
    "nr_escalao",
    "ds_fase",
)
INCRA_RENAME_MAP = {
    "cd_quilomb": "codigo",
    "no_comunidade": "nome",
    "no_municipio": "municipio",
    "sg_uf": "uf",
    "nu_area_ha": "area_ha",
    "nu_familia": "familias",
    "ds_fase": "fase",
    "st_titulad": "titulado",
    "dt_publica": "data_publicacao",
    "dt_titulo": "data_titulo",
    "co_sr": "regional",
    "nu_processo": "processo",
    "dt_public1": "data_publicacao_2",
    "no_responsavel": "responsavel",
    "no_esfera": "esfera",
    "dt_cadastro": "data_cadastro",
    "cd_sipra": "codigo_sipra",
    "ds_descricao": "descricao",
    "dt_decreto": "data_decreto",
    "tp_levanta": "tipo_levantamento",
    "nr_escalao": "escala",
}
INCRA_COLUMNS = (
    "codigo",
    "nome",
    "municipio",
    "uf",
    "area_ha",
    "familias",
    "fase",
    "titulado",
    "data_publicacao",
    "data_titulo",
    "feature_id",
    "regional",
    "processo",
    "data_publicacao_2",
    "responsavel",
    "esfera",
    "data_cadastro",
    "codigo_sipra",
    "descricao",
    "data_decreto",
    "tipo_levantamento",
    "escala",
)
INCRA_FASES_VALIDAS = frozenset(
    {"CCDRU", "DECRETO", "PORTARIA", "RTID", "TITULADO", "TITULO ANULADO", "TITULO PARCIAL"}
)
INCRA_DTYPES_TEMPORAIS = {
    INCRA_RENAME_MAP[raw]: "datetime64[ns]" for raw in sorted(INCRA_DATE_PROPERTIES)
} | {INCRA_RENAME_MAP[raw]: "datetime64[ns, UTC]" for raw in sorted(INCRA_DATETIME_PROPERTIES)}
INCRA_DATA_SEM_DATA = "0001-01-01"

INCRA_ANDAMENTO_PAGE_URL = (
    "https://www.gov.br/incra/pt-br/assuntos/governanca-fundiaria/quilombolas"
)
INCRA_ANDAMENTO_MAX_BODY_BYTES = 16 * 1024**2
INCRA_ANDAMENTO_MAX_TOTAL_BODY_BYTES = 32 * 1024**2
INCRA_ANDAMENTO_MAX_ATTEMPTS = 6
INCRA_ANDAMENTO_MAX_PAGES = 100
INCRA_ANDAMENTO_PARSER_VERSION = 1
INCRA_ANDAMENTO_COLUMNS = (
    "regional",
    "numero_publicado",
    "processo",
    "comunidade",
    "municipio",
    "area_ha_texto",
    "familias_texto",
    "edital_rtid_1",
    "edital_rtid_2",
    "retificacao_edital_1",
    "retificacao_edital_2",
    "portaria",
    "retificacao_portaria",
    "decreto",
    "titulo",
)
INCRA_ANDAMENTO_HEADERS = (
    "SR",
    "Nº",
    "Nº Processo",
    "Comunidade",
    "Município",
    "Área/ha*",
    "Nº de Famílias",
    "1º Edital RTID no DOU",
    "2º Edital RTID no DOU",
    "1ª Retificação Edital",
    "2ª Retificação Edital",
    "Portaria no DOU",
    "Retificação da portaria",
    "Decreto no DOU",
    "Título",
)

INCRA_VINCULOS_MAX_ROWS = 50_000
INCRA_VINCULOS_MAX_RETAINED_BYTES = 256 * 1024**2
INCRA_VINCULOS_REFERENCE_PATTERN = r"(?<![0-9])[0-9]{5}\.[0-9]{6}/[0-9]{4}-[0-9]{2}(?![0-9])"
INCRA_VINCULOS_COLUMNS = (
    "estado_vinculo",
    "referencia_tipo",
    "referencia_literal",
    "perimetro_posicao",
    "perimetro_referencia_posicao",
    "administrativo_referencia_posicao",
    "ocorrencias_perimetro_referencia",
    "ocorrencias_administrativo_referencia",
    "referencia_repetida",
    *(f"perimetro_{name}" for name in INCRA_COLUMNS),
    *(f"administrativo_{name}" for name in INCRA_ANDAMENTO_COLUMNS),
)

ANTT_MAX_CATALOG_BYTES = 2 * 1024**2
ANTT_MAX_CSV_BYTES = 512 * 1024**2
ANTT_MAX_PRACAS_BYTES = 16 * 1024**2
ANTT_MAX_SPOOL_BYTES = 1024**3
ANTT_MAX_TRANSFER_BYTES = 3 * 1024**3
ANTT_STREAM_CHUNK_BYTES = 65536
ANTT_MAX_ATTEMPTS = 3
ANTT_RETRY_BASE_SECONDS = 1.0
ANTT_PARSER_MAX_ROWS = 500_000
ANTT_PARSER_MAX_MEMORY_BYTES = 256 * 1024**2
ANTT_MAX_FIELD_CHARS = 65536
ANTT_CSV_READ_CHUNK = 65536
ANTT_INT64_MAX = 9223372036854775807
ANTT_EXPLICIT_AXLES_PATTERN = r"(?i)(?:ve[ií]culo\s+(?:comercial|passeio)\s+)?([0-9]+)\s+eixos?"
ANTT_FLUXO_COLUMNS = (
    "data",
    "concessionaria",
    "praca",
    "sentido",
    "n_eixos",
    "tipo_veiculo",
    "volume",
    "rodovia",
    "uf",
    "municipio",
    "categoria_eixo",
    "tipo_cobranca",
    "frequencia",
)
ANTT_TIPOS_VEICULO = ("Comercial", "Moto", "Passeio")

COMEXSTAT_DOWNLOAD_HOST = "balanca.mdic.gov.br"
COMEXSTAT_MAX_RESOURCE_BYTES = 256 * 1024**2
COMEXSTAT_MAX_DICTIONARY_BYTES = 8 * 1024**2
COMEXSTAT_MAX_TRANSFER_BYTES = 768 * 1024**2
COMEXSTAT_DOWNLOAD_TIMEOUT_SECONDS = 300
COMEXSTAT_CHUNK_BYTES = 65536
COMEXSTAT_MAX_PHYSICAL_REQUESTS = 9
COMEXSTAT_INTERMEDIATE_SHA256 = "f27095d0b40df765599404011cf8b167da06260e81dda727f163ef08221071e4"
COMEXSTAT_DICTIONARY_URLS = {
    "unidades": "https://balanca.mdic.gov.br/balanca/bd/tabelas/NCM_UNIDADE.csv",
    "paises": "https://balanca.mdic.gov.br/balanca/bd/tabelas/PAIS.csv",
    "vias": "https://balanca.mdic.gov.br/balanca/bd/tabelas/VIA.csv",
    "urfs": "https://balanca.mdic.gov.br/balanca/bd/tabelas/URF.csv",
}
COMEXSTAT_DEFAULT_MAX_ROWS = 500_000
COMEXSTAT_DEFAULT_MAX_MEMORY_BYTES = 256 * 1024**2
COMEXSTAT_CSV_CHUNK_BYTES = 65536
COMEXSTAT_MAX_FIELD_CHARS = 65536
COMEXSTAT_MAX_NUMBER_CHARS = 1024
COMEXSTAT_MAX_NUMBER_EXPONENT = 1024
COMEXSTAT_EXTRA_UFS = ("ND", "EX")
COMEXSTAT_EXPORT_PROPERTIES = (
    "CO_ANO",
    "CO_MES",
    "CO_NCM",
    "CO_UNID",
    "CO_PAIS",
    "SG_UF_NCM",
    "CO_VIA",
    "CO_URF",
    "QT_ESTAT",
    "KG_LIQUIDO",
    "VL_FOB",
)
COMEXSTAT_IMPORT_PROPERTIES = (*COMEXSTAT_EXPORT_PROPERTIES, "VL_FRETE", "VL_SEGURO")
COMEXSTAT_DICTIONARY_PROPERTIES = {
    "unidades": ("CO_UNID", "NO_UNID", "SG_UNID"),
    "paises": (
        "CO_PAIS",
        "CO_PAIS_ISON3",
        "CO_PAIS_ISOA3",
        "NO_PAIS",
        "NO_PAIS_ING",
        "NO_PAIS_ESP",
    ),
    "vias": ("CO_VIA", "NO_VIA"),
    "urfs": ("CO_URF", "NO_URF"),
}
COMEXSTAT_CODE_WIDTHS = {"CO_NCM": 8, "CO_UNID": 2, "CO_PAIS": 3, "CO_VIA": 2, "CO_URF": 7}
COMEXSTAT_RENAME_MAP = {
    "CO_ANO": "ano",
    "CO_MES": "mes",
    "CO_NCM": "ncm",
    "CO_UNID": "cod_unidade",
    "CO_PAIS": "cod_pais",
    "SG_UF_NCM": "uf",
    "CO_VIA": "cod_via",
    "CO_URF": "cod_urf",
    "QT_ESTAT": "qtd_estatistica",
    "KG_LIQUIDO": "kg_liquido",
    "VL_FOB": "valor_fob_usd",
    "VL_FRETE": "valor_frete_usd",
    "VL_SEGURO": "valor_seguro_usd",
    "NO_UNID": "unidade",
    "SG_UNID": "sigla_unidade",
    "CO_PAIS_ISON3": "cod_pais_iso_numerico",
    "CO_PAIS_ISOA3": "cod_pais_iso_alfa3",
    "NO_PAIS": "pais",
    "NO_PAIS_ING": "pais_ingles",
    "NO_PAIS_ESP": "pais_espanhol",
    "NO_VIA": "via",
    "NO_URF": "urf",
}

ANP_DIESEL_PRECOS_COLUMNS = [
    "data",
    "uf",
    "municipio",
    "produto",
    "preco_venda",
    "preco_compra",
    "n_postos",
    "margem",
    "periodo_inicio",
    "periodo_fim",
    "nivel",
    "unidade",
    "agregacao",
    "n_semanas",
    "n_postos_media",
]

ANP_DIESEL_PRECOS_DTYPES = {
    "data": "datetime64[ns]",
    "uf": "str",
    "municipio": "str",
    "produto": "str",
    "preco_venda": "float64",
    "preco_compra": "float64",
    "n_postos": "Int64",
    "margem": "float64",
    "periodo_inicio": "datetime64[ns]",
    "periodo_fim": "datetime64[ns]",
    "nivel": "str",
    "unidade": "str",
    "agregacao": "str",
    "n_semanas": "Int64",
    "n_postos_media": "float64",
}

NASA_POWER_PARAMETER_DEFINITIONS = (
    ("T2M", "temp_media", "C", "temp_media", "C"),
    ("T2M_MAX", "temp_max", "C", "temp_max_media", "C"),
    ("T2M_MIN", "temp_min", "C", "temp_min_media", "C"),
    ("PRECTOTCORR", "precip_mm", "mm/day", "precip_acum_mm", "mm"),
    ("RH2M", "umidade_rel", "%", "umidade_media", "%"),
    ("ALLSKY_SFC_SW_DWN", "radiacao_mj", "MJ/m^2/day", "radiacao_media_mj", "MJ/m^2/day"),
    ("WS2M", "vento_ms", "m/s", "vento_medio_ms", "m/s"),
    ("PS", "ps_kpa", "kPa", "ps_kpa", "kPa"),
    ("WS10M", "vento_10m_ms", "m/s", "vento_10m_ms", "m/s"),
    ("T2MDEW", "ponto_orvalho", "C", "ponto_orvalho", "C"),
    ("GWETROOT", "umidade_solo_raiz", "1", "umidade_solo_raiz", "1"),
    ("GWETTOP", "umidade_solo_superficie", "1", "umidade_solo_superficie", "1"),
)

CONAB_CUSTOS_CATALOG_URL = "https://www.gov.br/conab/pt-br/atuacao/informacoes-agropecuarias/custos-de-producao/arquivos-custo-de-producao/agricolas"

CONAB_CUSTOS_TAB_URL = "https://www.gov.br/conab/pt-br/atuacao/informacoes-agropecuarias/custos-de-producao/planilhas-de-custos-de-producao/copy_of_agricolas"

CONAB_SOCIOBIO_CATALOG_URL = "https://www.gov.br/conab/pt-br/atuacao/informacoes-agropecuarias/custos-de-producao/arquivos-custo-de-producao/sociobiodiversidade"

CONAB_SOCIOBIO_TAB_URL = "https://www.gov.br/conab/pt-br/atuacao/informacoes-agropecuarias/custos-de-producao/planilhas-de-custos-de-producao/copy_of_sociobiodiversidade"

CONAB_SAFRA_PARSER_VERSION = 3
IBGE_PAM_PARSER_VERSION = 2

IBGE_PEVS_PARSER_VERSION = 2
IBGE_ABATE_PARSER_VERSION = 2
IBGE_LEITE_PARSER_VERSION = 2
IBGE_PIB_PARSER_VERSION = 2
IBGE_CENSO_PARSER_VERSION = 3
IBGE_CENSO_HISTORICO_PARSER_VERSION = 2
IBGE_CENSO_MUNICIPAL_1985_PARSER_VERSION = 3

IBGE_PAM_VARIABLE_LABELS = {
    "Área plantada": "area_plantada",
    "Área plantada ou destinada à colheita": "area_plantada",
    "Área colhida": "area_colhida",
    "Quantidade produzida": "producao",
    "Rendimento médio da produção": "rendimento",
    "Valor da produção": "valor_producao",
}

CONAB_CUSTOS_PARSER_VERSION = 5

CONAB_CUSTOS_TOLERANCIA_PARCELA = 0.005

CONAB_SOCIOBIO_PARSER_VERSION = 2

CONAB_CUSTOS_CATALOG_TTL_SECONDS = 3_600

CONAB_CUSTOS_MAX_BODY_BYTES = 16777216

CONAB_CUSTOS_MAX_TOTAL_BYTES = 41943040

CONAB_CUSTOS_MAX_REQUESTS = 32

CONAB_CUSTOS_MAX_EXPANDED_BYTES = 134217728

CONAB_CUSTOS_MAX_SHEETS = 1000

CONAB_CUSTOS_MAX_SHEET_ROWS = 10000

CONAB_CUSTOS_MAX_SHEET_CELLS = 2000000

CONAB_SERIE_CAFE_SHEETS: dict[str, tuple[str, float]] = {
    "area em producao": ("area_em_producao_mil_ha", 0.001),
    "area em formacao": ("area_formacao_mil_ha", 0.001),
    "producao": ("producao_mil_ton", 0.06),
    "produtividade": ("produtividade_kg_ha", 60.0),
}

CONAB_SERIE_PRODUCT_SHEETS: dict[str, dict[str, tuple[str, float]]] = {
    "cafe": CONAB_SERIE_CAFE_SHEETS,
    "cafe_arabica": CONAB_SERIE_CAFE_SHEETS,
    "cafe_conilon": CONAB_SERIE_CAFE_SHEETS,
    "cana": {
        "area": ("area_colhida_mil_ha", 1.0),
        "producao": ("producao_mil_ton", 1.0),
        "produtividade": ("produtividade_kg_ha", 1.0),
    },
    "cana_area_total": {"area total": ("area_plantada_mil_ha", 1.0)},
    "algodao": {
        "area": ("area_plantada_mil_ha", 1.0),
        "producao algodao em caroco": ("producao_mil_ton", 1.0),
        "produtividade algodao em caroco": ("produtividade_kg_ha", 1.0),
    },
    "algodao_pluma": {
        "area": ("area_plantada_mil_ha", 1.0),
        "producao de pluma": ("producao_mil_ton", 1.0),
        "produtividade pluma": ("produtividade_kg_ha", 1.0),
    },
    "algodao_caroco": {
        "area": ("area_plantada_mil_ha", 1.0),
        "producao de caroco de algodao": ("producao_mil_ton", 1.0),
        "produtividade caroco de algodao": ("produtividade_kg_ha", 1.0),
    },
}

CONAB_SERIE_IGNORED_SHEETS: dict[str, dict[str, str]] = {
    "cana_area_total": {
        "area colhida": "area_colhida_nao_representa_area_total",
        "area de renovacao": "componente_da_area_total",
        "area de expansao": "componente_da_area_total",
        "area de plantio (exp+ren)": "componente_da_area_total",
        "area de mudas": "componente_da_area_total",
        "colheita manual": "modalidade_de_colheita_fora_do_contrato",
        "colheita mecanizada": "modalidade_de_colheita_fora_do_contrato",
        "producao de mudas": "mudas_nao_representam_producao_agricola",
    },
    **{
        product: {
            **{
                sheet: "outro_recorte_de_algodao"
                for other in ("algodao", "algodao_pluma", "algodao_caroco")
                for sheet in CONAB_SERIE_PRODUCT_SHEETS[other]
                if sheet not in CONAB_SERIE_PRODUCT_SHEETS[product]
            },
            "rendimento pluma (%)": "percentual_fora_do_contrato",
        }
        for product in ("algodao", "algodao_pluma", "algodao_caroco")
    },
}
