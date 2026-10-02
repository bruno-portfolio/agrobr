from __future__ import annotations

from agrobr.constants import URLS, Fonte
from agrobr.utils.geo import LayerConfig

ANA_BASE: str = URLS[Fonte.ANA]["arcgis"]

LAYERS: dict[str, LayerConfig] = {
    "hidrografia": {
        "service_path": "Hidrografia/MapServer/0",
        "max_record_count": 1000,
        "oid_field": "OBJECTID",
        "fields": "OBJECTID,COCURSODAG,COBACIA,NORIOCOMP,DEDOMINIAL",
        "rename_map": {
            "COCURSODAG": "codigo_curso",
            "COBACIA": "codigo_bacia",
            "NORIOCOMP": "nome_rio",
            "DEDOMINIAL": "dominio",
        },
        "colunas_saida": [
            "OBJECTID",
            "codigo_curso",
            "codigo_bacia",
            "nome_rio",
            "dominio",
        ],
        "required_cols": {"COCURSODAG"},
    },
    "pivos_irrigacao": {
        "service_path": "Pivos_Mapeados/MapServer/0",
        "max_record_count": 1000,
        "oid_field": "OBJECTID",
        "fields": "OBJECTID,CD_GEOCMU,NM_MUNICIP,NM_ESTADO,REGIAO_HID,HECTARES",
        "rename_map": {
            "CD_GEOCMU": "codigo_municipio",
            "NM_MUNICIP": "municipio",
            "NM_ESTADO": "estado",
            "REGIAO_HID": "regiao_hidro",
            "HECTARES": "area_ha",
        },
        "colunas_saida": [
            "OBJECTID",
            "codigo_municipio",
            "municipio",
            "estado",
            "regiao_hidro",
            "area_ha",
        ],
        "required_cols": {"NM_ESTADO"},
    },
    "demanda_irrigacao": {
        "service_path": "Demanda_de_Irrigacao_Vazao_de_Retirada_para_Irrigacao/MapServer/0",
        "max_record_count": 1000,
        "oid_field": "OBJECTID",
        "fields": "OBJECTID,ID,COBACIA,DSVERSAO,VZMAXMEN,VZMESSEC,VZMESIRR,VZMEDANO",
        "rename_map": {
            "COBACIA": "codigo_bacia",
            "DSVERSAO": "versao",
            "VZMAXMEN": "vazao_max_mensal",
            "VZMESSEC": "vazao_mes_seco",
            "VZMESIRR": "vazao_mes_irrigacao",
            "VZMEDANO": "vazao_media_anual",
        },
        "colunas_saida": [
            "OBJECTID",
            "ID",
            "codigo_bacia",
            "versao",
            "vazao_max_mensal",
            "vazao_mes_seco",
            "vazao_mes_irrigacao",
            "vazao_media_anual",
        ],
        "required_cols": {"COBACIA"},
    },
    "disponibilidade_hidrica": {
        "service_path": "Disponibilidade_Hidrica_Superficial/MapServer/0",
        "max_record_count": 1000,
        "oid_field": "OBJECTID",
        "fields": "OBJECTID,ID,NUAREAMONT,DISPQ95,NMRIO,DEDOMINIAL,DSVERSAO",
        "rename_map": {
            "NUAREAMONT": "area_montante_km2",
            "DISPQ95": "disponibilidade_m3_s",
            "NMRIO": "nome_rio",
            "DEDOMINIAL": "dominio",
            "DSVERSAO": "versao",
        },
        "colunas_saida": [
            "OBJECTID",
            "ID",
            "area_montante_km2",
            "disponibilidade_m3_s",
            "nome_rio",
            "dominio",
            "versao",
        ],
        "required_cols": {"DISPQ95"},
    },
}

ANA_SPR: str = URLS[Fonte.ANA]["arcgis_spr"]

_MASSAS_CAMPOS = (
    "FID,esp_cd,nmoriginal,nmalternat,detipomass,dedominial,defiscaliz,usoprinc,nuvolumhm3,"
    "nuareakm2,nuareaha,nuperimkm,dtreserv,nmriocomp,cod_snisb,cotrecho,nmufe,nmmun,deversao"
)

MASSAS_DAGUA: LayerConfig = {
    "service_path": "Massa_dagua/MapServer/0",
    "max_record_count": 1000,
    "oid_field": "FID",
    "fields": _MASSAS_CAMPOS,
    "rename_map": {
        "esp_cd": "codigo",
        "nmoriginal": "nome",
        "nmalternat": "nome_alternativo",
        "detipomass": "tipo",
        "dedominial": "dominio",
        "defiscaliz": "entidade_fiscalizadora",
        "usoprinc": "uso_principal",
        "nuvolumhm3": "volume_hm3",
        "nuareakm2": "area_km2",
        "nuareaha": "area_ha",
        "nuperimkm": "perimetro_km",
        "dtreserv": "data_construcao",
        "nmriocomp": "nome_rio",
        "cod_snisb": "codigo_snisb",
        "cotrecho": "codigo_trecho",
        "nmufe": "uf",
        "nmmun": "municipios",
        "deversao": "fonte_geometria",
    },
    "colunas_saida": [
        "FID",
        "codigo",
        "nome",
        "nome_alternativo",
        "tipo",
        "dominio",
        "entidade_fiscalizadora",
        "uso_principal",
        "volume_hm3",
        "area_km2",
        "area_ha",
        "perimetro_km",
        "data_construcao",
        "nome_rio",
        "codigo_snisb",
        "codigo_trecho",
        "uf",
        "municipios",
        "fonte_geometria",
    ],
    "required_cols": set(_MASSAS_CAMPOS.split(",")),
}
