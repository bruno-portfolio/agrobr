"""Normalização de dados - unidades, datas, regiões, encoding."""

from __future__ import annotations

from .encoding import decode_content, detect_encoding
from .units import (
    converter,
    preco_saca_para_tonelada,
    preco_tonelada_para_saca,
    sacas_para_toneladas,
    toneladas_para_sacas,
)
from .dates import (
    safra_atual,
    validar_safra,
    normalizar_safra,
    safra_para_anos,
    anos_para_safra,
    safra_anterior,
    safra_posterior,
    lista_safras,
    periodo_safra,
)
from .regions import (
    normalizar_uf,
    uf_para_nome,
    uf_para_regiao,
    uf_para_ibge,
    ibge_para_uf,
    listar_ufs,
    listar_regioes,
    normalizar_municipio,
    normalizar_praca,
    validar_uf,
)

__all__: list[str] = [
    "decode_content",
    "detect_encoding",
    "converter",
    "preco_saca_para_tonelada",
    "preco_tonelada_para_saca",
    "sacas_para_toneladas",
    "toneladas_para_sacas",
    "safra_atual",
    "validar_safra",
    "normalizar_safra",
    "safra_para_anos",
    "anos_para_safra",
    "safra_anterior",
    "safra_posterior",
    "lista_safras",
    "periodo_safra",
    "normalizar_uf",
    "uf_para_nome",
    "uf_para_regiao",
    "uf_para_ibge",
    "ibge_para_uf",
    "listar_ufs",
    "listar_regioes",
    "normalizar_municipio",
    "normalizar_praca",
    "validar_uf",
]
