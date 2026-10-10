"""Camada semântica de datasets do agrobr."""

from agrobr.datasets.abate_trimestral import abate_trimestral
from agrobr.datasets.autorizacoes_defensivos import autorizacoes_defensivos
from agrobr.datasets.balanco import balanco
from agrobr.datasets.cadastro_rural import cadastro_rural
from agrobr.datasets.censo_agropecuario import censo_agropecuario
from agrobr.datasets.censo_agropecuario_historico import censo_agropecuario_historico
from agrobr.datasets.censo_agropecuario_legado import censo_agropecuario_legado
from agrobr.datasets.censo_agropecuario_municipal_1985 import censo_agropecuario_municipal_1985
from agrobr.datasets.clima import clima
from agrobr.datasets.comercio_internacional import comercio_internacional
from agrobr.datasets.comparacao_anual_anec import comparacao_anual_anec
from agrobr.datasets.composicao_defensivos import composicao_defensivos
from agrobr.datasets.condicao_lavouras import condicao_lavouras
from agrobr.datasets.cotacoes_cambio import cotacoes_cambio
from agrobr.datasets.credito_rural import credito_rural
from agrobr.datasets.cultivares_protegidas import cultivares_protegidas
from agrobr.datasets.cultivares_registradas import cultivares_registradas
from agrobr.datasets.custo_producao import custo_producao
from agrobr.datasets.custo_sociobiodiversidade import custo_sociobiodiversidade
from agrobr.datasets.defensivos_formulados import defensivos_formulados
from agrobr.datasets.defensivos_tecnicos import defensivos_tecnicos
from agrobr.datasets.desmatamento import desmatamento
from agrobr.datasets.destinos_anec import destinos_anec
from agrobr.datasets.deterministic import deterministic, get_snapshot, is_deterministic
from agrobr.datasets.embarques_mensais_anec import embarques_mensais_anec
from agrobr.datasets.empregadores_lista_suja import empregadores_lista_suja
from agrobr.datasets.estimativa_safra import estimativa_safra
from agrobr.datasets.expectativas_mercado import expectativas_mercado
from agrobr.datasets.exportacao import exportacao
from agrobr.datasets.exportacao_anec import embarques_anec
from agrobr.datasets.extrativismo_vegetal import extrativismo_vegetal
from agrobr.datasets.fertilizante import fertilizante
from agrobr.datasets.futuros_agricolas import futuros_agricolas
from agrobr.datasets.importacao import importacao
from agrobr.datasets.leite_industrial import leite_industrial
from agrobr.datasets.moedas_cambio import moedas_cambio
from agrobr.datasets.movimentacao_portuaria import movimentacao_portuaria
from agrobr.datasets.oferta_demanda_global import oferta_demanda_global
from agrobr.datasets.pecuaria_municipal import pecuaria_municipal
from agrobr.datasets.pib_agro import pib_agro
from agrobr.datasets.posicionamento_fundos import posicionamento_fundos
from agrobr.datasets.preco_atacado import preco_atacado
from agrobr.datasets.preco_diario import preco_diario
from agrobr.datasets.precos_diesel import precos_diesel
from agrobr.datasets.producao_acucar_etanol import producao_acucar_etanol
from agrobr.datasets.producao_anual import producao_anual
from agrobr.datasets.progresso_safra import progresso_safra
from agrobr.datasets.queimadas import queimadas
from agrobr.datasets.registry import (
    describe,
    describe_all,
    get_dataset,
    info,
    list_datasets,
    list_products,
)
from agrobr.datasets.seguro_rural import seguro_rural
from agrobr.datasets.serie_historica_safra import serie_historica_safra
from agrobr.datasets.series_economicas import series_economicas
from agrobr.datasets.silvicultura import silvicultura
from agrobr.datasets.unidades_conservacao import unidades_conservacao
from agrobr.datasets.unidades_conservacao_federais import unidades_conservacao_federais
from agrobr.datasets.uso_do_solo import uso_do_solo
from agrobr.datasets.zoneamento_agricola import zoneamento_agricola

__all__ = [
    "abate_trimestral",
    "autorizacoes_defensivos",
    "balanco",
    "cadastro_rural",
    "censo_agropecuario",
    "censo_agropecuario_historico",
    "censo_agropecuario_legado",
    "censo_agropecuario_municipal_1985",
    "clima",
    "comercio_internacional",
    "comparacao_anual_anec",
    "composicao_defensivos",
    "condicao_lavouras",
    "cotacoes_cambio",
    "credito_rural",
    "cultivares_protegidas",
    "cultivares_registradas",
    "custo_producao",
    "custo_sociobiodiversidade",
    "defensivos_formulados",
    "defensivos_tecnicos",
    "desmatamento",
    "describe",
    "describe_all",
    "destinos_anec",
    "deterministic",
    "embarques_anec",
    "embarques_mensais_anec",
    "empregadores_lista_suja",
    "estimativa_safra",
    "expectativas_mercado",
    "exportacao",
    "extrativismo_vegetal",
    "fertilizante",
    "futuros_agricolas",
    "get_dataset",
    "get_snapshot",
    "importacao",
    "info",
    "is_deterministic",
    "leite_industrial",
    "list_datasets",
    "list_products",
    "moedas_cambio",
    "movimentacao_portuaria",
    "oferta_demanda_global",
    "pecuaria_municipal",
    "pib_agro",
    "posicionamento_fundos",
    "preco_atacado",
    "preco_diario",
    "precos_diesel",
    "producao_acucar_etanol",
    "producao_anual",
    "progresso_safra",
    "queimadas",
    "seguro_rural",
    "serie_historica_safra",
    "series_economicas",
    "silvicultura",
    "unidades_conservacao",
    "unidades_conservacao_federais",
    "uso_do_solo",
    "zoneamento_agricola",
]
