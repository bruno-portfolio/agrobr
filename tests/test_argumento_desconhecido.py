from __future__ import annotations

import pytest

from agrobr import (
    abiove,
    acervo_fundiario,
    ana,
    anda,
    b3,
    datasets,
    deral,
    ibama,
    icmbio,
    inmet,
    mapbiomas_alerta,
    queimadas,
    usda,
)
from agrobr.alt import sicar
from agrobr.conab import ceasa, progresso
from tests import helpers

FUNCOES = [
    abiove.exportacao,
    acervo_fundiario.sigef,
    acervo_fundiario.sigef_geo,
    acervo_fundiario.snci,
    acervo_fundiario.snci_geo,
    acervo_fundiario.assentamentos,
    acervo_fundiario.assentamentos_geo,
    sicar.imoveis,
    sicar.imoveis_geo,
    sicar.resumo,
    ana.hidrografia,
    ana.hidrografia_geo,
    ana.pivos_irrigacao,
    ana.pivos_irrigacao_geo,
    ana.demanda_irrigacao,
    ana.demanda_irrigacao_geo,
    ana.disponibilidade_hidrica,
    ana.disponibilidade_hidrica_geo,
    anda.entregas,
    b3.ajustes,
    b3.historico,
    b3.posicoes_abertas,
    b3.oi_historico,
    ceasa.precos,
    progresso.progresso_safra,
    datasets.condicao_lavouras,
    deral.condicao_lavouras,
    ibama.embargos,
    ibama.embargos_geo,
    icmbio.ucs_geo,
    inmet.estacao,
    inmet.clima_uf,
    inmet.historico,
    mapbiomas_alerta.alertas,
    mapbiomas_alerta.alertas_geo,
    queimadas.focos,
    queimadas.focos_geo,
    usda.psd,
]


@pytest.mark.parametrize(
    "funcao",
    FUNCOES,
    ids=lambda funcao: f"{funcao.__module__.removeprefix('agrobr.')}.{funcao.__name__}",
)
async def test_argumento_desconhecido_recusado_antes_da_rede(funcao):
    with helpers.levanta_exatamente(
        TypeError, match="unexpected keyword argument 'argumento_inexistente'"
    ):
        await funcao(argumento_inexistente=1)
