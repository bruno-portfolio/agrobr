from __future__ import annotations

from typing import Any

import pandas as pd
import pytest

from agrobr import datasets
from agrobr.exceptions import ContractViolationError, SourceUnavailableError

LIVE_CASES: dict[str, tuple[tuple[Any, ...], dict[str, Any]]] = {
    "cotacoes_cambio": ((), {"data": "04/09/2026"}),
    "moedas_cambio": ((), {"top": 3}),
    "expectativas_mercado": (
        (),
        {"inicio": "2026-08-24", "top": 6, "max_registros": 6},
    ),
    "abate_trimestral": (("bovino",), {"trimestre": "202303", "uf": "AC"}),
    "autorizacoes_defensivos": ((), {"nr_registro": "08725"}),
    "balanco": (("soja",), {}),
    "cadastro_rural": (("DF",), {"criado_apos": "2025-01-01"}),
    "censo_agropecuario": (("efetivo_rebanho",), {"uf": "AC"}),
    "censo_agropecuario_historico": (("estabelecimentos_area",), {"uf": "AC"}),
    "censo_agropecuario_legado": (("tecnologia",), {}),
    "censo_agropecuario_municipal_1985": (
        ("assistencia_tecnica",),
        {"uf": "AC"},
    ),
    "clima": ((), {"uf": "DF", "ano": 2023}),
    "comercio_internacional": (("soja",), {"periodo": "2023"}),
    "comparacao_anual_anec": (("soybean",), {"ano": 2026, "semana": 13, "use_cache": False}),
    "composicao_defensivos": ((), {"tipo": "tecnicos", "nr_registro": "00301"}),
    "condicao_lavouras": (("soja",), {}),
    "credito_rural": (
        ("soja",),
        {"safra": "2023/24", "uf": "MT", "agregacao": "uf"},
    ),
    "cultivares_protegidas": ((), {"nr_processo": "21806.000202/2014"}),
    "cultivares_registradas": ((), {"nr_registro": "42039"}),
    "custo_producao": (
        ("soja",),
        {
            "uf": "BA",
            "planilha": "serie-historica-custos-soja-1997-a-2025.xls",
            "aba": "Barreiras-BA-2025",
        },
    ),
    "defensivos_formulados": ((), {"nr_registro": "08725"}),
    "custo_sociobiodiversidade": (("acai",), {"uf": "AM", "ano": 2024}),
    "defensivos_tecnicos": ((), {"nr_registro": "00301"}),
    "desmatamento": (
        ("Cerrado",),
        {"tipo": "prodes", "ano": 2023, "uf": "DF"},
    ),
    "embarques_anec": (("soybean",), {"ano": 2026, "use_cache": False}),
    "embarques_mensais_anec": (("soybean",), {"ano": 2026, "semana": 13, "use_cache": False}),
    "empregadores_lista_suja": ((), {"id_registro": "1", "formato": "csv"}),
    "destinos_anec": (("soybean",), {"ano": 2026, "semana": 13, "use_cache": False}),
    "estimativa_safra": (("soja",), {"safra": "2023/24", "uf": "MT"}),
    "exportacao": (("soja",), {"ano": 2023, "uf": "MT"}),
    "extrativismo_vegetal": (("acai",), {"ano": 2023, "uf": "AC"}),
    "fertilizante": (("total",), {"ano": 2023}),
    "futuros_agricolas": (("boi",), {"data": "2025-03-05"}),
    "importacao": (("soja",), {"ano": 2023, "uf": "PR"}),
    "leite_industrial": (("leite",), {"trimestre": "202303", "uf": "AC"}),
    "movimentacao_portuaria": (("soja",), {"ano": 2024}),
    "oferta_demanda_global": (("soja",), {"ano_comercial": 2023}),
    "pecuaria_municipal": (("bovino",), {"ano": 2023, "uf": "AC"}),
    "pib_agro": (("agropecuaria",), {"trimestre": "202303"}),
    "posicionamento_fundos": (
        ("soja",),
        {"inicio": "2025-01-07", "fim": "2025-01-14"},
    ),
    "preco_atacado": (("TOMATE",), {"ceasa": "CEAGESP - SAO PAULO"}),
    "preco_diario": (("soja",), {}),
    "producao_anual": (("soja",), {"ano": 2023, "uf": "AC"}),
    "progresso_safra": (
        ("milho_2",),
        {
            "uf": "MT",
            "semana_url": (
                "https://www.gov.br/conab/pt-br/atuacao/informacoes-agropecuarias/safras/"
                "progresso-de-safra/acompanhamento-das-lavouras-24-08-a-30-08-26/"
                "acompanhamento-das-lavouras-24-08-a-30-08-26"
            ),
        },
    ),
    "queimadas": (("Cerrado",), {"ano": 2025, "mes": 1, "uf": "MT"}),
    "seguro_rural": (("soja",), {"ano": 2023, "uf": "MT"}),
    "serie_historica_safra": (
        ("soja",),
        {"ano_inicio": 2023, "ano_fim": 2023, "uf": "MT"},
    ),
    "series_economicas": (
        (),
        {"codigo": 1, "inicio": "01/01/2024", "fim": "10/01/2024"},
    ),
    "silvicultura": (("carvao",), {"ano": 2023, "uf": "AC"}),
    "unidades_conservacao_federais": ((), {"uf": "MT", "grupo": "PI", "bioma": "Cerrado"}),
    "precos_diesel": ((), {"nivel": "brasil", "inicio": "2026-08-01", "fim": "2026-08-31"}),
    "uso_do_solo": (
        ("cobertura",),
        {"bioma": "Cerrado", "uf": "DF", "ano": 2022, "classe_id": 39},
    ),
    "zoneamento_agricola": (
        ("SOJA",),
        {"uf": "MT", "safra": "2025/2026"},
    ),
}


def test_live_cases_cover_registry():
    assert set(LIVE_CASES) == set(datasets.list_datasets())


@pytest.mark.integration
@pytest.mark.timeout(300)
@pytest.mark.parametrize("dataset_name", datasets.list_datasets())
async def test_dataset_live_satisfies_contract(
    dataset_name: str, record_property, apply_live_policy
):
    args, kwargs = LIVE_CASES[dataset_name]
    apply_live_policy(dataset_name, kwargs)

    try:
        result, meta = await datasets.get_dataset(dataset_name).fetch(
            *args, **kwargs, return_meta=True
        )
    except ContractViolationError as exc:
        pytest.fail(
            f"Dataset {dataset_name!r} violou o próprio contrato: {exc}",
            pytrace=False,
        )
    except SourceUnavailableError as exc:
        pytest.fail(
            f"Indisponibilidade inesperada para {dataset_name!r}: {exc}",
            pytrace=False,
        )

    assert isinstance(result, pd.DataFrame)
    record_property("selected_source", meta.selected_source)
    record_property("attempted_sources", ",".join(meta.attempted_sources))
    record_property("records_count", len(result))
    assert not result.empty, f"A consulta de referência de {dataset_name!r} não retornou registros"
    assert meta.records_count == len(result)
    assert meta.columns == result.columns.tolist()
    assert meta.selected_source
    record_property("live_matrix_status", "validated")
