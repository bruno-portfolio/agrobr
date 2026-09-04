from __future__ import annotations

from typing import Any

import pandas as pd
import pytest

from agrobr import datasets
from agrobr.exceptions import ContractViolationError, SourceUnavailableError

LIVE_CASES: dict[str, tuple[tuple[Any, ...], dict[str, Any]]] = {
    "abate_trimestral": (("bovino",), {"trimestre": "202303", "uf": "AC"}),
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
    "condicao_lavouras": (("soja",), {}),
    "credito_rural": (
        ("soja",),
        {"safra": "2023/24", "uf": "MT", "agregacao": "uf"},
    ),
    "custo_producao": (("soja",), {"uf": "MT"}),
    "desmatamento": (
        ("Cerrado",),
        {"tipo": "prodes", "ano": 2023, "uf": "DF"},
    ),
    "embarques_anec": (("soybean",), {"ano": 2026, "use_cache": False}),
    "estimativa_safra": (("soja",), {"safra": "2023/24", "uf": "MT"}),
    "exportacao": (("soja",), {"ano": 2023, "uf": "MT"}),
    "extrativismo_vegetal": (("acai",), {"ano": 2023, "uf": "AC"}),
    "fertilizante": (("total",), {"ano": 2023, "uf": "MT"}),
    "futuros_agricolas": (("boi",), {"data": "2025-03-05"}),
    "importacao": (("soja",), {"ano": 2023, "uf": "MT"}),
    "leite_industrial": (("leite",), {"trimestre": "202303", "uf": "AC"}),
    "movimentacao_portuaria": (("soja",), {"ano": 2024}),
    "oferta_demanda_global": (("soja",), {"market_year": 2023}),
    "pecuaria_municipal": (("bovino",), {"ano": 2023, "uf": "AC"}),
    "pib_agro": (("agropecuaria",), {"trimestre": "202303"}),
    "posicionamento_fundos": (
        ("soja",),
        {"start": "2025-01-07", "end": "2025-01-14"},
    ),
    "preco_atacado": (("TOMATE",), {"ceasa": "CEAGESP"}),
    "preco_diario": (
        ("soja",),
        {"inicio": "2025-01-02", "fim": "2025-01-02"},
    ),
    "producao_anual": (("soja",), {"ano": 2023, "uf": "AC"}),
    "progresso_safra": (("soja",), {"estado": "MT"}),
    "queimadas": (("Cerrado",), {"ano": 2025, "mes": 1, "uf": "MT"}),
    "seguro_rural": (("soja",), {"ano": 2023, "uf": "MT"}),
    "serie_historica_safra": (
        ("soja",),
        {"inicio": 2023, "fim": 2023, "uf": "MT"},
    ),
    "silvicultura": (("carvao",), {"ano": 2023, "uf": "AC"}),
    "uso_do_solo": (
        ("cobertura",),
        {"bioma": "Cerrado", "estado": "DF", "ano": 2022, "classe_id": 39},
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
async def test_dataset_live_satisfies_contract(dataset_name: str):
    args, kwargs = LIVE_CASES[dataset_name]

    try:
        result = await datasets.get_dataset(dataset_name).fetch(*args, **kwargs)
    except ContractViolationError as exc:
        pytest.fail(
            f"Dataset {dataset_name!r} violou o próprio contrato: {exc}",
            pytrace=False,
        )
    except SourceUnavailableError as exc:
        pytest.skip(f"fonte indisponível para {dataset_name!r}: {str(exc)[:200]}")

    assert isinstance(result, pd.DataFrame)
