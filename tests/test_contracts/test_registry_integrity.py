from __future__ import annotations

import pandas as pd
import pytest

from agrobr import contracts
from agrobr.contracts import conab, datasets, ibge


def test_duplicate_registration_preserves_original(monkeypatch):
    original = contracts.Contract("original", "1.0", [])
    monkeypatch.setattr(contracts, "_CONTRACT_REGISTRY", {"sample": original})
    contracts.register_contract("sample", original)
    with pytest.raises(ValueError, match="already registered"):
        contracts.register_contract("sample", contracts.Contract("replacement", "2.0", []))
    assert contracts.get_contract("sample") is original


@pytest.mark.parametrize(
    "module,symbol,key,version",
    [
        (conab, "CONAB_CUSTO_PRODUCAO_V1", "custo_producao", "1.0"),
        (conab, "CONAB_CUSTO_PRODUCAO_V2", "custo_producao", "2.0"),
        (ibge, "IBGE_PAM_V1", "producao_anual", "1.0"),
        (datasets, "ANTT_PEDAGIO_FLUXO_V1", "antt_pedagio_fluxo", "1.0"),
        (datasets, "ANTT_PEDAGIO_FLUXO_V2", "antt_pedagio_fluxo", "2.0"),
        (datasets, "ANP_DIESEL_PRECOS_V1", "anp_diesel_precos", "1.0"),
        (datasets, "COMERCIO_BILATERAL_V1", "comercio_bilateral", "1.0"),
        (datasets, "TRADE_MIRROR_V1", "trade_mirror", "1.0"),
        (datasets, "DESMATAMENTO_PRODES_V1", "desmatamento_prodes", "1.0"),
        (datasets, "DESMATAMENTO_DETER_V1", "desmatamento_deter", "1.0"),
        (datasets, "FERTILIZANTE_V1", "fertilizante", "1.0"),
        (datasets, "IMPORTACAO_V1", "importacao", "1.0"),
        (datasets, "CLIMA_V1", "clima", "1.0"),
        (datasets, "CLIMA_V2", "clima", "2.0"),
        (datasets, "ZONEAMENTO_AGRICOLA_V1", "zoneamento_agricola", "1.0"),
        (datasets, "SICAR_IMOVEIS_V1", "cadastro_rural", "1.0"),
    ],
)
def test_historical_import_preserves_active_contract(module, symbol, key, version):
    active = contracts.get_contract(key)
    with pytest.warns(DeprecationWarning, match="contrato histórico"):
        historical = getattr(module, symbol)
    assert historical.version == version
    assert historical is not active
    assert contracts.get_contract(key) is active
    assert symbol not in module.__all__
    assert not set(map(id, historical.columns)) & set(map(id, active.columns))


@pytest.mark.parametrize(
    "module,symbol,current",
    [
        (conab, "CONAB_SAFRA_V1", "CONAB_SAFRA_V2"),
        (ibge, "IBGE_LSPA_V1", "IBGE_LSPA_V2"),
        (ibge, "IBGE_CENSO_AGRO_LEGADO_V1", "IBGE_CENSO_AGRO_LEGADO_V2"),
    ],
)
def test_alias_supersedido_preserva_destino_e_avisa(module, symbol, current):
    with pytest.warns(DeprecationWarning, match="alias supersedido"):
        alias = getattr(module, symbol)
    assert alias is getattr(module, current)
    assert symbol not in module.__all__


@pytest.mark.parametrize("categories", [[1, 2], [True, False], ["soja", 1]])
def test_string_recusa_categoria_vazia_com_dominio_nao_textual(categories):
    series = pd.Series(pd.Categorical([], categories=categories))
    assert contracts.Column("produto", contracts.ColumnType.STRING).validate(series)


def test_registry_roundtrip_preserves_sorted_names(monkeypatch):
    monkeypatch.setattr(contracts, "_CONTRACT_REGISTRY", {})
    zeta = contracts.Contract("zeta", "1.0", [])
    alpha = contracts.Contract("alpha", "1.0", [])
    contracts.register_contract("zeta", zeta)
    contracts.register_contract("alpha", alpha)
    assert contracts.get_contract("alpha") is alpha
    assert contracts.get_contract("zeta") is zeta
    assert contracts.list_contracts() == ["alpha", "zeta"]
