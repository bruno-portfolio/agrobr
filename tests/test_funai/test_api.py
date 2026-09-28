from __future__ import annotations

from pathlib import Path

import pytest

from agrobr.exceptions import ContractViolationError
from agrobr.funai import api, client
from tests.helpers import funai_features, install_funai_wfs

GOLDEN = Path(__file__).parents[1] / "golden_data/funai/official_20260907"
ALIASES = {
    "codigo": "terrai_codigo",
    "nome": "terrai_nome",
    "etnia": "etnia_nome",
    "municipio": "municipio_nome",
    "uf": "uf_sigla",
    "area_ha": "superficie_perimetro_ha",
    "fase": "fase_ti",
    "modalidade": "modalidade_ti",
    "data_atualizacao": "data_atualizacao",
    "feature_id": None,
    "gid": "gid",
    "reestudo_ti": "reestudo_ti",
    "cr": "cr",
    "faixa_fronteira": "faixa_fronteira",
    "undadm_codigo": "undadm_codigo",
    "undadm_nome": "undadm_nome",
    "undadm_sigla": "undadm_sigla",
    "dominio_uniao": "dominio_uniao",
    "epsg": "epsg",
}
INTEGERS = ("codigo", "gid", "undadm_codigo", "epsg")


@pytest.mark.parametrize("return_meta", [False, True])
@pytest.mark.parametrize("empty", [False, True])
async def test_funai_contract_mandatory_without_meta_and_when_empty(
    monkeypatch, return_meta, empty
):
    install_funai_wfs(monkeypatch, [] if empty else funai_features())
    fetch = client.fetch_acquisition

    async def invalid_frame(query):
        acquired = await fetch(query)
        acquired.frame["area_ha"] = acquired.frame["area_ha"].astype(object)
        return acquired

    monkeypatch.setattr(client, "fetch_acquisition", invalid_frame)
    with pytest.raises(ContractViolationError):
        await api.terras_indigenas(return_meta=return_meta)
