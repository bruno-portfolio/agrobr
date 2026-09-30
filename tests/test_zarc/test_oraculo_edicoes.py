from __future__ import annotations

import hashlib
import json
import re
from typing import Any

import pytest

from agrobr.exceptions import InvalidParameterError
from agrobr.zarc import api, cache, models
from tests import helpers
from tests.test_zarc import test_reconciliacao_r11 as r11

GOLDEN = r11.ROOT / "tests/golden_data/zarc/edicoes_20260923"
MANIFEST = json.loads((GOLDEN / "manifest.json").read_bytes())
ORACLE = json.loads((GOLDEN / "oracle.json").read_bytes())
RESOURCES = {resource["family"].split(":", 1)[1]: resource for resource in MANIFEST["resources"]}


def servir(monkeypatch: pytest.MonkeyPatch, resource: dict[str, Any]) -> dict[str, list[str]]:
    corpo = GOLDEN / resource["derived_file"]
    assert hashlib.sha256(corpo.read_bytes()).hexdigest() == resource["derived_sha256"]
    cache.clear()
    return helpers.install_replay_http(
        monkeypatch,
        {
            "requests": [
                r11._request(r11.CATALOG_URL, GOLDEN / "catalogo.json", "application/json"),
                r11._request(resource["original"]["requested_url"], corpo, "text/csv"),
            ]
        },
        r11.ROOT,
    )


@pytest.mark.parametrize("case", MANIFEST["cases"], ids=lambda case: case["id"])
async def test_edicoes_intermediarias_publicam_o_corpo_oficial(
    monkeypatch: pytest.MonkeyPatch, case: dict[str, Any]
):
    resource = RESOURCES[case["resource"]]
    seen = servir(monkeypatch, resource)
    with helpers.sem_excecao():
        frame, meta = await api.zoneamento(**r11.consulta(case), use_cache=False, return_meta=True)
    r11.assert_values(
        frame, [ORACLE[case["resource"]][p - 1] for p in case["expected_derived_positions"]]
    )
    assert meta.raw_content_hash == resource["derived_sha256"]
    assert meta.source_details["parser"]["validated_rows"] == len(
        resource["selected_original_records"]
    )
    helpers.assert_replay_served(seen)


def test_rotulos_das_tabuas_oficiais_formam_o_catalogo():
    decisoes = {
        **r11.MANIFEST["alias_decisions"],
        **MANIFEST["alias_decisions"],
        **MANIFEST["alias_decisions_perene_integral_r11"],
    }
    observados = {
        rotulo
        for recurso in [*r11.MANIFEST["resources"], *MANIFEST["resources"]]
        for rotulo in recurso["domains"]["Nome_cultura"]
    }
    assert set(decisoes) == observados == set(models.CULTURAS_ZARC)
    assert {rotulo: models.normalize_cultura(rotulo) for rotulo in sorted(observados)} == {
        rotulo: decisoes[rotulo] for rotulo in sorted(observados)
    }
    assert api.culturas() == sorted(set(decisoes.values()))


@pytest.mark.parametrize(
    ("cultura", "familia"),
    [
        ("milho", "2025_2026"),
        ("milho_1", "2020_2021"),
        ("trigo", "2019_2020"),
        ("trigo", "perene"),
        ("cafe_arabica", "2019_2020"),
    ],
)
async def test_cultura_ausente_da_edicao_indica_as_safras_publicadas(
    monkeypatch: pytest.MonkeyPatch, cultura: str, familia: str
):
    safras = MANIFEST["annual_editions_by_culture"].get(cultura, [])
    if familia == "perene":
        safra = "perene"
        seen = r11.install_replay(monkeypatch, r11.RESOURCES["perene"])
    else:
        safra = RESOURCES[familia]["safra"]
        seen = servir(monkeypatch, RESOURCES[familia])
    assert safra not in safras
    if not safras:
        dica = f"{cultura!r} está na tábua perene: use safra='perene'"
    elif len(safras) == 1:
        dica = f"{cultura!r} aparece na safra {safras[0]}"
    else:
        dica = f"{cultura!r} aparece nas safras {safras[0]} a {safras[-1]}"
    with helpers.levanta_exatamente(InvalidParameterError, match=re.escape(dica)):
        await api.zoneamento(produto=cultura, safra=safra, use_cache=False)
    helpers.assert_replay_served(seen)
