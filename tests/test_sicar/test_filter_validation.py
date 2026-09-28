from __future__ import annotations

from typing import Any

import pytest

from agrobr.alt.sicar import api, client
from agrobr.exceptions import InvalidParameterError
from tests.helpers import collect_failures

GEOGRAFICOS: list[tuple[Any, dict[str, Any]]] = [
    (None, {}),
    (True, {}),
    (0, {}),
    ("", {}),
    (" ", {}),
    ("XX", {}),
    ("MT", {"cod_municipio": True}),
    ("MT", {"cod_municipio": 5107925.0}),
    ("MT", {"cod_municipio": "5107925"}),
    ("MT", {"cod_municipio": 510792}),
    ("MT", {"cod_municipio": 51079250}),
    ("MT", {"cod_municipio": -5107925}),
    ("MT", {"cod_municipio": 5300108}),
    ("MT", {"municipio": 123}),
    ("MT", {"municipio": " "}),
    ("MT", {"municipio": "Sorriso", "cod_municipio": 5107925}),
]
FILTROS: list[tuple[str, dict[str, Any]]] = [
    ("MT", {"status": True}),
    ("MT", {"status": " AT "}),
    ("MT", {"status": "INVALIDO"}),
    ("MT", {"tipo": 1}),
    ("MT", {"tipo": ""}),
    ("MT", {"tipo": "XYZ"}),
    ("MT", {"area_min": True}),
    ("MT", {"area_max": False}),
    ("MT", {"area_min": "10"}),
    ("MT", {"area_min": -1}),
    ("MT", {"area_max": -1}),
    ("MT", {"area_min": float("nan")}),
    ("MT", {"area_max": float("inf")}),
    ("MT", {"area_min": float("-inf")}),
    ("MT", {"area_min": 10**400}),
    ("MT", {"area_min": 10, "area_max": 9}),
    ("MT", {"criado_apos": True}),
    ("MT", {"criado_apos": ""}),
    ("MT", {"criado_apos": "2026-02-29"}),
    ("MT", {"criado_apos": "2026-04-31"}),
    ("MT", {"atualizado_apos": 20260101}),
    ("MT", {"atualizado_apos": "2026-02-30T12:30:00"}),
    ("MT", {"atualizado_apos": "2026-01-01T24:00:00"}),
    ("MT", {"atualizado_apos": "2026-01-01T23:60:00"}),
    ("MT", {"atualizado_apos": "2026-01-01T23:59:60"}),
    ("MT", {"atualizado_apos": "2026-09-03T14:27:12.2121Z"}),
    ("MT", {"atualizado_apos": "2026-09-03T14:27:12.212001Z"}),
    ("MT", {"atualizado_apos": "2026-09-03T14:27:12.212000001Z"}),
    ("MT", {"atualizado_apos": "2026-09-03T14:27:12.000000000001Z"}),
    ("SP", {"atualizado_apos": "2026-06-07"}),
    ("TO", {"atualizado_apos": "2026-06-07T00:00:00Z"}),
]
LIMITES = [True, False, 0, -1, 1.5, "10"]


def forbid_session() -> None:
    raise AssertionError("A validacao deve ocorrer antes de abrir sessao HTTP")


async def call_api(name: str, uf: Any, filters: dict[str, Any]) -> None:
    if name == "imoveis_geo_stream":
        async for _ in api.imoveis_geo_stream(uf, **filters):
            pass
    else:
        await getattr(api, name)(uf, **filters)


async def test_filtros_invalidos_recusados_antes_da_rede(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(client, "make_session", forbid_session)
    casos = [
        *(
            (name, uf, filters, None)
            for name in ("imoveis", "imoveis_geo", "imoveis_geo_stream", "resumo")
            for uf, filters in GEOGRAFICOS
        ),
        *(
            (name, uf, filters, None)
            for name in ("imoveis", "imoveis_geo", "imoveis_geo_stream")
            for uf, filters in FILTROS
        ),
        *(("imoveis_geo", "MT", {"max_features": limite}, "max_features") for limite in LIMITES),
    ]
    with collect_failures() as check:
        for name, uf, filters, mensagem in casos:
            with check((name, uf, filters)), pytest.raises(InvalidParameterError, match=mensagem):
                await call_api(name, uf, filters)
