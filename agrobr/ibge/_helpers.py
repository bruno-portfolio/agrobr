from __future__ import annotations

from collections.abc import Sequence

from agrobr.constants import URLS, Fonte
from agrobr.ibge import client

SIDRA_BASE = URLS[Fonte.IBGE]["base"]

NIVEL_MAP: dict[str, str] = {
    "brasil": "1",
    "uf": "3",
    "municipio": "6",
}

NIVEL_MAP_HISTORICO: dict[str, str] = {
    "brasil": "1",
    "regiao": "2",
    "uf": "3",
}


def resolve_ibge_code(
    uf: str | None,
    nivel: str,
    *,
    nivel_map: dict[str, str] | None = None,
) -> tuple[str, str]:
    if nivel_map is None:
        nivel_map = NIVEL_MAP
    if nivel not in nivel_map:
        raise ValueError(f"nível inválido: {nivel!r}. Use um de: {sorted(nivel_map)}")
    territorial_level = nivel_map[nivel]
    ibge_code = "all"
    if uf:
        uf_ibge = client.uf_to_ibge_code(uf)
        if nivel == "municipio":
            ibge_code = f"in N3 {uf_ibge}"
        elif nivel == "uf":
            ibge_code = uf_ibge
    return territorial_level, ibge_code


def resolve_period(value: int | str | Sequence[int | str] | None) -> str:
    if value is None:
        return "last"
    if isinstance(value, list):
        return ",".join(str(v) for v in value)
    return str(value)
