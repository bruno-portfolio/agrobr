from __future__ import annotations

import json
import unicodedata
from functools import lru_cache
from pathlib import Path
from typing import Any, TypedDict

from agrobr.exceptions import InvalidParameterError
from agrobr.normalize import regions

_NOMES_ANTERIORES: dict[int, tuple[str, ...]] = {
    1400605: ("São Luiz",),
    2400208: ("Açu",),
    2401206: ("Arês",),
}


class MunicipioInfo(TypedDict):
    codigo_ibge: int
    nome: str
    uf: str


def _remover_acentos(texto: str) -> str:
    nfkd = unicodedata.normalize("NFKD", texto)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


@lru_cache(maxsize=1)
def _load_municipios() -> list[Any]:
    path = Path(__file__).parent / "_municipios_ibge.json"
    with path.open(encoding="utf-8") as f:
        return json.load(f)  # type: ignore[no-any-return]


@lru_cache(maxsize=1)
def _build_lookup() -> dict[str, list[MunicipioInfo]]:
    lookup: dict[str, list[MunicipioInfo]] = {}
    for codigo, nome, uf, *_coords in _load_municipios():
        key = _remover_acentos(str(nome).lower().strip())
        entry: MunicipioInfo = {
            "codigo_ibge": int(codigo),
            "nome": str(nome),
            "uf": str(uf),
        }
        lookup.setdefault(key, []).append(entry)
    codigos = _build_codigo_lookup()
    for codigo, nomes in _NOMES_ANTERIORES.items():
        for nome in nomes:
            lookup.setdefault(_remover_acentos(nome.lower()), []).append(codigos[codigo])
    return lookup


@lru_cache(maxsize=1)
def _build_codigo_lookup() -> dict[int, MunicipioInfo]:
    result: dict[int, MunicipioInfo] = {}
    for codigo, nome, uf, *_coords in _load_municipios():
        result[int(codigo)] = {
            "codigo_ibge": int(codigo),
            "nome": str(nome),
            "uf": str(uf),
        }
    return result


def municipio_para_ibge(nome: str, uf: str | None = None) -> int | None:
    key = _remover_acentos(nome.lower().strip())
    lookup = _build_lookup()

    matches = lookup.get(key)
    if not matches:
        return None

    if uf:
        uf_upper = uf.upper().strip()
        for m in matches:
            if m["uf"] == uf_upper:
                return m["codigo_ibge"]
        return None

    return matches[0]["codigo_ibge"]


def ibge_para_municipio(codigo: int) -> MunicipioInfo | None:
    info = _build_codigo_lookup().get(codigo)
    return None if info is None else info.copy()


def buscar_municipios(termo: str, uf: str | None = None, limite: int = 10) -> list[MunicipioInfo]:
    if isinstance(limite, bool) or not isinstance(limite, int) or limite < 0:
        raise InvalidParameterError(f"limite deve ser inteiro não negativo, recebeu {limite!r}")
    uf_upper = None if uf is None else regions.sigla_uf(uf)
    termo_norm = _remover_acentos(termo.lower().strip())
    results: dict[int, MunicipioInfo] = {}

    for key, entries in _build_lookup().items():
        if termo_norm in key:
            for entry in entries:
                if uf_upper and entry["uf"] != uf_upper:
                    continue
                results.setdefault(entry["codigo_ibge"], entry)

    return [m.copy() for m in sorted(results.values(), key=lambda m: m["nome"])[:limite]]


def total_municipios() -> int:
    return len(_load_municipios())


@lru_cache(maxsize=1)
def _build_coord_index() -> tuple[list[float], list[float], list[MunicipioInfo]]:
    lats: list[float] = []
    lons: list[float] = []
    infos: list[MunicipioInfo] = []
    for entry in _load_municipios():
        if len(entry) >= 5:
            lats.append(float(entry[3]))
            lons.append(float(entry[4]))
            infos.append({"codigo_ibge": int(entry[0]), "nome": str(entry[1]), "uf": str(entry[2])})
    return lats, lons, infos


_MAX_DISTANCE_DEG_SQ = 1.5**2


def coordenada_para_municipio(lat: float, lon: float) -> MunicipioInfo | None:
    """Reverse geocode: find the nearest municipality for a (lat, lon) pair.

    Uses brute-force Euclidean distance against ~5570 municipality centroids.
    Returns None if the nearest centroid is more than 1.5 degrees away (~167km).
    """
    idx_lats, idx_lons, idx_infos = _build_coord_index()
    min_dist_sq = float("inf")
    best: MunicipioInfo | None = None
    for elat, elon, info in zip(idx_lats, idx_lons, idx_infos):
        dlat = elat - lat
        dlon = elon - lon
        dist_sq = dlat * dlat + dlon * dlon
        if dist_sq < min_dist_sq:
            min_dist_sq = dist_sq
            best = info
    if best is None or min_dist_sq > _MAX_DISTANCE_DEG_SQ:
        return None
    return best.copy()


__all__ = [
    "MunicipioInfo",
    "buscar_municipios",
    "coordenada_para_municipio",
    "ibge_para_municipio",
    "municipio_para_ibge",
    "total_municipios",
]
