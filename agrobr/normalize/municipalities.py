from __future__ import annotations

import json
import unicodedata
from functools import lru_cache
from pathlib import Path
from typing import Any, TypedDict


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
    for codigo, nome, uf in _load_municipios():
        key = _remover_acentos(str(nome).lower().strip())
        entry: MunicipioInfo = {
            "codigo_ibge": int(codigo),
            "nome": str(nome),
            "uf": str(uf),
        }
        lookup.setdefault(key, []).append(entry)
    return lookup


@lru_cache(maxsize=1)
def _build_codigo_lookup() -> dict[int, MunicipioInfo]:
    result: dict[int, MunicipioInfo] = {}
    for codigo, nome, uf in _load_municipios():
        result[int(codigo)] = {
            "codigo_ibge": int(codigo),
            "nome": str(nome),
            "uf": str(uf),
        }
    return result


def municipio_para_ibge(nome: str, uf: str | None = None) -> int | None:
    """Resolve nome de municipio para codigo IBGE de 7 digitos.

    Busca case-insensitive e accent-insensitive.
    Se houver homonimos em UFs diferentes, use o parametro `uf` para desambiguar.

    Args:
        nome: Nome do municipio (qualquer variante de caixa/acento).
        uf: Sigla UF para desambiguar homonimos (ex: "SP").

    Returns:
        Codigo IBGE de 7 digitos ou None se nao encontrado.
    """
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
    """Resolve codigo IBGE para informacoes do municipio.

    Args:
        codigo: Codigo IBGE de 7 digitos.

    Returns:
        Dict com codigo_ibge, nome, uf ou None se nao encontrado.
    """
    return _build_codigo_lookup().get(codigo)


def buscar_municipios(termo: str, uf: str | None = None, limite: int = 10) -> list[MunicipioInfo]:
    """Busca municipios por termo parcial (accent/case insensitive).

    Args:
        termo: Termo de busca parcial.
        uf: Filtrar por UF (opcional).
        limite: Maximo de resultados.

    Returns:
        Lista de MunicipioInfo ordenada por nome.
    """
    termo_norm = _remover_acentos(termo.lower().strip())
    uf_upper = uf.upper().strip() if uf else None
    results: list[MunicipioInfo] = []

    for key, entries in _build_lookup().items():
        if termo_norm in key:
            for entry in entries:
                if uf_upper and entry["uf"] != uf_upper:
                    continue
                results.append(entry)

    results.sort(key=lambda m: m["nome"])
    return results[:limite]


def total_municipios() -> int:
    """Retorna total de municipios no cadastro IBGE."""
    return len(_load_municipios())


__all__ = [
    "MunicipioInfo",
    "buscar_municipios",
    "ibge_para_municipio",
    "municipio_para_ibge",
    "total_municipios",
]
