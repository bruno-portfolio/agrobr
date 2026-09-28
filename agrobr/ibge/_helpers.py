from __future__ import annotations

import re
from collections.abc import Sequence

import pandas as pd

from agrobr.constants import URLS, Fonte
from agrobr.exceptions import InvalidParameterError
from agrobr.ibge import client
from agrobr.models import MetaInfo
from agrobr.utils.time import utcnow
from agrobr.utils.validation import validate_uf

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
        raise InvalidParameterError(f"nível inválido: {nivel!r}. Use um de: {sorted(nivel_map)}")
    uf = validate_uf(uf)
    if uf and nivel not in ("uf", "municipio"):
        raise InvalidParameterError(
            f"uf={uf!r} só filtra com nivel='uf' ou 'municipio'; com nivel={nivel!r}, "
            "a consulta devolveria o agregado sem o filtro"
        )
    territorial_level = nivel_map[nivel]
    ibge_code = "all"
    if uf:
        uf_ibge = client.uf_to_ibge_code(uf)
        ibge_code = f"in N3 {uf_ibge}" if nivel == "municipio" else uf_ibge
    return territorial_level, ibge_code


def resolve_period(value: int | str | Sequence[int | str] | None) -> str:
    if value is None:
        return "last"
    if isinstance(value, list):
        return ",".join(str(v) for v in value)
    return str(value)


def resolve_quarter_period(value: str | Sequence[str] | None) -> str:
    if value is None:
        return "last"
    if isinstance(value, Sequence) and not isinstance(value, str):
        if not value:
            raise InvalidParameterError("trimestre deve conter pelo menos um período")
        return ",".join(resolve_quarter_period(item) for item in value)
    if not isinstance(value, str):
        raise InvalidParameterError("trimestre deve ser uma string no formato AAAA0T")
    text = value.strip().upper()
    if text == "ALL" or re.fullmatch(r"LAST(?:\s+\d+)?", text):
        return text.lower()
    match = re.fullmatch(r"(\d{4})(?:0|[-/]T?|[TQ])([1-4])", text)
    if not match:
        raise InvalidParameterError(
            f"Trimestre inválido: {value!r}. Use AAAA0T ou AAAA-T (T entre 1 e 4)"
        )
    return f"{match.group(1)}0{match.group(2)}"


CANAL_FALLBACK = "ibge_servicodados"


def registrar_canal(meta: MetaInfo | None, df: pd.DataFrame) -> None:
    """Registra no MetaInfo o canal de cada consulta SIDRA/agregados. Quando alguma
    consulta usou o fallback, ``attempted_sources`` ganha ``ibge_servicodados`` e
    ``selected_source`` passa a ser esse canal; ``source_details["consultas"]``
    guarda canal, URL e, na SIDRA, SHA-256 e bytes de cada corpo.

    Com uma consulta, ``source_url``, ``raw_content_hash`` e ``raw_content_size`` são os
    dela; com mais de uma, ``source_url`` volta à página em ``source_details["pagina"]`` e
    hash e tamanho ficam só em cada consulta."""
    canal = df.attrs.get("canal")
    if meta is None or not canal:
        return
    consulta = {"canal": canal, "url": df.attrs.get("url", "")}
    campos = ("sha256", "bytes", "tabela", "periodos_modificacao", "periodos_modificacao_erro")
    consulta.update({chave: df.attrs[chave] for chave in campos if chave in df.attrs})
    if "periodos_modificacao" in df.attrs:
        por_tabela = meta.source_details.setdefault("periodos_modificacao", {})
        por_tabela.setdefault(df.attrs["tabela"], {}).update(df.attrs["periodos_modificacao"])
    consultas = meta.source_details.setdefault("consultas", [])
    consultas.extend({**consulta, **fatia} for fatia in df.attrs.get("fatias") or [{}])
    pagina = meta.source_details.setdefault("pagina", meta.source_url)
    unica = consultas[0] if len(consultas) == 1 else {}
    meta.source_url = unica.get("url", pagina)
    meta.raw_content_hash = unica.get("sha256")
    meta.raw_content_size = unica.get("bytes", 0)
    meta.fetch_timestamp = utcnow()
    meta.source_details["canal"] = canal if len({c["canal"] for c in consultas}) == 1 else "misto"
    if canal != "sidra":
        if CANAL_FALLBACK not in meta.attempted_sources:
            meta.attempted_sources = [*meta.attempted_sources, CANAL_FALLBACK]
        meta.selected_source = CANAL_FALLBACK
