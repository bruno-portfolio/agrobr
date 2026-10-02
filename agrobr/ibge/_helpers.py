from __future__ import annotations

import re
from collections.abc import Iterable, Sequence

import pandas as pd

from agrobr import contracts
from agrobr.constants import URLS, Fonte
from agrobr.exceptions import InvalidParameterError
from agrobr.ibge import client
from agrobr.models import MetaInfo
from agrobr.normalize import regions
from agrobr.utils.result import ATRIBUTO_AVISOS
from agrobr.utils.time import hoje, utcnow
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


def normalizar_opcao(valor: str, nome: str, validos: Iterable[str]) -> str:
    opcoes = list(validos)
    texto = regions.remover_acentos(valor.strip().lower()) if isinstance(valor, str) else ""
    for opcao in opcoes:
        if texto == regions.remover_acentos(opcao.lower()):
            return opcao
    adjetivo = "inválida" if nome in ("Variável", "Espécie") else "inválido"
    raise InvalidParameterError(f"{nome} {adjetivo}: {valor!r}. Disponíveis: {opcoes}")


def tipar_resultado(
    df: pd.DataFrame, contrato: str, colunas: Sequence[str] | None = None
) -> pd.DataFrame:
    modelo = contracts.get_contract(contrato).empty_frame()
    if colunas is None:
        colunas = [c for c in modelo.columns if c not in ("cod_municipio", "precos")]
    resultado = df.reindex(columns=list(colunas)).copy()
    for coluna in colunas:
        tipo = modelo[coluna].dtype
        if pd.api.types.is_numeric_dtype(tipo):
            resultado[coluna] = pd.to_numeric(resultado[coluna], errors="coerce").astype(tipo)
        else:
            resultado[coluna] = resultado[coluna].astype(tipo)
    return resultado


def _validate_years(
    ano: int | float | str | Sequence[int | float | str] | None,
) -> int | list[int] | None:
    if ano is None:
        return None
    if isinstance(ano, bytes):
        raise InvalidParameterError("ano deve conter anos inteiros")
    if isinstance(ano, Sequence) and not isinstance(ano, str):
        values = ano
        is_sequence = True
        if not values:
            raise InvalidParameterError("ano deve conter pelo menos um ano inteiro")
    else:
        values = [ano]
        is_sequence = False
    current_year = hoje().year
    years: list[int] = []
    for value in values:
        if isinstance(value, (bool, bytes)) or (
            isinstance(value, float) and not value.is_integer()
        ):
            raise InvalidParameterError("ano deve conter anos inteiros")
        try:
            years.append(int(value))
        except (TypeError, ValueError, OverflowError) as exc:
            raise InvalidParameterError("ano deve conter anos inteiros") from exc
    if any(year < 1974 or year > current_year for year in years):
        raise InvalidParameterError(f"ano deve estar entre 1974 e {current_year}")
    return years if is_sequence else years[0]


def resolve_ibge_code(
    uf: str | None,
    nivel: str,
    *,
    nivel_map: dict[str, str] | None = None,
) -> tuple[str, str]:
    if nivel_map is None:
        nivel_map = NIVEL_MAP
    nivel = normalizar_opcao(nivel, "nível", nivel_map)
    uf = validate_uf(uf)
    if uf and nivel not in ("uf", "municipio"):
        finos = " ou ".join(repr(n) for n in ("uf", "municipio") if n in nivel_map)
        raise InvalidParameterError(
            f"uf={uf!r} só filtra com nivel={finos}; com nivel={nivel!r}, "
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
    if isinstance(value, Sequence) and not isinstance(value, str):
        if not value:
            raise InvalidParameterError("ano deve conter pelo menos um período")
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
    if meta is not None:
        meta.validation_warnings.extend(df.attrs.get(ATRIBUTO_AVISOS, []))
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
