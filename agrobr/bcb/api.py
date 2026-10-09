from __future__ import annotations

import hashlib
import json
import time
import warnings
from typing import Any, Literal, overload

import pandas as pd

from agrobr import _log, contracts
from agrobr.contracts import bcb_sicor
from agrobr.exceptions import InvalidParameterError, ParseError
from agrobr.models import MetaInfo
from agrobr.normalize import dates, regions
from agrobr.utils import time as time_utils
from agrobr.utils.result import (
    ATRIBUTO_AVISOS,
    DataFrame,
    DataFrameResult,
    build_source_meta,
    finalize_result,
)
from agrobr.utils.validation import validate_uf

from . import client
from .models import (
    SICOR_TOTAL_FINALIDADES,
    UF_CODES,
    normalize_produto_sicor,
    normalize_safra_sicor,
    resolve_produto_sicor,
)
from .parser import (
    PARSER_VERSION,
    _inteiros,
    agregar_por_programa,
    agregar_por_uf,
    nomear_dimensoes_do_registro,
    parse_credito_rural,
    parse_credito_rural_total,
)

logger = _log.get_logger(__name__)

_CREDITO_RURAL_COLUMNS = [
    "safra",
    "produto",
    "uf",
    "finalidade",
    "agregacao",
    "programa",
    "cd_programa",
    "qtd_contratos",
    "valor",
    "area_financiada",
    "fonte",
]
_SEM_BIGQUERY = (
    "a tabela da Base dos Dados agrega por município e não traz programa, subprograma, fonte de "
    "recursos, tipo de seguro, modalidade nem atividade; o fallback só vale para agregacao='uf' "
    "sem programa e sem tipo_seguro"
)
_REGISTRO = bcb_sicor.BCB_CREDITO_RURAL_REGISTRO_V1
_TEXTO_DO_REGISTRO = tuple(
    coluna.name for coluna in _REGISTRO.columns if coluna.type == contracts.ColumnType.STRING
)


def _marcar_safra_em_curso(registros: pd.DataFrame, df: pd.DataFrame) -> dict[str, Any]:
    """A safra de julho a junho que contém hoje ainda recebe contratos: o total dela muda.

    Os meses cobertos saem dos registros antes da agregação; o aviso vai para `df.attrs`, de
    onde o `build_source_meta` o leva ao `MetaInfo`, como no mês parcial das queimadas.
    """
    hoje = time_utils.hoje()
    inicio = hoje.year if hoje.month >= dates.INICIO_SAFRA_MES else hoje.year - 1
    safra = dates.anos_para_safra(inicio)
    if "safra" not in df.columns or not df["safra"].eq(safra).any():
        return {}
    da_safra = (
        registros[registros["safra"] == safra]
        if "safra" in registros.columns
        else registros.iloc[:0]
    )
    meses = sorted(
        {
            f"{int(ano):04d}-{int(mes):02d}"
            for ano, mes in zip(
                da_safra.get("ano_emissao", []), da_safra.get("mes_emissao", []), strict=True
            )
            if pd.notna(ano) and pd.notna(mes)
        }
    )
    cobertura = f"de {meses[0]} a {meses[-1]}" if meses else "sem mês de emissão"
    aviso = (
        f"credito_rural: a safra {safra} está em curso (julho a junho); o resultado cobre "
        f"{cobertura} e muda até o fim da safra."
    )
    df.attrs.setdefault(ATRIBUTO_AVISOS, []).append(aviso)
    warnings.warn(aviso, UserWarning, stacklevel=3)
    return {"safra_em_curso": safra, "meses_cobertos": meses}


def _aggregate_credito_rural(
    df: pd.DataFrame,
    agregacao: Literal["uf", "programa"],
    source_used: str,
) -> pd.DataFrame:
    df = agregar_por_uf(df) if agregacao == "uf" else agregar_por_programa(df)

    df["agregacao"] = agregacao
    if "programa" not in df.columns:
        df["programa"] = pd.Series(pd.NA, index=df.index, dtype=pd.Series([""]).dtype)
    if "cd_programa" not in df.columns:
        df["cd_programa"] = pd.Series(pd.NA, index=df.index, dtype=pd.Series([""]).dtype)
    df["fonte"] = f"bcb_{source_used}"
    return df.reindex(columns=_CREDITO_RURAL_COLUMNS).astype(
        contracts.get_contract("credito_rural").empty_frame().dtypes.to_dict()
    )


def _registros_credito_rural(df: pd.DataFrame, source_used: str) -> pd.DataFrame:
    df = nomear_dimensoes_do_registro(df).assign(agregacao="registro", fonte=f"bcb_{source_used}")
    df = (
        df.reindex(columns=_REGISTRO.list_columns())
        .astype(_REGISTRO.empty_frame().dtypes.to_dict())
        .sort_values(_REGISTRO.primary_key, kind="stable")
        .reset_index(drop=True)
    )
    contracts.validate_dataset(df, _REGISTRO)
    return df


@overload
async def credito_rural(
    produto: str,
    safra: str | None = None,
    finalidade: str = "custeio",
    uf: str | None = None,
    agregacao: Literal["uf", "programa", "registro"] = "uf",
    programa: str | None = None,
    tipo_seguro: str | None = None,
    *,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def credito_rural(
    produto: str,
    safra: str | None = None,
    finalidade: str = "custeio",
    uf: str | None = None,
    agregacao: Literal["uf", "programa", "registro"] = "uf",
    programa: str | None = None,
    tipo_seguro: str | None = None,
    *,
    as_polars: bool = False,
    return_meta: Literal[False] = False,
) -> DataFrame: ...


@overload
async def credito_rural(
    produto: str,
    safra: str | None = None,
    finalidade: str = "custeio",
    uf: str | None = None,
    agregacao: Literal["uf", "programa", "registro"] = "uf",
    programa: str | None = None,
    tipo_seguro: str | None = None,
    *,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def credito_rural(
    produto: str,
    safra: str | None = None,
    finalidade: str = "custeio",
    uf: str | None = None,
    agregacao: Literal["uf", "programa", "registro"] = "uf",
    programa: str | None = None,
    tipo_seguro: str | None = None,
    *,
    as_polars: bool = False,
    return_meta: Literal[True],
) -> tuple[DataFrame, MetaInfo]: ...


@overload
async def credito_rural(
    produto: str,
    safra: str | None = None,
    finalidade: str = "custeio",
    uf: str | None = None,
    agregacao: Literal["uf", "programa", "registro"] = "uf",
    programa: str | None = None,
    tipo_seguro: str | None = None,
    *,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult: ...


async def credito_rural(
    produto: str,
    safra: str | None = None,
    finalidade: str = "custeio",
    uf: str | None = None,
    agregacao: Literal["uf", "programa", "registro"] = "uf",
    programa: str | None = None,
    tipo_seguro: str | None = None,
    *,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult:
    """Crédito rural por produto e finalidade do SICOR.

    Custeio usa produtos agrícolas; investimento usa itens de investimento,
    como BOVINOS, CAFÉ, CANA-DE-AÇUCAR, BANANA e tratores. Soja e milho podem
    não ter registros nessa finalidade. Filtros válidos sem registros retornam
    um DataFrame vazio com o esquema de crédito rural.
    """
    t0 = time.monotonic()

    if not isinstance(as_polars, bool) or not isinstance(return_meta, bool):
        raise InvalidParameterError("as_polars e return_meta devem ser booleanos")
    if isinstance(finalidade, str):
        finalidade = regions.remover_acentos(finalidade.strip()).casefold()
    for nome, filtro in (("programa", programa), ("tipo_seguro", tipo_seguro)):
        if filtro is not None and (not isinstance(filtro, str) or not filtro.strip()):
            raise InvalidParameterError(f"{nome} deve ser texto não vazio")
    if not isinstance(agregacao, str) or agregacao not in {"uf", "programa", "registro"}:
        hint = (
            "Use agregacao='uf', 'programa' ou 'registro'. O SICOR publica município por produto "
            "(CusteioMunicipioProduto e InvestMunicipioProduto), que o agrobr ainda não lê; "
            "o extra agrobr[bigquery] traz dados municipais."
        )
        raise InvalidParameterError(f"agregacao inválida: {agregacao!r}. {hint}")
    if str(finalidade).lower() == "industrializacao":
        raise InvalidParameterError(
            "O SICOR não publica a industrialização por produto; use "
            "bcb.credito_rural_total(finalidade='industrializacao'), com o total por UF"
        )
    if str(finalidade).lower() not in client.ENDPOINT_MAP:
        raise InvalidParameterError(
            f"Finalidade inválida: {finalidade!r}. Opções: {list(client.ENDPOINT_MAP)}"
        )

    produto_sicor = resolve_produto_sicor(produto)
    safra_sicor = normalize_safra_sicor(safra) if safra is not None else None
    uf = validate_uf(uf)
    cd_uf = UF_CODES[uf] if uf else None

    logger.info(
        "bcb_credito_rural_request",
        produto=produto,
        produto_sicor=produto_sicor,
        safra=safra,
        safra_sicor=safra_sicor,
        finalidade=finalidade,
        uf=uf,
        programa=programa,
        tipo_seguro=tipo_seguro,
    )

    with client.registrar_aquisicao() as aquisicao:
        dados, source_used = await client.fetch_credito_rural_with_fallback(
            finalidade=finalidade,
            produto_sicor=produto_sicor,
            safra_sicor=safra_sicor,
            cd_uf=cd_uf,
            sem_fallback=_SEM_BIGQUERY if agregacao != "uf" or programa or tipo_seguro else None,
        )

    fetch_ms = int((time.monotonic() - t0) * 1000)

    attempted_sources = ["bcb_odata"]
    if source_used == "bigquery":
        attempted_sources.append("bcb_bigquery")

    t1 = time.monotonic()
    df = parse_credito_rural(dados, finalidade=finalidade)
    df["produto"] = normalize_produto_sicor(produto)

    for coluna, filtro in (("uf", uf), ("programa", programa), ("tipo_seguro", tipo_seguro)):
        if filtro and not df.empty and coluna not in df.columns:
            raise ParseError(
                source="bcb",
                parser_version=PARSER_VERSION,
                reason=f"Corpo sem a coluna {coluna}; o filtro {coluna}={filtro!r} não pode ser aplicado",
            )

    if uf and "uf" in df.columns:
        df = df[df["uf"] == uf].reset_index(drop=True)

    if programa and "programa" in df.columns:
        df = df[df["programa"].str.lower() == programa.lower()].reset_index(drop=True)

    if tipo_seguro and "tipo_seguro" in df.columns:
        df = df[df["tipo_seguro"].str.lower() == tipo_seguro.lower()].reset_index(drop=True)

    registros = df
    if agregacao == "registro":
        df = _registros_credito_rural(df, source_used)
    else:
        df = _aggregate_credito_rural(df, agregacao, source_used)
    parcial = _marcar_safra_em_curso(registros, df)

    parse_ms = int((time.monotonic() - t1) * 1000)

    source_method = "httpx" if source_used == "odata" else "bigquery"

    logger.info(
        "bcb_credito_rural_ok",
        produto=produto,
        safra=safra,
        records=len(df),
        source_used=source_used,
    )

    odata = source_used == "odata" and bool(aquisicao.paginas)
    meta = build_source_meta(
        "bcb_credito",
        aquisicao.consulta
        if odata and aquisicao.consulta
        else "https://basedosdados.org/dataset/br-bcb-sicor"
        if source_used == "bigquery"
        else f"{client.BASE_URL}/{client.ENDPOINT_MAP.get(finalidade.lower(), 'CusteioMunicipio')}",
        source_method,
        fetch_ms,
        parse_ms,
        df,
        PARSER_VERSION,
        schema_version=_REGISTRO.version if agregacao == "registro" else "2.0",
        attempted_sources=attempted_sources,
        selected_source=f"bcb_{source_used}",
        **(_proveniencia(aquisicao) if odata else {}),
    )
    if odata:
        _carimbar_aquisicao(meta, aquisicao)
    meta.source_details.update(parcial)
    if agregacao == "registro":
        meta.contract_version = _REGISTRO.version
        meta.source_details["contract"] = _REGISTRO.name
    return finalize_result(
        df,
        meta,
        as_polars=as_polars,
        return_meta=return_meta,
        string_columns=_TEXTO_DO_REGISTRO
        if agregacao == "registro"
        else (
            "safra",
            "produto",
            "uf",
            "finalidade",
            "agregacao",
            "programa",
            "cd_programa",
            "fonte",
        ),
    )


def _proveniencia(aquisicao: client.AquisicaoOData) -> dict[str, Any]:
    """Proveniência das páginas OData: o topo é o manifesto canônico `{query, resources}`, como no Focus."""
    recursos = [
        {**pagina, "fetched_at": pagina["fetched_at"].isoformat()} for pagina in aquisicao.paginas
    ]
    manifesto = json.dumps(
        {"query": aquisicao.consulta, "resources": recursos},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return {
        "raw_content_hash": hashlib.sha256(manifesto).hexdigest(),
        "raw_content_size": len(manifesto),
        "source_details": {
            "query": aquisicao.consulta,
            "resources": recursos,
            "hash_kind": "resource_manifest_sha256",
            "manifest_encoding": "canonical_json_utf8",
            "manifest_fields": ["query", "resources"],
            "resource_bytes": sum(pagina["bytes"] for pagina in aquisicao.paginas),
        },
    }


def _carimbar_aquisicao(meta: MetaInfo, aquisicao: client.AquisicaoOData) -> None:
    meta.fetched_at = max(pagina["fetched_at"] for pagina in aquisicao.paginas)
    meta.fetch_timestamp = meta.fetched_at


@overload
async def credito_rural_total(
    safra: str | None = None,
    finalidade: str | None = None,
    uf: str | None = None,
    agregacao: Literal["uf", "programa"] = "uf",
    *,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def credito_rural_total(
    safra: str | None = None,
    finalidade: str | None = None,
    uf: str | None = None,
    agregacao: Literal["uf", "programa"] = "uf",
    *,
    as_polars: bool = False,
    return_meta: Literal[False] = False,
) -> DataFrame: ...


@overload
async def credito_rural_total(
    safra: str | None = None,
    finalidade: str | None = None,
    uf: str | None = None,
    agregacao: Literal["uf", "programa"] = "uf",
    *,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def credito_rural_total(
    safra: str | None = None,
    finalidade: str | None = None,
    uf: str | None = None,
    agregacao: Literal["uf", "programa"] = "uf",
    *,
    as_polars: bool = False,
    return_meta: Literal[True],
) -> tuple[DataFrame, MetaInfo]: ...


@overload
async def credito_rural_total(
    safra: str | None = None,
    finalidade: str | None = None,
    uf: str | None = None,
    agregacao: Literal["uf", "programa"] = "uf",
    *,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult: ...


async def credito_rural_total(
    safra: str | None = None,
    finalidade: str | None = None,
    uf: str | None = None,
    agregacao: Literal["uf", "programa"] = "uf",
    *,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult:
    """Crédito rural por UF e finalidade, sem produto: a entidade RegiaoUF do SICOR, com as
    quatro finalidades, inclusive a industrialização. O SICOR não publica linha Brasil; o
    total do país é a soma das UFs."""
    if not isinstance(as_polars, bool) or not isinstance(return_meta, bool):
        raise InvalidParameterError("as_polars e return_meta devem ser booleanos")
    if isinstance(finalidade, str):
        finalidade = regions.remover_acentos(finalidade.strip()).casefold()
    if not isinstance(agregacao, str) or agregacao not in {"uf", "programa"}:
        raise InvalidParameterError(
            f"agregacao inválida: {agregacao!r}. Use agregacao='uf' ou 'programa'"
        )
    if finalidade is not None and str(finalidade).lower() not in SICOR_TOTAL_FINALIDADES:
        raise InvalidParameterError(
            f"Finalidade inválida: {finalidade!r}. Opções: {list(SICOR_TOTAL_FINALIDADES)}"
        )
    finalidades = (
        tuple(SICOR_TOTAL_FINALIDADES) if finalidade is None else (str(finalidade).lower(),)
    )
    safra_sicor = normalize_safra_sicor(safra) if safra is not None else None
    uf = validate_uf(uf)
    logger.info(
        "bcb_credito_rural_total_request",
        safra=safra_sicor,
        finalidade=finalidade,
        uf=uf,
        agregacao=agregacao,
    )

    t0 = time.monotonic()
    with client.registrar_aquisicao() as aquisicao:
        dados = await client.fetch_credito_rural_total(safra_sicor, UF_CODES[uf] if uf else None)
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    df = parse_credito_rural_total(dados, finalidades, agregacao)
    contracts.validate_dataset(df, bcb_sicor.BCB_CREDITO_RURAL_TOTAL_V1)
    parse_ms = int((time.monotonic() - t1) * 1000)

    emissao = [
        _inteiros(pd.Series([r.get(campo) for r in dados], name=campo, dtype=object))
        for campo in ("AnoEmissao", "MesEmissao")
    ]
    meses = sorted({(int(ano), int(mes)) for ano, mes in zip(*emissao, strict=True)})
    proveniencia = _proveniencia(aquisicao) if aquisicao.paginas else {"source_details": {}}
    meta = build_source_meta(
        "bcb_credito",
        aquisicao.consulta
        if aquisicao.paginas and aquisicao.consulta
        else f"{client.BASE_URL}/{client.TOTAL_ENDPOINT}",
        "httpx",
        fetch_ms,
        parse_ms,
        df,
        PARSER_VERSION,
        schema_version=bcb_sicor.BCB_CREDITO_RURAL_TOTAL_V1.version,
        attempted_sources=["bcb_odata"],
        selected_source="bcb_odata",
        raw_content_hash=proveniencia.get("raw_content_hash"),
        raw_content_size=proveniencia.get("raw_content_size", 0),
        source_details={
            **proveniencia["source_details"],
            "meses": {
                "primeiro": f"{meses[0][0]}-{meses[0][1]:02d}",
                "ultimo": f"{meses[-1][0]}-{meses[-1][1]:02d}",
                "quantidade": len(meses),
            }
            if meses
            else None,
        },
    )
    if aquisicao.paginas:
        _carimbar_aquisicao(meta, aquisicao)
    return finalize_result(
        df,
        meta,
        as_polars=as_polars,
        return_meta=return_meta,
        string_columns=(
            "safra",
            "uf",
            "finalidade",
            "agregacao",
            "programa",
            "cd_programa",
            "fonte",
        ),
    )
