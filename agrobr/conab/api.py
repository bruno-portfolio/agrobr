from __future__ import annotations

import asyncio
import hashlib
import re
import time
from datetime import date
from decimal import Decimal
from io import BytesIO
from typing import Any, Literal, overload

import pandas as pd

from agrobr import _log, constants
from agrobr.cache.keys import build_cache_key
from agrobr.conab import client, models
from agrobr.conab._serie_historica import client as serie_client
from agrobr.conab._serie_historica import parser as serie_parser
from agrobr.conab.parsers.v1 import ConabParserV1
from agrobr.contracts import conab as conab_contract
from agrobr.exceptions import InvalidParameterError, SourceUnavailableError
from agrobr.models import MetaInfo
from agrobr.normalize import crops, regions
from agrobr.normalize.dates import safra_para_anos
from agrobr.utils import result as result_utils
from agrobr.utils.result import build_source_meta, finalize_result
from agrobr.utils.time import hoje
from agrobr.utils.validation import validate_uf
from agrobr.utils.warnings import warn_once

logger = _log.get_logger(__name__)

_SAFRAS_URL = "https://www.conab.gov.br/info-agro/safras/graos"


def _publicacao(metadata: dict[str, Any]) -> dict[str, Any]:
    data = metadata.get("data_publicacao")
    return {
        "levantamento": metadata.get("levantamento"),
        "safra": metadata.get("safra"),
        "data_publicacao": str(data) if data else None,
        "url": metadata.get("url"),
    }


def _balanco_frame(records: list[dict[str, Any]]) -> pd.DataFrame:
    vazio = conab_contract.CONAB_BALANCO_V1_1.empty_frame().drop(columns="fonte")
    if not records:
        return vazio
    frame = pd.DataFrame(records).rename(columns={"suprimento_total": "suprimento"})
    for name in vazio.columns:
        if name not in frame.columns:
            frame[name] = pd.Series(index=frame.index, dtype=vazio[name].dtype)
    for name, dtype in constants.CONAB_BALANCO_DTYPES.items():
        if dtype == "float64":
            frame[name] = pd.to_numeric(frame[name], errors="coerce").astype("float64")
    return frame[vazio.columns]


def _safra_frame(records: list[dict[str, Any]]) -> pd.DataFrame:
    if not records:
        return conab_contract.CONAB_SAFRA_V2.empty_frame()
    frame = pd.DataFrame(records, columns=conab_contract.CONAB_SAFRA_V2.list_columns())
    for coluna in ("area_plantada", "area_colhida", "produtividade", "producao"):
        frame[coluna] = pd.to_numeric(frame[coluna], errors="coerce").astype("float64")
    frame["levantamento"] = frame["levantamento"].astype("Int64")
    frame["data_publicacao"] = pd.to_datetime(frame["data_publicacao"])
    return frame


def _nome_total(rotulo: str) -> str:
    return crops.normalizar_cultura(re.sub(r"\s*\(\d+\)", "", rotulo).replace(" - ", " "))


def _brasil_total_frame(records: list[dict[str, Any]]) -> pd.DataFrame:
    if not records:
        return conab_contract.CONAB_BRASIL_TOTAL_V2.empty_frame()
    produtos = {
        (_nome_total(rotulo), _nome_total(grupo) if grupo else None): produto
        for rotulo, grupo, produto, _ in constants.CONAB_BRASIL_TOTAL_SERIES
    }
    linhas = []
    for registro in records:
        rotulo = registro["produto"]
        grupo = registro["grupo"]
        chave = (_nome_total(rotulo), _nome_total(grupo) if grupo else None)
        linhas.append({**registro, "produto": produtos.get(chave, chave[0]), "rotulo": rotulo})
    frame = pd.DataFrame(linhas, columns=conab_contract.CONAB_BRASIL_TOTAL_V2.list_columns())
    for coluna in ("area_plantada", "produtividade", "producao"):
        frame[coluna] = pd.to_numeric(frame[coluna], errors="coerce").astype("float64")
    return frame


def _edicao(metadata: dict[str, Any]) -> str:
    return f"{metadata.get('levantamento')}º levantamento de {metadata.get('safra')}"


def _avisar_balanco_incoerente(suprimentos: list[dict[str, Any]], metadata: dict[str, Any]) -> None:
    edicao = _edicao(metadata)
    for linha in suprimentos:
        for identidade, somados, subtraidos in constants.CONAB_BALANCO_IDENTIDADES:
            termos = [linha.get(campo) for campo in (*somados, *subtraidos)]
            if any(termo is None for termo in termos):
                continue
            residuo = sum(linha[campo] for campo in somados) - sum(
                linha[campo] for campo in subtraidos
            )
            if abs(residuo) > constants.CONAB_ARREDONDAMENTO * len(termos):
                warn_once(
                    f"conab_balanco_incoerente:{metadata.get('url')}:{linha['produto']}:"
                    f"{linha['safra']}:{identidade}",
                    f"CONAB: no balanço de {linha['produto']} {linha['safra']} ({edicao}), "
                    f"{identidade} = {residuo:.1f} mil t, e não 0; o agrobr repassa os números "
                    "publicados",
                )


def _hoje() -> date:
    return hoje()


def _pode_estar_fora_do_boletim(safra: str, levantamento: int | None) -> bool:
    return levantamento is None and int(safra[:4]) < _hoje().year - 1


def _periodos(safra: str) -> tuple[str, str]:
    return safra, str(safra_para_anos(safra)[1])


async def _baixar_series(rotulos: dict[str, str]) -> dict[str, tuple[str, bytes, dict[str, Any]]]:
    por_url: dict[str, list[str]] = {}
    for produto in rotulos:
        por_url.setdefault(serie_client.get_xls_url(produto), []).append(produto)

    async def baixar(produto: str) -> tuple[str, bytes, dict[str, Any]]:
        xls, metadata = await serie_client.download_xls(produto)
        return produto, xls.getvalue(), metadata

    resultados = await asyncio.gather(
        *(baixar(produtos[0]) for produtos in por_url.values()), return_exceptions=True
    )
    series: dict[str, tuple[str, bytes, dict[str, Any]]] = {}
    falhas = []
    for (url, produtos), resultado in zip(por_url.items(), resultados, strict=True):
        if isinstance(resultado, SourceUnavailableError):
            falhas.append(f"{', '.join(rotulos[p] for p in produtos)} ({resultado.last_error})")
        elif isinstance(resultado, BaseException):
            raise resultado
        else:
            series[url] = resultado
    if falhas:
        raise SourceUnavailableError(
            source="conab_serie_historica",
            url=serie_client.SERIES_HISTORICAS_URL,
            last_error=(
                f"série histórica indisponível para {'; '.join(falhas)}; "
                "use levantamento= para a edição original do boletim"
            ),
        )
    return series


def _serie_mais_nova(
    levantamentos: list[dict[str, Any]],
    safra: str,
    produto: str,
    referencia: tuple[str, date] | None,
) -> bool:
    edicao = client.edicao_da_safra(levantamentos, safra)
    if edicao is None:
        return True
    data = edicao.get("data_publicacao")
    if referencia is not None and data is not None and referencia[1] > data:
        return True
    warn_once(
        f"conab_serie_defasada:{produto}:{safra}",
        f"CONAB: a série histórica de {produto} "
        f"({referencia[0] if referencia else 'sem referência'}) não é posterior ao "
        f"{edicao['levantamento']}º levantamento de {edicao['safra']}"
        f"{f' ({data})' if data else ''}; a safra {safra} sai dessa publicação do boletim",
    )
    return False


def _descricao_serie(
    produto: str, raw: bytes, metadata: dict[str, Any], referencia: tuple[str, date] | None
) -> dict[str, Any]:
    return {
        "produto": produto,
        "url": metadata.get("url"),
        "sha256": hashlib.sha256(raw).hexdigest(),
        "referencia": referencia[0] if referencia else None,
    }


def _rotulo_da_linha(rotulo: str, grupo: str | None) -> str:
    return f"{rotulo} ({grupo})" if grupo else rotulo


async def _ultima_edicao_do_boletim(
    levantamentos: list[dict[str, Any]], safra: str
) -> tuple[dict[str, Any], BytesIO] | None:
    """Só na safra que acabou de sair do boletim, a mais nova servida pela série: a legenda da
    série pode ser posterior à última edição que publicou a safra sem que a coluna tenha sido
    revista."""
    edicao = client.edicao_da_safra(levantamentos, safra)
    if edicao is None or int(safra[:4]) != levantamentos[0]["ano_inicio"] - 2:
        return None
    metadata = dict(edicao)
    return metadata, await client.download_xlsx(metadata["url"], metadata=metadata)


def _conferir_com_o_boletim(
    safra: str,
    edicao: dict[str, Any],
    serie: dict[tuple[str, str], Any],
    boletim: dict[tuple[str, str], Any],
) -> dict[str, Any]:
    divergencias: list[dict[str, Any]] = [
        {
            "linha": linha,
            "coluna": coluna,
            "serie": float(valor),
            "boletim": float(boletim[(linha, coluna)]),
        }
        for (linha, coluna), valor in serie.items()
        if valor is not None
        and boletim.get((linha, coluna)) is not None
        and abs(float(valor) - float(boletim[(linha, coluna)])) > 2 * constants.CONAB_ARREDONDAMENTO
    ]
    if divergencias:
        warn_once(
            f"conab_serie_x_boletim:{safra}:{edicao['url']}:"
            f"{'|'.join(sorted({divergencia['linha'] for divergencia in divergencias}))}",
            f"CONAB: {safra} sai da série histórica, que diverge do {_edicao(edicao)}, a última "
            "edição do boletim que publicou a safra: "
            + "; ".join(
                f"{d['linha']} {d['coluna']} {d['serie']:.1f} na série × {d['boletim']:.1f} no boletim"
                for d in divergencias
            )
            + ". A legenda da série é posterior, mas a coluna pode não ter sido revista",
        )
    return {**_publicacao(edicao), "divergencias": divergencias}


async def _safras_da_serie(
    produto: str, safra: str, uf: str | None
) -> tuple[pd.DataFrame, MetaInfo] | None:
    levantamentos = await client.list_levantamentos()
    if not client.fora_do_boletim(levantamentos, safra):
        return None
    t0 = time.monotonic()
    ((_, raw, metadata),) = (await _baixar_series({produto: produto})).values()
    referencia = serie_parser.referencia_publicacao(raw)
    if not _serie_mais_nova(levantamentos, safra, produto, referencia):
        return None
    ultima = await _ultima_edicao_do_boletim(levantamentos, safra)
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    conferencia = None
    inicio = int(safra[:4])
    registros = [
        registro
        for registro in serie_parser.parse_serie_historica(
            BytesIO(raw), produto, inicio=inicio, fim=inicio + 1, uf=uf
        )
        if registro.uf and registro.safra in _periodos(safra)
    ]
    df = _safra_frame(
        [
            {
                "fonte": constants.Fonte.CONAB,
                "produto": produto.lower(),
                "safra": safra,
                "uf": registro.uf,
                "area_plantada": registro.area_plantada_mil_ha,
                "area_colhida": None,
                "produtividade": registro.produtividade_kg_ha,
                "producao": registro.producao_mil_ton,
                "levantamento": None,
                "data_publicacao": None,
            }
            for registro in registros
        ]
    )
    if not df.empty:
        brasil = serie_parser.linhas_brasil(raw, produto)
        publicado = {
            coluna: brasil[(periodo, campo)]
            for periodo in _periodos(safra)
            for campo, coluna in (
                ("area_plantada_mil_ha", "area_plantada"),
                ("producao_mil_ton", "producao"),
            )
            if (periodo, campo) in brasil
        }
        serie_parser.avisar_soma_das_ufs(
            produto,
            "série histórica",
            df,
            {(safra, coluna): valor for coluna, valor in publicado.items()},
        )
        if ultima is not None:
            edicao, xlsx = ultima
            conferencia = _conferir_com_o_boletim(
                safra,
                edicao,
                {(produto, coluna): valor for coluna, valor in publicado.items()},
                {
                    (produto, coluna): valor
                    for registro in ConabParserV1().parse_safra_produto(
                        xlsx, produto, safra_ref=safra
                    )
                    if registro.meta.get("rotulo") == "BRASIL"
                    for coluna, valor in (
                        ("area_plantada", registro.area_plantada),
                        ("producao", registro.producao),
                    )
                },
            )
    parse_ms = int((time.monotonic() - t1) * 1000)

    meta = build_source_meta(
        "conab",
        metadata.get("url", serie_client.SERIES_HISTORICAS_URL),
        "httpx",
        fetch_ms,
        parse_ms,
        df,
        serie_parser.PARSER_VERSION,
        schema_version="2.0",
        raw_content_hash=hashlib.sha256(raw).hexdigest(),
        raw_content_size=len(raw),
        source_details={
            "publicacao": {
                "origem": "serie_historica",
                "series": [_descricao_serie(produto, raw, metadata, referencia)],
                **({"conferencia": conferencia} if conferencia else {}),
            }
        },
    )
    meta.cache_key = build_cache_key(
        "conab:safras",
        {"produto": produto.lower(), "safra": safra, "publicacao": "serie_historica", "uf": uf},
        schema_version=meta.schema_version,
    )
    return df, meta


def _linha_total(
    produto: str,
    grupo: str | None,
    safra: str,
    area: Decimal | None,
    produtividade: Decimal | None,
    producao: Decimal | None,
) -> dict[str, Any]:
    return {
        "produto": produto,
        "grupo": grupo,
        "safra": safra,
        "area_plantada": area,
        "produtividade": produtividade,
        "producao": producao,
        "unidade_area": "mil_ha",
        "unidade_producao": "mil_ton",
    }


def _subtotal(
    rotulo: str, grupo: str | None, safra: str, linhas: list[dict[str, Any]]
) -> dict[str, Any]:
    area = sum(
        (linha["area_plantada"] for linha in linhas if linha["area_plantada"] is not None),
        Decimal(0),
    )
    producao = sum(
        (linha["producao"] for linha in linhas if linha["producao"] is not None), Decimal(0)
    )
    return _linha_total(
        rotulo, grupo, safra, area, producao * 1000 / area if area else None, producao
    )


def _avisar_partes_incoerentes(linhas: list[dict[str, Any]], safra: str) -> None:
    """Cada linha BRASIL da série é a soma das UFs arredondadas, e a identidade entre linhas
    herda esse arredondamento: a tolerância é a da soma das 27 UFs."""
    por_chave = {(linha["produto"], linha["grupo"]): linha for linha in linhas}
    tolerancia = constants.CONAB_ARREDONDAMENTO * (len(constants.CONAB_UFS) + 1)
    for total, partes in constants.CONAB_BRASIL_TOTAL_PARTES:
        for campo in ("area_plantada", "producao"):
            valores = [por_chave[parte][campo] for parte in partes]
            publicado = por_chave[total][campo]
            if publicado is None or None in valores:
                continue
            diferenca = sum(valores) - publicado
            if abs(diferenca) > tolerancia:
                warn_once(
                    f"conab_partes_incoerentes:{safra}:{total[0]}:{campo}",
                    f"CONAB: no brasil_total de {safra} (série histórica), "
                    f"{' + '.join(rotulo for rotulo, _ in partes)} em {campo} somam "
                    f"{sum(valores):.1f}, e {total[0]} publicado, {publicado:.1f} (diferença de "
                    f"{diferenca:.1f}); o agrobr repassa os números publicados",
                )


async def _brasil_total_das_series(safra: str) -> tuple[pd.DataFrame, MetaInfo] | None:
    levantamentos = await client.list_levantamentos()
    if not client.fora_do_boletim(levantamentos, safra):
        return None
    t0 = time.monotonic()
    rotulos: dict[str, str] = {}
    for rotulo, grupo, produto, _ in constants.CONAB_BRASIL_TOTAL_SERIES:
        rotulos[produto] = _rotulo_da_linha(rotulo, grupo)
    series = await _baixar_series(rotulos)
    referencias = {
        url: serie_parser.referencia_publicacao(raw) for url, (_, raw, _) in series.items()
    }
    if not all(
        _serie_mais_nova(
            levantamentos, safra, produto, referencias[serie_client.get_xls_url(produto)]
        )
        for produto in rotulos
    ):
        return None
    ultima = await _ultima_edicao_do_boletim(levantamentos, safra)
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    linhas: dict[str | None, list[dict[str, Any]]] = {"verao": [], "inverno": []}
    blocos: dict[bool, list[dict[str, Any]]] = {False: [], True: []}
    for rotulo, grupo, produto, secao in constants.CONAB_BRASIL_TOTAL_SERIES:
        _, raw, _ = series[serie_client.get_xls_url(produto)]
        valores = serie_parser.linha_brasil(raw, produto, _periodos(safra))
        decimais = {
            campo: Decimal(str(valor)) if valor is not None else None
            for campo, valor in valores.items()
        }
        linha = _linha_total(
            rotulo,
            grupo,
            safra,
            decimais.get("area_plantada_mil_ha"),
            decimais.get("produtividade_kg_ha"),
            decimais.get("producao_mil_ton"),
        )
        blocos[grupo == constants.CONAB_INVERNO].append(linha)
        if secao is not None:
            linhas[secao].append(linha)
    _avisar_partes_incoerentes([*blocos[False], *blocos[True]], safra)
    conferencia = None
    if ultima is not None:
        edicao, xlsx = ultima
        conferencia = _conferir_com_o_boletim(
            safra,
            edicao,
            {
                (_rotulo_da_linha(linha["produto"], linha["grupo"]), coluna): linha[coluna]
                for linha in [*blocos[False], *blocos[True]]
                for coluna in ("area_plantada", "producao")
            },
            {
                (_rotulo_da_linha(linha["produto"], linha["grupo"]), coluna): linha[coluna]
                for linha in ConabParserV1().parse_brasil_total(xlsx, safra_ref=safra)
                for coluna in ("area_plantada", "producao")
            },
        )
    verao = _subtotal("SUBTOTAL", None, safra, linhas["verao"])
    inverno = _subtotal("SUBTOTAL", constants.CONAB_INVERNO, safra, linhas["inverno"])
    totais = [
        *blocos[False],
        verao,
        *blocos[True],
        inverno,
        _subtotal("BRASIL (2)", None, safra, [verao, inverno]),
    ]
    df = _brasil_total_frame(totais)
    parse_ms = int((time.monotonic() - t1) * 1000)

    meta = build_source_meta(
        "conab",
        serie_client.SERIES_HISTORICAS_URL,
        "httpx",
        fetch_ms,
        parse_ms,
        df,
        serie_parser.PARSER_VERSION,
        schema_version=conab_contract.CONAB_BRASIL_TOTAL_V2.version,
        source_details={
            "publicacao": {
                "origem": "serie_historica",
                "series": [
                    _descricao_serie(produto, raw, metadata, referencias[url])
                    for url, (produto, raw, metadata) in series.items()
                ],
                **({"conferencia": conferencia} if conferencia else {}),
            }
        },
    )
    return df, meta


async def _suprimento_mais_recente(
    parser: ConabParserV1, produto: str | None, safra: str
) -> tuple[dict[str, Any], list[dict[str, Any]], BytesIO]:
    levantamentos = await client.list_levantamentos()
    inicio = int(safra[:4])
    anos = sorted(
        {lev["ano_inicio"] for lev in levantamentos if lev["ano_inicio"] >= inicio}, reverse=True
    )
    for ano in anos:
        metadata = dict(next(lev for lev in levantamentos if lev["ano_inicio"] == ano))
        xlsx = await client.download_xlsx(metadata["url"], metadata=metadata)
        suprimentos = parser.parse_suprimento(xlsx=xlsx, produto=produto)
        if ano == inicio or any(linha["safra"] in _periodos(safra) for linha in suprimentos):
            return metadata, suprimentos, xlsx
    raise SourceUnavailableError(
        source="conab",
        url=constants.URLS[constants.Fonte.CONAB]["boletim_graos"],
        last_error=f"No levantamento found for safra={safra}",
    )


@overload
async def safras(
    produto: str,
    safra: str | None = None,
    uf: str | None = None,
    levantamento: int | None = None,
    *,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def safras(
    produto: str,
    safra: str | None = None,
    uf: str | None = None,
    levantamento: int | None = None,
    *,
    as_polars: bool = False,
    return_meta: Literal[False] = False,
) -> result_utils.DataFrame: ...


@overload
async def safras(
    produto: str,
    safra: str | None = None,
    uf: str | None = None,
    levantamento: int | None = None,
    *,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def safras(
    produto: str,
    safra: str | None = None,
    uf: str | None = None,
    levantamento: int | None = None,
    *,
    as_polars: bool = False,
    return_meta: Literal[True],
) -> tuple[result_utils.DataFrame, MetaInfo]: ...


async def safras(
    produto: str,
    safra: str | None = None,
    uf: str | None = None,
    levantamento: int | None = None,
    *,
    as_polars: bool = False,
    return_meta: bool = False,
) -> result_utils.DataFrameResult:
    safra = models.validate_selection(safra, levantamento)
    uf = validate_uf(uf)
    if not isinstance(produto, str) or produto.lower() not in constants.CONAB_PRODUTOS:
        raise InvalidParameterError(
            f"Produto CONAB {produto!r} inválido. Válidos: {sorted(constants.CONAB_PRODUTOS)}"
        )
    logger.info(
        "conab_safras_request",
        produto=produto,
        safra=safra,
        uf=uf,
        levantamento=levantamento,
    )

    if safra is not None and _pode_estar_fora_do_boletim(safra, levantamento):
        revisada = await _safras_da_serie(produto, safra, uf)
        if revisada is not None:
            return finalize_result(*revisada, as_polars=as_polars, return_meta=return_meta)

    t0 = time.monotonic()
    xlsx, metadata = await client.fetch_safra_xlsx(safra=safra, levantamento=levantamento)
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    source_url = metadata.get("url", _SAFRAS_URL)

    parser = ConabParserV1()
    safra_list = parser.parse_safra_produto(
        xlsx=xlsx,
        produto=produto,
        safra_ref=safra or metadata["safra"],
        levantamento=metadata.get("levantamento"),
        data_publicacao=metadata.get("data_publicacao"),
    )

    brasil = {
        (registro.safra, coluna): float(valor)
        for registro in safra_list
        if registro.meta.get("rotulo") == "BRASIL"
        for coluna, valor in (
            ("area_plantada", registro.area_plantada),
            ("producao", registro.producao),
        )
        if valor is not None
    }
    safra_list = [s for s in safra_list if s.uf is not None]

    if uf:
        safra_list = [s for s in safra_list if s.uf == uf.upper()]

    if not safra_list:
        logger.warning(
            "conab_safras_empty",
            produto=produto,
            safra=safra,
            uf=uf,
        )
        df = _safra_frame([])
    else:
        df = _safra_frame([s.model_dump() for s in safra_list])
        serie_parser.avisar_soma_das_ufs(produto, _edicao(metadata), df, brasil)

        logger.info(
            "conab_safras_success",
            produto=produto,
            records=len(df),
        )

    parse_ms = int((time.monotonic() - t1) * 1000)

    meta = build_source_meta(
        "conab",
        source_url,
        metadata.get("source_method", "httpx"),
        fetch_ms,
        parse_ms,
        df,
        parser.version,
        schema_version="2.0",
        raw_content_hash=hashlib.sha256(xlsx.getvalue()).hexdigest(),
        raw_content_size=len(xlsx.getvalue()),
        source_details={"publicacao": _publicacao(metadata)},
    )
    meta.cache_key = build_cache_key(
        "conab:safras",
        {
            "produto": produto.lower(),
            "safra": safra or metadata["safra"],
            "publicacao": metadata["safra"],
            "levantamento": metadata.get("levantamento"),
            "uf": uf,
        },
        schema_version=meta.schema_version,
    )

    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


@overload
async def balanco(
    produto: str | None = None,
    safra: str | None = None,
    *,
    as_polars: Literal[False] = False,
    levantamento: int | None = None,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def balanco(
    produto: str | None = None,
    safra: str | None = None,
    *,
    as_polars: bool = False,
    levantamento: int | None = None,
    return_meta: Literal[False] = False,
) -> result_utils.DataFrame: ...


@overload
async def balanco(
    produto: str | None = None,
    safra: str | None = None,
    *,
    as_polars: Literal[False] = False,
    levantamento: int | None = None,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def balanco(
    produto: str | None = None,
    safra: str | None = None,
    *,
    as_polars: bool = False,
    levantamento: int | None = None,
    return_meta: Literal[True],
) -> tuple[result_utils.DataFrame, MetaInfo]: ...


async def balanco(
    produto: str | None = None,
    safra: str | None = None,
    *,
    as_polars: bool = False,
    return_meta: bool = False,
    levantamento: int | None = None,
) -> result_utils.DataFrameResult:
    safra = models.validate_selection(safra, levantamento)
    if produto is not None and (
        not isinstance(produto, str)
        or regions.remover_acentos(produto).casefold() not in constants.CONAB_BALANCO_PRODUTOS
    ):
        raise InvalidParameterError(
            f"Produto do balanço CONAB {produto!r} inválido. "
            f"Válidos: {list(constants.CONAB_BALANCO_PRODUTOS)}"
        )
    logger.info(
        "conab_balanco_request",
        produto=produto,
        safra=safra,
        levantamento=levantamento,
    )

    t0 = time.monotonic()
    parser = ConabParserV1()
    if safra is not None and levantamento is None:
        metadata, suprimentos, xlsx = await _suprimento_mais_recente(parser, produto, safra)
    else:
        xlsx, metadata = await client.fetch_safra_xlsx(
            safra=safra, levantamento=levantamento, da_propria_safra=True
        )
        suprimentos = parser.parse_suprimento(xlsx=xlsx, produto=produto)
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    _avisar_balanco_incoerente(suprimentos, metadata)
    df = _balanco_frame(suprimentos)

    if not suprimentos:
        logger.warning(
            "conab_balanco_empty",
            produto=produto,
        )
    else:
        logger.info(
            "conab_balanco_success",
            produto=produto,
            records=len(df),
        )

    parse_ms = int((time.monotonic() - t1) * 1000)
    source_url = metadata.get("url", _SAFRAS_URL)

    meta = build_source_meta(
        "conab",
        source_url,
        metadata.get("source_method", "httpx"),
        fetch_ms,
        parse_ms,
        df,
        parser.version,
        schema_version="1.1",
        raw_content_hash=hashlib.sha256(xlsx.getvalue()).hexdigest(),
        raw_content_size=len(xlsx.getvalue()),
        source_details={"publicacao": _publicacao(metadata)},
    )
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


@overload
async def brasil_total(
    safra: str | None = None,
    *,
    as_polars: Literal[False] = False,
    levantamento: int | None = None,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def brasil_total(
    safra: str | None = None,
    *,
    as_polars: bool = False,
    levantamento: int | None = None,
    return_meta: Literal[False] = False,
) -> result_utils.DataFrame: ...


@overload
async def brasil_total(
    safra: str | None = None,
    *,
    as_polars: Literal[False] = False,
    levantamento: int | None = None,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def brasil_total(
    safra: str | None = None,
    *,
    as_polars: bool = False,
    levantamento: int | None = None,
    return_meta: Literal[True],
) -> tuple[result_utils.DataFrame, MetaInfo]: ...


async def brasil_total(
    safra: str | None = None,
    *,
    as_polars: bool = False,
    return_meta: bool = False,
    levantamento: int | None = None,
) -> result_utils.DataFrameResult:
    safra = models.validate_selection(safra, levantamento)
    logger.info(
        "conab_brasil_total_request",
        safra=safra,
        levantamento=levantamento,
    )

    if safra is not None and _pode_estar_fora_do_boletim(safra, levantamento):
        revisado = await _brasil_total_das_series(safra)
        if revisado is not None:
            return finalize_result(*revisado, as_polars=as_polars, return_meta=return_meta)

    t0 = time.monotonic()
    xlsx, metadata = await client.fetch_safra_xlsx(safra=safra, levantamento=levantamento)
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    parser = ConabParserV1()
    totais = parser.parse_brasil_total(xlsx=xlsx, safra_ref=safra)

    if not totais:
        logger.warning("conab_brasil_total_empty", safra=safra)
        df = _brasil_total_frame([])
    else:
        df = _brasil_total_frame(totais)
        logger.info(
            "conab_brasil_total_success",
            records=len(df),
        )

    parse_ms = int((time.monotonic() - t1) * 1000)
    source_url = metadata.get("url", _SAFRAS_URL)

    meta = build_source_meta(
        "conab",
        source_url,
        metadata.get("source_method", "httpx"),
        fetch_ms,
        parse_ms,
        df,
        parser.version,
        schema_version=conab_contract.CONAB_BRASIL_TOTAL_V2.version,
        raw_content_hash=hashlib.sha256(xlsx.getvalue()).hexdigest(),
        raw_content_size=len(xlsx.getvalue()),
        source_details={"publicacao": _publicacao(metadata)},
    )
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


async def levantamentos() -> list[dict[str, Any]]:
    return await client.list_levantamentos()


async def produtos() -> list[str]:
    return list(constants.CONAB_PRODUTOS.keys())


async def ufs() -> list[str]:
    return constants.CONAB_UFS.copy()
