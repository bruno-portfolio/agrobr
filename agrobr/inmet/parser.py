from __future__ import annotations

import contextlib
import csv
import hashlib
import io
import json
import warnings
from datetime import date
from typing import Any

import pandas as pd
import pydantic

from agrobr import _log
from agrobr.exceptions import ParseError
from agrobr.normalize import dates, encoding
from agrobr.normalize.regions import remover_acentos
from agrobr.utils.result import ATRIBUTO_AVISOS

from . import models

logger = _log.get_logger(__name__)

PARSER_VERSION = 1
HISTORICO_PARSER_VERSION = 2

COLUNAS_HORARIAS = {
    "DT_MEDICAO": "data",
    "HR_MEDICAO": "hora_utc",
    "CD_ESTACAO": "estacao",
    "UF": "uf",
    "TEM_INS": "temperatura",
    "TEM_MAX": "temperatura_max",
    "TEM_MIN": "temperatura_min",
    "UMD_INS": "umidade",
    "UMD_MAX": "umidade_max",
    "UMD_MIN": "umidade_min",
    "CHUVA": "precipitacao_mm",
    "PRE_INS": "pressao_hpa",
    "VEN_VEL": "vento_ms",
    "VEN_DIR": "vento_dir",
    "VEN_RAJ": "vento_rajada_ms",
    "RAD_GLO": "radiacao_kj_m2",
    "PTO_INS": "ponto_orvalho",
}

COLUNAS_NUMERICAS = [
    "temperatura",
    "temperatura_max",
    "temperatura_min",
    "umidade",
    "umidade_max",
    "umidade_min",
    "precipitacao_mm",
    "pressao_hpa",
    "vento_ms",
    "vento_dir",
    "vento_rajada_ms",
    "radiacao_kj_m2",
    "ponto_orvalho",
]

SENTINEL = -9999.0

COLUNAS_DATA_CATALOGO = ("inicio_operacao", "DT_FIM_OPERACAO")
INSTANTE_ISO = r"\d{4}-\d{2}-\d{2}(?:T\d{2}:\d{2}(?::\d{2}(?:\.\d{1,9})?)?(?:Z|[+-]\d{2}:\d{2})?)?"


def converter_datas_catalogo(df: pd.DataFrame) -> None:
    """Converte no lugar as datas do catálogo de estações, instantes ISO com fuso, para UTC.

    Texto fora do formato ISO é mudança de layout (`ParseError`); data ISO inexistente vira
    `NaT` com aviso em `df.attrs`, de onde o `build_source_meta` a leva ao `MetaInfo`.
    """
    for coluna in COLUNAS_DATA_CATALOGO:
        if coluna not in df:
            continue
        texto = df[coluna].astype("string").str.strip()
        presentes = texto.notna() & texto.ne("")
        fora = presentes & ~texto.str.fullmatch(INSTANTE_ISO).fillna(False)
        if fora.any():
            raise ParseError(
                source="inmet",
                parser_version=PARSER_VERSION,
                reason=f"{coluna} fora do formato ISO no catálogo: {texto[fora].iloc[0]!r}",
            )
        instantes = pd.to_datetime(
            texto.where(presentes), format="ISO8601", utc=True, errors="coerce"
        )
        perdidas = int((presentes & instantes.isna()).sum())
        if perdidas:
            aviso = f"inmet: {perdidas} valor(es) de {coluna} viraram NaT (data inexistente)."
            df.attrs.setdefault(ATRIBUTO_AVISOS, []).append(aviso)
            warnings.warn(aviso, UserWarning, stacklevel=2)
        df[coluna] = instantes.dt.as_unit("ns")


def validate_observation_scope(
    dados: list[dict[str, Any]],
    inicio: date,
    fim: date,
    *,
    codigo: str | None = None,
    uf: str | None = None,
) -> None:
    for raw in dados:
        identity = None
        with contextlib.suppress(pydantic.ValidationError):
            identity = models.APIObservationIdentity.model_validate(raw)
        if identity is None:
            raise ParseError(
                source="inmet",
                parser_version=PARSER_VERSION,
                reason="Identidade/data de observação inválida na API",
            )
        if codigo is not None and identity.codigo != codigo:
            raise ParseError(
                source="inmet",
                parser_version=PARSER_VERSION,
                reason="API retornou estação diferente da solicitada",
            )
        if uf is not None and identity.uf != uf:
            raise ParseError(
                source="inmet",
                parser_version=PARSER_VERSION,
                reason="API retornou UF diferente da solicitada",
            )
        if not inicio <= identity.data <= fim:
            raise ParseError(
                source="inmet",
                parser_version=PARSER_VERSION,
                reason="API retornou observação fora do intervalo solicitado",
            )


def parse_observacoes(dados: list[dict[str, Any]]) -> pd.DataFrame:
    if not dados:
        raise ParseError(
            source="inmet",
            parser_version=PARSER_VERSION,
            reason="Resposta INMET vazia (nenhuma observação)",
        )

    df = pd.DataFrame(dados)

    colunas_presentes = {k: v for k, v in COLUNAS_HORARIAS.items() if k in df.columns}

    if not colunas_presentes:
        raise ParseError(
            source="inmet",
            parser_version=PARSER_VERSION,
            reason=f"Nenhuma coluna esperada encontrada. Colunas recebidas: {df.columns.tolist()}",
        )

    df = df.rename(columns=colunas_presentes)

    if "hora_utc" not in df:
        raise ParseError(
            source="inmet", parser_version=PARSER_VERSION, reason="Hora UTC ausente na observação"
        )
    try:
        df["hora_utc"] = df["hora_utc"].map(models.normalize_hora_utc)
    except ValueError as exc:
        raise ParseError(
            source="inmet", parser_version=PARSER_VERSION, reason="Hora UTC inválida na observação"
        ) from exc

    for col in COLUNAS_NUMERICAS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            df.loc[df[col] == SENTINEL, col] = pd.NA

    dates.converter_coluna(
        df, "data", fonte="inmet", efeito=" As observações sem data saíram do resultado."
    )

    df = df.dropna(subset=["data"])
    df = df.sort_values(["estacao", "data", "hora_utc"]).reset_index(drop=True)

    logger.debug(
        "inmet_parse_ok",
        records=len(df),
        estacoes=df["estacao"].nunique() if "estacao" in df.columns else 0,
    )

    return df


HISTORICO_COLUNA_PREFIXOS: dict[str, str] = {
    "precipitacao total": "precipitacao_mm",
    "pressao atmosferica ao nivel": "pressao_hpa",
    "radiacao global": "radiacao_kj_m2",
    "temperatura do ar": "temperatura",
    "temperatura do ponto de orvalho": "ponto_orvalho",
    "temperatura maxima na hora": "temperatura_max",
    "temperatura minima na hora": "temperatura_min",
    "umidade relativa do ar": "umidade",
    "umidade rel. max": "umidade_max",
    "umidade rel. min": "umidade_min",
    "vento, direcao": "vento_dir",
    "vento, rajada": "vento_rajada_ms",
    "vento, velocidade": "vento_ms",
}


def _mapear_header_historico(header: list[str]) -> dict[str, str]:
    rename: dict[str, str] = {}
    for col in header:
        norm = remover_acentos(col).strip().lower()
        if norm == "data" or norm.startswith("data ("):
            rename[col] = "data"
        elif norm.startswith("hora"):
            rename[col] = "hora_utc"
        else:
            for prefixo, destino in HISTORICO_COLUNA_PREFIXOS.items():
                if norm.startswith(prefixo):
                    rename[col] = destino
                    break
    return rename


def _historico_header(raw: bytes) -> tuple[models.HistoricoEstacao, list[str], int]:
    lines = raw.decode(encoding.detect_encoding_chain(raw)).splitlines()
    if len(lines) < 3:
        raise ParseError(source="inmet", parser_version=2, reason="CSV histórico truncado")
    metadata: dict[str, str] = {}
    for index, line in enumerate(lines):
        header = next(csv.reader([line], delimiter=";"))
        rename = _mapear_header_historico(header)
        if {"data", "hora_utc", "precipitacao_mm"}.issubset(rename.values()):
            try:
                station = models.HistoricoEstacao.model_validate(
                    {
                        "codigo": metadata.get("codigo (wmo)"),
                        "uf": metadata.get("uf"),
                        "nome": metadata.get("estacao"),
                        "regiao": metadata.get("regiao"),
                        "latitude": metadata.get("latitude"),
                        "longitude": metadata.get("longitude"),
                        "altitude": metadata.get("altitude"),
                        "fundacao": metadata.get("data de fundacao"),
                    }
                )
            except pydantic.ValidationError as exc:
                raise ParseError(
                    source="inmet", parser_version=2, reason=f"Metadata inválida: {exc}"
                ) from exc
            return station, lines, index
        key, _, value = line.partition(";")
        normalized = remover_acentos(key).strip().rstrip(":").lower()
        if normalized.startswith("data de fundacao"):
            normalized = "data de fundacao"
        metadata[normalized] = value.strip()
    raise ParseError(
        source="inmet", parser_version=2, reason="Header do CSV histórico não reconhecido"
    )


def parse_historico_metadata(raw: bytes) -> models.HistoricoEstacao:
    return _historico_header(raw)[0]


def historico_layout_fingerprint(raw: bytes) -> dict[str, str | int]:
    _, lines, header_index = _historico_header(raw)

    def normalize(value: str) -> str:
        return " ".join(remover_acentos(value).strip().lower().split())

    layout = {
        "delimiter": ";",
        "metadata_keys": [
            normalize(line.partition(";")[0].strip().rstrip(":")) for line in lines[:header_index]
        ],
        "columns": [
            normalize(column) for column in next(csv.reader([lines[header_index]], delimiter=";"))
        ],
    }
    encoded = json.dumps(layout, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )
    return {
        "algorithm": "sha256",
        "version": 1,
        "parser_version": HISTORICO_PARSER_VERSION,
        "sha256": hashlib.sha256(encoded).hexdigest(),
    }


def empty_historico(agregacao: str = "horario") -> pd.DataFrame:
    if agregacao == "horario":
        types = dict.fromkeys(COLUNAS_NUMERICAS, "float64")
        types.update({"data": "datetime64[ns]", "hora_utc": "str", "estacao": "str", "uf": "str"})
        return pd.DataFrame(
            {name: pd.Series(dtype=types[name]) for name in COLUNAS_HORARIAS.values()}
        )
    if agregacao == "diario":
        types = dict.fromkeys(
            (
                "temp_media",
                "temp_max",
                "temp_min",
                "precipitacao_mm",
                "umidade_media",
                "radiacao_total_kj_m2",
            ),
            "float64",
        )
        types = {"data": "datetime64[ns]", "estacao": "str", "uf": "str", **types}
    else:
        types = {
            "mes": "datetime64[ns]",
            "uf": "str",
            "precip_acum_mm": "float64",
            "temp_media": "float64",
            "temp_max_media": "float64",
            "temp_min_media": "float64",
            "num_estacoes": "int64",
            "estacoes_chuva": "int64",
            "estacoes_chuva_parciais": "int64",
            "dias": "int64",
            "data_inicio": "datetime64[ns]",
            "data_fim": "datetime64[ns]",
        }
    return pd.DataFrame({name: pd.Series(dtype=dtype) for name, dtype in types.items()})


def parse_historico_csv(raw: bytes, codigo: str) -> pd.DataFrame:
    station, lines, index = _historico_header(raw)
    if station.codigo != models.validate_codigo(codigo):
        raise ParseError(
            source="inmet", parser_version=2, reason="Estação do cabeçalho difere da solicitada"
        )
    reader = csv.reader(io.StringIO("\n".join(lines[index:])), delimiter=";")
    header = next(reader)
    rename = _mapear_header_historico(header)
    if len(set(rename.values())) != len(rename):
        raise ParseError(source="inmet", parser_version=2, reason="Colunas históricas duplicadas")
    records: list[dict[str, Any]] = []
    for line_number, row in enumerate(reader, start=index + 2):
        if not row or not any(cell.strip() for cell in row):
            continue
        if len(row) != len(header):
            raise ParseError(
                source="inmet",
                parser_version=2,
                reason=f"Linha {line_number} truncada ou com colunas extras",
            )
        values = {
            rename[name]: value for name, value in zip(header, row, strict=True) if name in rename
        }
        try:
            record = models.HistoricoObservacao.model_validate(values).model_dump()
        except pydantic.ValidationError as exc:
            raise ParseError(
                source="inmet", parser_version=2, reason=f"Linha {line_number} inválida: {exc}"
            ) from exc
        records.append(record)
    if not records:
        return empty_historico()
    df = pd.DataFrame(records)
    df["data"] = pd.to_datetime(df["data"])
    df["estacao"] = station.codigo
    df["uf"] = station.uf
    df[COLUNAS_NUMERICAS] = df[COLUNAS_NUMERICAS].astype("float64")
    return (
        df[list(COLUNAS_HORARIAS.values())].sort_values(["data", "hora_utc"]).reset_index(drop=True)
    )


def agregar_diario(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    agg_dict: dict[str, tuple[str, Any]] = {
        "temp_media": ("temperatura", "mean"),
        "temp_max": ("temperatura_max", "max"),
        "temp_min": ("temperatura_min", "min"),
        "precipitacao_mm": ("precipitacao_mm", lambda values: values.sum(min_count=1)),
        "umidade_media": ("umidade", "mean"),
        "radiacao_total_kj_m2": ("radiacao_kj_m2", lambda values: values.sum(min_count=1)),
    }

    agg_filtrado = {k: v for k, v in agg_dict.items() if v[0] in df.columns}

    if not agg_filtrado:
        return df

    group_cols = ["estacao"]
    if "uf" in df.columns:
        group_cols.append("uf")

    result = (
        df.groupby([pd.Grouper(key="data", freq="D")] + group_cols)  # type: ignore[operator]
        .agg(**{k: pd.NamedAgg(column=v[0], aggfunc=v[1]) for k, v in agg_filtrado.items()})
        .reset_index()
    )

    return result


def agregar_mensal_uf(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "uf" not in df.columns:
        return df

    df = df.copy()
    df["mes"] = df["data"].dt.to_period("M")

    agg: dict[str, pd.NamedAgg] = {}

    if "precipitacao_mm" in df.columns:
        agg["precip_acum_mm"] = pd.NamedAgg(
            column="precipitacao_mm", aggfunc=lambda values: values.sum(min_count=1)
        )
    if "temp_media" in df.columns:
        agg["temp_media"] = pd.NamedAgg(column="temp_media", aggfunc="mean")
    if "temp_max" in df.columns:
        agg["temp_max_media"] = pd.NamedAgg(column="temp_max", aggfunc="mean")
    if "temp_min" in df.columns:
        agg["temp_min_media"] = pd.NamedAgg(column="temp_min", aggfunc="mean")
    if "estacao" in df.columns:
        agg["num_estacoes"] = pd.NamedAgg(column="estacao", aggfunc="nunique")

    if not agg:
        return df

    result = df.groupby(["mes", "uf"]).agg(**agg)
    if "precipitacao_mm" in df.columns and "estacao" in df.columns:
        chuva = df.groupby(["mes", "uf", "estacao"])["precipitacao_mm"].agg(
            total=lambda values: values.sum(min_count=1), dias="count"
        )
        dias_no_mes = pd.PeriodIndex(chuva.index.get_level_values("mes")).days_in_month.to_numpy()
        completa = chuva["dias"].eq(dias_no_mes)
        parcial = chuva["dias"].gt(0) & ~completa
        result["precip_acum_mm"] = (
            chuva["total"].where(completa).groupby(level=["mes", "uf"]).mean()
        )
        result["estacoes_chuva"] = completa.groupby(level=["mes", "uf"]).sum()
        result["estacoes_chuva_parciais"] = parcial.groupby(level=["mes", "uf"]).sum()
    medidas = [c for c in ("precipitacao_mm", "temp_media", "temp_max", "temp_min") if c in df]
    cobertura = (
        df[df[medidas].notna().any(axis=1)]
        .groupby(["mes", "uf"])["data"]
        .agg(dias="nunique", data_inicio="min", data_fim="max")
    )
    result = result.join(cobertura)
    result["dias"] = result["dias"].fillna(0).astype("int64")
    result = result.reset_index()
    result["mes"] = result["mes"].dt.to_timestamp()

    return result
