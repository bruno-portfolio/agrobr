from __future__ import annotations

import csv
import hashlib
import io
import json
import re
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pandas as pd

GOLDEN = Path(__file__).parents[1] / "golden_data" / "ibama" / "oficial_20260923"
RECORTE = GOLDEN / "termo_de_embargo_recorte.csv"
COLUNAS = [
    ("seq_tad", "SEQ_TAD", "texto"),
    ("numero_tad", "NUM_TAD", "texto"),
    ("data_embargo", "DAT_EMBARGO", "data"),
    ("num_processo", "NUM_PROCESSO", "texto"),
    ("descricao", "DES_TAD", "texto"),
    ("codigo_municipio", "COD_MUNICIPIO", "texto"),
    ("municipio", "MUNICIPIO", "texto"),
    ("uf", "UF", "texto"),
    ("latitude", "NUM_LATITUDE_TAD", "numero"),
    ("longitude", "NUM_LONGITUDE_TAD", "numero"),
    ("area_embargada_ha", "QTD_AREA_EMBARGADA", "area"),
    ("nome_imovel", "NOME_IMOVEL", "texto"),
    ("status", "DES_STATUS_FORMULARIO", "texto"),
    ("cancelado", "SIT_CANCELADO", "sim_nao"),
    ("data_desembargo", "DAT_DESEMBARGO", "data"),
]
BBOX_DOC = (-56.0, -16.0, -54.0, -14.0)


def manifest() -> dict[str, Any]:
    data: dict[str, Any] = json.loads((GOLDEN / "manifest.json").read_bytes())
    corpo = RECORTE.read_bytes()
    assert hashlib.sha256(corpo).hexdigest() == data["recorte"]["sha256"]
    return data


def fonte() -> list[dict[str, str]]:
    texto = RECORTE.read_bytes().decode("utf-8-sig")
    return list(csv.DictReader(io.StringIO(texto, newline=""), delimiter=";"))


def edicao() -> datetime:
    return max(
        datetime.strptime(r["ULTIMA_ATUALIZACAO_RELATORIO"], "%Y-%m-%d %H:%M:%S")
        for r in fonte()
        if r["ULTIMA_ATUALIZACAO_RELATORIO"]
    )


def _valor(bruto: str, tipo: str) -> Any:
    if bruto == "":
        return False if tipo == "sim_nao" else None
    if tipo == "data":
        data = datetime.strptime(bruto, "%Y-%m-%d %H:%M:%S")
        plausivel = 1900 <= data.year <= 2099 and data.date() <= edicao().date()
        return data if plausivel else None
    if tipo == "area":
        inteiro, _, fracao = bruto.partition(",")
        return float(Decimal(f"{inteiro}.{fracao or '0'}"))
    if tipo == "numero":
        return float(Decimal(bruto))
    if tipo == "sim_nao":
        return bruto == "S"
    return bruto


def esperado(registro: dict[str, str]) -> dict[str, Any]:
    return {saida: _valor(registro[origem], tipo) for saida, origem, tipo in COLUNAS}


def publicado(frame: pd.DataFrame) -> list[dict[str, Any]]:
    linhas = []
    for registro in frame.drop(columns="geometry", errors="ignore").to_dict("records"):
        linha = {}
        for coluna, valor in registro.items():
            if valor is None or valor is pd.NaT or (not isinstance(valor, str) and pd.isna(valor)):
                linha[str(coluna)] = None
            elif isinstance(valor, pd.Timestamp):
                linha[str(coluna)] = valor.to_pydatetime()
            elif hasattr(valor, "item"):
                linha[str(coluna)] = valor.item()
            else:
                linha[str(coluna)] = valor
        linhas.append(linha)
    return linhas


def ponto(registro: dict[str, str]) -> tuple[float, float] | None:
    try:
        return float(registro["NUM_LONGITUDE_TAD"]), float(registro["NUM_LATITUDE_TAD"])
    except ValueError:
        return None


def ponto_no_bbox(registro: dict[str, str], bbox: tuple[float, float, float, float]) -> bool:
    p = ponto(registro)
    return p is not None and bbox[0] <= p[0] <= bbox[2] and bbox[1] <= p[1] <= bbox[3]


def numeros_do_wkt(wkt: str) -> list[float]:
    return [float(numero) for numero in re.findall(r"-?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?", wkt)]
