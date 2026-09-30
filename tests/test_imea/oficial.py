from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pandas as pd

GOLDEN = Path(__file__).parents[1] / "golden_data" / "imea" / "oficial_20260923"
BASE = "https://api1.imea.com.br/api/v2/mobile/cadeias"
CADEIAS = {
    1: "algodao",
    2: "bovinocultura",
    3: "milho",
    4: "soja",
    5: "conjuntura",
    7: "suinocultura",
    8: "leite",
    10: "custo_producao",
}
COLUNAS = [
    "cadeia",
    "indicador_id",
    "indicador",
    "localidade",
    "valor",
    "variacao",
    "safra",
    "unidade",
    "unidade_descricao",
    "data_publicacao",
]


def manifest() -> dict[str, Any]:
    data: dict[str, Any] = json.loads((GOLDEN / "manifest.json").read_bytes())
    for item in data["arquivos"]:
        corpo = (GOLDEN / item["arquivo"]).read_bytes()
        assert hashlib.sha256(corpo).hexdigest() == item["sha256"], item["arquivo"]
    for nome, item in data["recortes"].items():
        assert hashlib.sha256((GOLDEN / nome).read_bytes()).hexdigest() == item["sha256"], nome
    return data


def corpos() -> dict[str, bytes]:
    manifest()
    servidos = {}
    for cadeia in CADEIAS:
        servidos[f"{BASE}/{cadeia}/cotacoes"] = (GOLDEN / f"cotacoes_{cadeia}.json").read_bytes()
        servidos[f"{BASE}/{cadeia}/indicadores"] = (
            GOLDEN / f"indicadores_{cadeia}.json"
        ).read_bytes()
    return servidos


def registros(cadeia: int) -> list[dict[str, Any]]:
    dados: list[dict[str, Any]] = json.loads((GOLDEN / f"cotacoes_{cadeia}.json").read_bytes())
    return dados


def nomes(cadeia: int) -> dict[str, str]:
    catalogo = json.loads((GOLDEN / f"indicadores_{cadeia}.json").read_bytes())
    return {item["Id"]: item["Nome"] for item in catalogo}


def esperado(cadeia: int, registro: dict[str, Any]) -> dict[str, Any]:
    return {
        "cadeia": CADEIAS[cadeia],
        "indicador_id": registro["IndicadorFinalId"],
        "indicador": nomes(cadeia).get(registro["IndicadorFinalId"]),
        "localidade": registro["Localidade"],
        "valor": None if registro["Valor"] is None else float(registro["Valor"]),
        "variacao": None if registro["Variacao"] is None else float(registro["Variacao"]),
        "safra": registro["Safra"],
        "unidade": registro["UnidadeSigla"],
        "unidade_descricao": registro["UnidadeDescricao"],
        "data_publicacao": None
        if registro["DataPublicacao"] is None
        else pd.Timestamp(registro["DataPublicacao"]),
    }


def chave(linha: dict[str, Any]) -> tuple[str, ...]:
    return tuple(
        "" if linha[c] is None else str(linha[c])
        for c in ("indicador_id", "localidade", "data_publicacao", "safra", "unidade")
    )


def publicado(frame: pd.DataFrame) -> list[dict[str, Any]]:
    linhas = []
    for registro in frame.to_dict("records"):
        linhas.append(
            {
                str(coluna): None
                if valor is None or (not isinstance(valor, str) and pd.isna(valor))
                else valor.item()
                if hasattr(valor, "item")
                else valor
                for coluna, valor in registro.items()
            }
        )
    return sorted(linhas, key=chave)


def ordenado(linhas: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(linhas, key=chave)
