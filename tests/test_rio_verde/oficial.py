from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pandas as pd

GOLDEN = Path(__file__).parents[1] / "golden_data" / "rio_verde"
ORACULO = GOLDEN / "oraculo_20260923.json"
COLUNAS = [
    "safra",
    "empresa",
    "cultivar",
    "grupo_maturacao",
    "ciclo_dias",
    "produtividade_1_epoca_sc_ha",
    "produtividade_2_epoca_sc_ha",
    "produtividade_3_epoca_sc_ha",
    "produtividade_4_epoca_sc_ha",
    "produtividade_media_sc_ha",
]


def oraculo() -> dict[str, Any]:
    dados: dict[str, Any] = json.loads(ORACULO.read_bytes())
    for safra in dados["safras"].values():
        corpo = (GOLDEN / safra["arquivo"]).read_bytes()
        assert hashlib.sha256(corpo).hexdigest() == safra["sha256"], safra["arquivo"]
    return dados


def corpos() -> dict[str, bytes]:
    return {s["url"]: (GOLDEN / s["arquivo"]).read_bytes() for s in oraculo()["safras"].values()}


def _numero(texto: str) -> float | None:
    return None if texto == "-" else float(texto.replace(",", "."))


def esperado(safra: str, linhas: list[dict[str, str]] | None = None) -> list[dict[str, Any]]:
    if linhas is None:
        linhas = oraculo()["safras"][safra]["linhas"]
    return [
        {
            "safra": safra,
            "empresa": linha["empresa"],
            "cultivar": linha["cultivar"],
            "grupo_maturacao": linha["gm"],
            "ciclo_dias": int(linha["ciclo"]),
            "produtividade_1_epoca_sc_ha": _numero(linha["e1"]),
            "produtividade_2_epoca_sc_ha": _numero(linha["e2"]),
            "produtividade_3_epoca_sc_ha": _numero(linha["e3"]),
            "produtividade_4_epoca_sc_ha": _numero(linha["e4"]),
            "produtividade_media_sc_ha": _numero(linha["media"]),
        }
        for linha in linhas
    ]


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
    return linhas
