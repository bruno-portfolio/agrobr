from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any

import pandas as pd

GOLDEN = Path(__file__).parents[1] / "golden_data" / "sfb" / "oficial_20260923"
COLUNAS_CNFP = [
    "fid",
    "nome",
    "uf",
    "bioma",
    "categoria",
    "tipo",
    "governo",
    "classe",
    "area_ha",
    "ano_criacao",
    "ano_criacao_texto",
    "municipio",
]
COLUNAS_CONCESSOES = ["fid", "nome", "uf", "bioma", "area_ha", "ano_criacao", "grupo", "categoria"]
_ANO = re.compile(r"(?<![0-9])[0-9]{4}(?![0-9])")


def manifest() -> dict[str, Any]:
    dados: dict[str, Any] = json.loads((GOLDEN / "manifest.json").read_bytes())
    for resposta in dados["respostas"]:
        corpo = (GOLDEN / resposta["file"]).read_bytes()
        assert hashlib.sha256(corpo).hexdigest() == resposta["sha256"], resposta["file"]
    return dados


def respostas(cenario: str) -> list[dict[str, Any]]:
    return [r for r in manifest()["respostas"] if r["cenario"] == cenario]


def corpos(*cenarios: str) -> dict[str, bytes]:
    return {r["url"]: (GOLDEN / r["file"]).read_bytes() for c in cenarios for r in respostas(c)}


def urls(cenario: str) -> list[str]:
    return [r["url"] for r in respostas(cenario)]


def feicoes(cenario: str) -> list[dict[str, Any]]:
    linhas: list[dict[str, Any]] = []
    for resposta in respostas(cenario)[1:]:
        documento = json.loads((GOLDEN / resposta["file"]).read_bytes())
        linhas += [f.get("attributes") or f.get("properties") for f in documento["features"]]
    return linhas


def ano(valor: object) -> int | None:
    if isinstance(valor, int):
        return valor
    anos = set(_ANO.findall(valor)) if isinstance(valor, str) else set()
    return int(anos.pop()) if len(anos) == 1 else None


def ambiguos(cenario: str) -> int:
    return sum(len(set(_ANO.findall(f["anocriacao"] or ""))) > 1 for f in feicoes(cenario))


def esperado_cnfp(cenario: str) -> list[dict[str, Any]]:
    return [
        {
            "fid": f["fid"],
            "nome": f["nome"],
            "uf": f["uf"],
            "bioma": f["bioma"],
            "categoria": f["categoria"],
            "tipo": f["tipo"],
            "governo": f["governo"],
            "classe": f["classe"],
            "area_ha": f["area_ha"],
            "ano_criacao": ano(f["anocriacao"]),
            "ano_criacao_texto": f["anocriacao"],
            "municipio": f["municipio"],
        }
        for f in feicoes(cenario)
    ]


def esperado_concessoes(cenario: str) -> list[dict[str, Any]]:
    return [
        {
            "fid": f["fid"],
            "nome": f["nome_uc"],
            "uf": f["uf"],
            "bioma": f["bioma"],
            "area_ha": f["hectares"],
            "ano_criacao": ano(f["criacao"]),
            "grupo": f["grupo"],
            "categoria": f["cat_nome"],
        }
        for f in feicoes(cenario)
    ]


def sem_orientacao(aneis: list[list[list[float]]]) -> list[list[list[float]]]:
    return sorted(min(anel, anel[::-1]) for anel in aneis)


def aneis_oficiais(arquivo: str) -> dict[int, list[list[list[float]]]]:
    return {
        f["attributes"]["fid"]: sem_orientacao(f["geometry"]["rings"])
        for f in json.loads((GOLDEN / arquivo).read_bytes())
    }


def publicado(frame: pd.DataFrame) -> list[dict[str, Any]]:
    linhas = []
    for registro in frame.drop(columns="geometry", errors="ignore").to_dict("records"):
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
