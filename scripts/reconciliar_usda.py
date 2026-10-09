from __future__ import annotations

import argparse
import asyncio
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import pandas as pd

GATEWAY = "https://api.fas.usda.gov/api/psd"
GOLDEN = Path(__file__).resolve().parents[1] / "tests/golden_data/usda/psd_gateway_20260926"
AGENT = "agrobr-reconciliacao/1.0 (+https://github.com/bruno-portfolio/agrobr)"
CATALOGOS = {
    "commodityAttributes": ("attributeId", "attributeName"),
    "commodities": ("commodityCode", "commodityName"),
    "countries": ("countryCode", "countryName"),
    "unitsOfMeasure": ("unitId", "unitDescription"),
}
PAISES = {"BR": "BR", "US": "US", "world": "world", "mundo": "world"}


def mapa(registros: list[dict[str, Any]], chave: str, rotulo: str) -> dict[Any, str]:
    return {registro[chave]: registro[rotulo].strip() for registro in registros}


def conferir_catalogos(cliente: httpx.Client) -> tuple[dict[str, Any], dict[str, dict[Any, str]]]:
    from agrobr.usda import models

    locais = {
        "commodityAttributes": models.nomes_de_atributo(),
        "commodities": models.nomes_de_produto(),
        "countries": models.nomes_de_pais(),
        "unitsOfMeasure": models.unidades(),
    }
    vivos = {}
    problemas = []
    for nome, (chave, rotulo) in CATALOGOS.items():
        resposta = cliente.get(f"{GATEWAY}/{nome}")
        resposta.raise_for_status()
        vivos[nome] = mapa(resposta.json(), chave, rotulo)
        novos = sorted(set(vivos[nome]) - set(locais[nome]), key=str)
        sumidos = sorted(set(locais[nome]) - set(vivos[nome]), key=str)
        trocados = sorted(
            (k for k in set(vivos[nome]) & set(locais[nome]) if vivos[nome][k] != locais[nome][k]),
            key=str,
        )
        problemas += [f"{nome}: código novo {k} ({vivos[nome][k]})" for k in novos]
        problemas += [f"{nome}: código sumiu {k}" for k in sumidos]
        problemas += [f"{nome}: {k} mudou de nome para {vivos[nome][k]}" for k in trocados]
    return {"status": "ok" if not problemas else "mismatch", "problems": problemas}, vivos


def cortes() -> list[tuple[str, str, int, str]]:
    manifesto = json.loads((GOLDEN / "manifest.json").read_bytes())
    saida = []
    for entrada in manifesto["arquivos"]:
        partes = entrada["arquivo"].removesuffix(".json").rsplit("_", 2)
        if entrada["origem"].startswith("Conferência de 26/09/2026") and partes[-2] in PAISES:
            saida.append((partes[0], PAISES[partes[-2]], int(partes[-1]), entrada["arquivo"]))
    return sorted(saida)


def esperado(
    corpo: list[dict[str, Any]], vivos: dict[str, dict[Any, str]]
) -> list[tuple[Any, ...]]:
    return sorted(
        (
            r["attributeId"],
            r["commodityCode"],
            r["countryCode"],
            int(r["marketYear"]),
            vivos["commodityAttributes"][r["attributeId"]],
            float(r["value"]),
            vivos["unitsOfMeasure"][r["unitId"]],
            int(r["calendarYear"]),
            int(r["month"]) or None,
        )
        for r in corpo
    )


def observado(df: pd.DataFrame) -> list[tuple[Any, ...]]:
    colunas = [
        "attribute_id",
        "commodity_code",
        "country_code",
        "market_year",
        "attribute",
        "value",
        "unit",
        "last_update_year",
        "last_update_month",
    ]
    return sorted(
        tuple(None if pd.isna(v) else v for v in linha)
        for linha in df[colunas].astype(object).itertuples(index=False)
    )


def identidade(df: pd.DataFrame) -> list[str]:
    def valor(rotulo: str) -> float:
        return float(df.loc[df["attribute_br"] == rotulo, "value"].sum())

    oferta = valor("estoque_inicial") + valor("producao") + valor("importacao")
    saida = (
        valor("exportacao") + valor("consumo_domestico") + valor("perdas") + valor("estoque_final")
    )
    problemas = []
    if abs(oferta - valor("oferta_total")) > 1e-6:
        problemas.append(f"oferta {oferta} × Total Supply {valor('oferta_total')}")
    if abs(saida - valor("distribuicao_total")) > 1e-6:
        problemas.append(f"distribuição {saida} × Total Distribution {valor('distribuicao_total')}")
    return problemas


def comparar(
    cliente: httpx.Client,
    produto: str,
    pais: str,
    ano: int,
    arquivo: str,
    vivos: dict[str, dict[Any, str]],
) -> dict[str, Any]:
    from agrobr import usda

    df, meta = asyncio.run(usda.psd(produto, country=pais, market_year=ano, return_meta=True))
    resposta = cliente.get(meta.source_url)
    resposta.raise_for_status()
    corpo = resposta.json()
    problemas = []
    if esperado(corpo, vivos) != observado(df):
        problemas.append(
            f"saída do agrobr ({len(df)} linhas) × corpo ao vivo ({len(corpo)} registros)"
        )
    problemas += identidade(df)
    golden = {r["attributeId"]: r for r in json.loads((GOLDEN / arquivo).read_bytes())}
    revisados = sorted(
        r["attributeId"]
        for r in corpo
        if r["attributeId"] in golden and r["value"] != golden[r["attributeId"]]["value"]
    )
    return {
        "status": "ok" if not problemas else "mismatch",
        "problems": problemas,
        "source_url": meta.source_url,
        "registros": len(corpo),
        "revisados_desde_o_golden": revisados,
        "ultima_atualizacao": sorted({(r["calendarYear"], r["month"]) for r in corpo}),
    }


def run(saida: Path) -> int:
    chave = os.environ["AGROBR_USDA_API_KEY"]
    resultados: dict[str, Any] = {}
    with httpx.Client(
        headers={"User-Agent": AGENT, "X-Api-Key": chave}, timeout=60, follow_redirects=True
    ) as cliente:
        resultados["catalogos"], vivos = conferir_catalogos(cliente)
        print("catalogos", resultados["catalogos"]["status"], flush=True)
        for produto, pais, ano, arquivo in cortes():
            caso = f"{produto}_{pais}_{ano}"
            resultados[caso] = comparar(cliente, produto, pais, ano, arquivo, vivos)
            print(caso, resultados[caso]["status"], resultados[caso]["problems"][:2], flush=True)
    relatorio = {
        "checked_at": datetime.now(UTC).isoformat(),
        "scope": (
            "gateway PSD ao vivo: catálogos oficiais × catálogos do pacote; 34 recortes das duas capturas independentes "
            "pela API pública do agrobr × leitura independente do corpo ao vivo com os catálogos ao vivo; "
            "identidade de oferta e distribuição; revisões desde o golden só informativas"
        ),
        "checks": resultados,
    }
    saida.parent.mkdir(parents=True, exist_ok=True)
    saida.write_text(
        json.dumps(relatorio, indent=2, ensure_ascii=False, default=str), encoding="utf-8"
    )
    falhas = sum(check["status"] != "ok" for check in resultados.values())
    print(f"{len(resultados) - falhas} ok / {falhas} mismatch")
    return int(falhas > 0)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Confere ao vivo o USDA PSD contra o gateway oficial"
    )
    parser.add_argument("--output", required=True, type=Path)
    return run(parser.parse_args().output)


if __name__ == "__main__":
    raise SystemExit(main())
