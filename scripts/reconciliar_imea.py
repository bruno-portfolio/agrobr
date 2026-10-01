from __future__ import annotations

import argparse
import asyncio
import collections
import json
import warnings
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import pandas as pd

BASE = "https://api1.imea.com.br/api/v2/mobile/cadeias"
AGENT = "agrobr-reconciliacao/1.0 (+https://github.com/bruno-portfolio/agrobr)"
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
FILTROS: list[tuple[str, int, dict[str, str]]] = [
    ("soja_safra_24_25", 4, {"safra": "24/25"}),
    ("soja_rs_sc", 4, {"unidade": "R$/sc"}),
]
CHAVE = ("indicador_id", "localidade", "data_publicacao", "safra", "unidade")


def esperado(cadeia: int, registro: dict[str, Any], nomes: dict[str, str]) -> dict[str, Any]:
    return {
        "cadeia": CADEIAS[cadeia],
        "indicador_id": str(registro["IndicadorFinalId"]),
        "indicador": nomes.get(str(registro["IndicadorFinalId"])),
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
    return tuple("" if linha[c] is None else str(linha[c]) for c in CHAVE)


def ordem(linha: dict[str, Any]) -> tuple[tuple[str, ...], str]:
    return chave(linha), json.dumps(linha, sort_keys=True, default=str)


def publicado(frame: pd.DataFrame) -> list[dict[str, Any]]:
    linhas = []
    for registro in frame.to_dict("records"):
        linha: dict[str, Any] = {}
        for coluna, valor in registro.items():
            if valor is None or (not isinstance(valor, str) and pd.isna(valor)):
                linha[str(coluna)] = None
            else:
                linha[str(coluna)] = valor.item() if hasattr(valor, "item") else valor
        linhas.append(linha)
    return sorted(linhas, key=ordem)


async def saida_agrobr(nome: str, filtro: dict[str, str]) -> tuple[pd.DataFrame, Any]:
    from agrobr import imea

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return await imea.cotacoes(
            nome, safra=filtro.get("safra"), unidade=filtro.get("unidade"), return_meta=True
        )


def oficial(cliente: httpx.Client, cadeia: int) -> tuple[list[dict[str, Any]], dict[str, str]]:
    cotacoes = cliente.get(f"{BASE}/{cadeia}/cotacoes")
    cotacoes.raise_for_status()
    indicadores = cliente.get(f"{BASE}/{cadeia}/indicadores")
    indicadores.raise_for_status()
    nomes = {str(item["Id"]): str(item["Nome"]) for item in indicadores.json()}
    registros: list[dict[str, Any]] = cotacoes.json()
    return registros, nomes


def comparar(cliente: httpx.Client, cadeia: int, filtro: dict[str, str]) -> dict[str, Any]:
    registros, nomes = oficial(cliente, cadeia)
    frame, meta = asyncio.run(saida_agrobr(CADEIAS[cadeia], filtro))
    campo = {"safra": "Safra", "unidade": "UnidadeSigla"}
    selecionados = [r for r in registros if all(r[campo[k]] == v for k, v in filtro.items())]
    todas = [esperado(cadeia, r, nomes) for r in selecionados]
    unicas = {json.dumps(linha, sort_keys=True, default=str): linha for linha in todas}
    oficiais = sorted(unicas.values(), key=ordem)
    observadas = publicado(frame)
    problemas: list[str] = []
    pendencias: list[str] = []
    if len(observadas) != len(oficiais):
        problemas.append(f"linhas: agrobr {len(observadas)} × oficiais {len(oficiais)}")
    divergentes = sum(1 for a, b in zip(observadas, oficiais, strict=False) if a != b)
    if divergentes:
        problemas.append(f"{divergentes} linhas divergentes")
    distintas = {json.dumps(linha, sort_keys=True, default=str): linha for linha in observadas}
    if len(distintas) != len(observadas):
        problemas.append(
            f"{len(observadas) - len(distintas)} linhas repetidas, iguais em todas as colunas"
        )
    contagem = collections.Counter(map(chave, distintas.values()))
    repetidas = sum(n - 1 for n in contagem.values() if n > 1)
    if repetidas:
        linhas_repetidas = sum(n for n in contagem.values() if n > 1)
        avisadas = meta.source_details.get("chaves_repetidas", {}).get("linhas")
        if avisadas == linhas_repetidas:
            pendencias.append(
                f"{linhas_repetidas} linhas com chaves repetidas e valores diferentes, "
                "avisadas no MetaInfo"
            )
        else:
            problemas.append(f"{repetidas} linhas repetem a chave com valores diferentes")
    colapsadas = len(todas) - len(unicas)
    informado = meta.source_details.get("duplicatas_colapsadas", {}).get("linhas")
    if informado != colapsadas:
        problemas.append(f"duplicatas colapsadas: agrobr {informado} × oficiais {colapsadas}")
    sem_nome = sum(1 for linha in observadas if linha["indicador"] is None)
    if sem_nome:
        problemas.append(f"{sem_nome} linhas sem nome de indicador")
    if meta.source_url != f"{BASE}/{cadeia}/cotacoes":
        problemas.append(f"source_url {meta.source_url}")
    if meta.source_details.get("indicadores_url") != f"{BASE}/{cadeia}/indicadores":
        problemas.append(f"indicadores_url {meta.source_details}")
    return {
        "status": "mismatch" if problemas else "pendente" if pendencias else "ok",
        "problems": problemas,
        "pendencias": pendencias,
        "linhas_oficiais": len(oficiais),
        "duplicatas_oficiais": colapsadas,
        "linhas_publicadas": len(observadas),
        "celulas": len(oficiais) * 10,
    }


def catalogo(cliente: httpx.Client) -> dict[str, Any]:
    resposta = cliente.get(BASE)
    resposta.raise_for_status()
    ativas = sorted(
        int(c["Id"]) for c in resposta.json() if c.get("Ativo") and c.get("ExibeIndicadores")
    )
    problemas = (
        [] if set(ativas) == set(CADEIAS) else [f"cadeias ativas {ativas} × {sorted(CADEIAS)}"]
    )
    return {
        "status": "ok" if not problemas else "mismatch",
        "problems": problemas,
        "ativas": ativas,
    }


def run(saida: Path) -> int:
    resultados: dict[str, Any] = {}
    with httpx.Client(
        headers={"User-Agent": AGENT}, timeout=httpx.Timeout(60, read=120)
    ) as cliente:
        resultados["catalogo"] = catalogo(cliente)
        for cadeia, nome in CADEIAS.items():
            resultados[nome] = comparar(cliente, cadeia, {})
            print(nome, resultados[nome]["status"], resultados[nome]["problems"][:3], flush=True)
        for nome, cadeia, filtro in FILTROS:
            resultados[nome] = comparar(cliente, cadeia, filtro)
            print(nome, resultados[nome]["status"], resultados[nome]["problems"][:3], flush=True)
    relatorio = {
        "checked_at": datetime.now(UTC).isoformat(),
        "scope": (
            "cotações e catálogo de indicadores de cada cadeia ativa lidos com json × saída pública do agrobr; "
            "linhas comparadas pela chave indicador_id + localidade + data + safra + unidade; catálogo de cadeias ativas"
        ),
        "checks": resultados,
    }
    saida.parent.mkdir(parents=True, exist_ok=True)
    saida.write_text(
        json.dumps(relatorio, indent=2, ensure_ascii=False, default=str), encoding="utf-8"
    )
    falhas = sum(check["status"] == "mismatch" for check in resultados.values())
    pendentes = sum(check["status"] == "pendente" for check in resultados.values())
    print(f"{len(resultados) - falhas - pendentes} ok / {falhas} mismatch / {pendentes} pendente")
    return int(falhas > 0)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Confere ao vivo as cotações do IMEA contra a API oficial"
    )
    parser.add_argument("--output", required=True, type=Path)
    return run(parser.parse_args().output)


if __name__ == "__main__":
    raise SystemExit(main())
