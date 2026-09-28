from __future__ import annotations

import argparse
import asyncio
import json
import os
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pandas as pd

GRAPHQL = "https://plataforma.alerta.mapbiomas.org/api/v2/graphql"
GOLDEN = Path(__file__).resolve().parents[1] / "tests/golden_data/mapbiomas_alerta/oficial_20260926"
AGENT = "agrobr-reconciliacao/1.0 (+https://github.com/bruno-portfolio/agrobr)"
LIMITE_REFERENCIA = 20000
CAIXA = (-55.0, -8.0, -50.0, -3.0)
REFERENCIA = """
query referencia($startDate: BaseDate, $endDate: BaseDate, $limit: Int, $boundingBox: [Float!]) {
  alerts(startDate: $startDate, endDate: $endDate, boundingBox: $boundingBox, page: 1,
         limit: $limit, sortField: ALERT_CODE, sortDirection: ASC) {
    metadata { totalCount totalPages }
    collection {
      alertCode areaHa detectedAt publishedAt statusName sources
      coordenates { latitude longitude }
    }
  }
}
"""

Linha = tuple[Any, ...]


def recortes(hoje: date) -> dict[str, dict[str, Any]]:
    primeiro = hoje.replace(day=1)
    fim_do_mes = primeiro.replace(year=primeiro.year - 1) - timedelta(days=1)
    return {
        "semana_do_golden": {"startDate": "2025-01-13", "endDate": "2025-01-19"},
        "janeiro_2025_ae39": {"startDate": "2025-01-01", "endDate": "2025-01-31"},
        "mes_de_um_ano_atras": {
            "startDate": fim_do_mes.replace(day=1).isoformat(),
            "endDate": fim_do_mes.isoformat(),
        },
        "janeiro_2025_caixa": {
            "startDate": "2025-01-01",
            "endDate": "2025-01-31",
            "boundingBox": list(CAIXA),
        },
    }


def consultar(cliente: httpx.Client, variables: dict[str, Any]) -> dict[str, Any]:
    resposta = cliente.post(GRAPHQL, json={"query": REFERENCIA, "variables": variables})
    resposta.raise_for_status()
    dado: dict[str, Any] = resposta.json()
    if "errors" in dado:
        raise RuntimeError(f"GraphQL: {dado['errors'][0].get('message')}")
    alertas: dict[str, Any] = dado["data"]["alerts"]
    return alertas


def esperado(colecao: list[dict[str, Any]]) -> list[Linha]:
    return [
        (
            a["alertCode"],
            round(float(a["areaHa"]), 4),
            a["detectedAt"][:10],
            (a["publishedAt"] or "")[:10],
            a["statusName"],
            ", ".join(a["sources"] or []),
            a["coordenates"]["latitude"],
            a["coordenates"]["longitude"],
        )
        for a in colecao
    ]


def observado(df: pd.DataFrame) -> list[Linha]:
    def dia(valor: Any) -> str:
        return "" if pd.isna(valor) else pd.Timestamp(valor).strftime("%Y-%m-%d")

    registros: list[dict[Any, Any]] = df.to_dict("records")
    return [
        (
            int(r["alert_code"]),
            round(float(r["area_ha"]), 4),
            dia(r["data_deteccao"]),
            dia(r["data_publicacao"]),
            r["status"],
            r["fonte"],
            r["lat"],
            r["lon"],
        )
        for r in registros
    ]


def comparar(cliente: httpx.Client, nome: str, variables: dict[str, Any]) -> dict[str, Any]:
    from agrobr import mapbiomas_alerta

    contagem = consultar(cliente, {**variables, "limit": 1})["metadata"]["totalCount"]
    if contagem > LIMITE_REFERENCIA:
        return {"status": "pendente", "problems": [f"{contagem} alertas: acima da referência"]}
    referencia = consultar(cliente, {**variables, "limit": max(contagem, 1)})
    caixa = variables.get("boundingBox")
    df, meta = asyncio.run(
        mapbiomas_alerta.alertas(
            start_date=variables["startDate"],
            end_date=variables["endDate"],
            bbox=tuple(caixa) if caixa else None,
            max_registros=None,
            return_meta=True,
        )
    )
    oficial = esperado(referencia["collection"])
    saida = observado(df)
    problemas = []
    if saida != oficial:
        faltam = sorted({linha[0] for linha in oficial} - {linha[0] for linha in saida})
        sobram = sorted({linha[0] for linha in saida} - {linha[0] for linha in oficial})
        problemas.append(
            f"saída ({len(saida)} linhas) × referência ({len(oficial)}); "
            f"faltam {faltam[:5]}, sobram {sobram[:5]}"
        )
    anunciado = referencia["metadata"]["totalCount"]
    if meta.records_count != anunciado or meta.source_details.get("total_anunciado") != anunciado:
        problemas.append(f"MetaInfo: {meta.records_count} linhas × totalCount {anunciado}")
    if meta.validation_warnings:
        problemas.append(f"avisos: {meta.validation_warnings}")
    resultado: dict[str, Any] = {
        "status": "ok" if not problemas else "mismatch",
        "problems": problemas,
        "alertas": anunciado,
        "area_ha": round(sum(float(a["areaHa"]) for a in referencia["collection"]), 4),
        "paginas_do_agrobr": len(meta.source_details.get("corpos", [])),
    }
    if nome == "semana_do_golden":
        golden = json.loads((GOLDEN / "oraculo_20260926.json").read_bytes())
        codigos = [a["alert_code"] for a in golden["alertas"]]
        resultado["revisados_desde_o_golden"] = codigos != [linha[0] for linha in oficial]
    return resultado


def run(saida: Path) -> int:
    token = os.environ["AGROBR_MAPBIOMAS_ALERTA_TOKEN"]
    resultados: dict[str, Any] = {}
    cabecalhos = {"User-Agent": AGENT, "Authorization": f"Bearer {token}"}
    with httpx.Client(headers=cabecalhos, timeout=180) as cliente:
        for nome, variables in recortes(datetime.now(UTC).date()).items():
            try:
                resultados[nome] = comparar(cliente, nome, variables)
            except (httpx.HTTPError, RuntimeError) as exc:
                resultados[nome] = {"status": "indisponivel", "problems": [type(exc).__name__]}
            print(nome, resultados[nome]["status"], resultados[nome]["problems"][:2], flush=True)
    relatorio = {
        "checked_at": datetime.now(UTC).isoformat(),
        "scope": (
            "GraphQL do MapBiomas Alerta ao vivo: a API pública do agrobr (paginada, max_registros=None) × "
            "a referência em página única (ALERT_CODE ASC) na semana do golden, em janeiro/2025, no "
            "mês fechado de um ano atrás (a publicação atrasa meses) e numa caixa; código, área, "
            "datas, status, fontes, coordenadas e totalCount"
        ),
        "checks": resultados,
    }
    saida.parent.mkdir(parents=True, exist_ok=True)
    saida.write_text(
        json.dumps(relatorio, indent=2, ensure_ascii=False, default=str), encoding="utf-8"
    )
    falhas = sum(check["status"] == "mismatch" for check in resultados.values())
    print(f"{len(resultados) - falhas} sem divergência / {falhas} mismatch")
    return int(falhas > 0)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Confere ao vivo o MapBiomas Alerta contra a referência em página única"
    )
    parser.add_argument("--output", required=True, type=Path)
    return run(parser.parse_args().output)


if __name__ == "__main__":
    raise SystemExit(main())
