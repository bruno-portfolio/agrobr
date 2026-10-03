from __future__ import annotations

import argparse
import asyncio
import json
import re
import warnings
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import httpx
import pandas as pd

BASE = "https://mapas.florestal.gov.br/server/rest/services"
CNFP = f"{BASE}/Hosted/CNFP_v19_03_retificado_17072025/FeatureServer/9"
CONCESSOES = f"{BASE}/Hosted/unidades_concessoes_florestais/FeatureServer/0"
IFN_PONTOS = f"{BASE}/DadosAbertos-IFN/dataset_ifn_tb_pontos_lote/FeatureServer/0"
IFN_LOTES = f"{BASE}/DadosAbertos-IFN/dataset_ifn_tb_lote/FeatureServer/23"
EDICOES_CONHECIDAS = {
    "Hosted/CNFP_2024",
    "Hosted/cnfp_v2_2024",
    "Hosted/CNFP_v19_03_retificado_17072025",
}
AGENT = "agrobr-reconciliacao/1.0 (+https://github.com/bruno-portfolio/agrobr)"
ANO = re.compile(r"(?<![0-9])[0-9]{4}(?![0-9])")
PAGINA = 2000
CAMPOS_CNFP = {
    "nome": "nome",
    "uf": "uf",
    "bioma": "bioma",
    "categoria": "categoria",
    "tipo": "tipo",
    "governo": "governo",
    "classe": "classe",
    "area_ha": "area_ha",
    "municipio": "municipio",
    "ano_criacao_texto": "anocriacao",
}
CAMPOS_CONCESSOES = {
    "nome": "nome_uc",
    "uf": "uf",
    "bioma": "bioma",
    "area_ha": "hectares",
    "grupo": "grupo",
    "categoria": "cat_nome",
}


def consulta(camada: str, **params: str | int) -> str:
    return f"{camada}/query?{urlencode(params)}"


def ano(valor: object) -> int | None:
    if isinstance(valor, int):
        return valor
    anos = set(ANO.findall(valor)) if isinstance(valor, str) else set()
    return int(anos.pop()) if len(anos) == 1 else None


def nulo(valor: Any) -> Any:
    if valor is None or (not isinstance(valor, str) and pd.isna(valor)):
        return None
    return valor.item() if hasattr(valor, "item") else valor


def oficiais(
    cliente: httpx.Client, camada: str, geometria: bool = False, *, where: str = "1=1"
) -> list[dict[str, Any]]:
    resposta = cliente.get(consulta(camada, where=where, returnIdsOnly="true", f="json"))
    resposta.raise_for_status()
    dados = resposta.json()
    oid, ids = dados["objectIdFieldName"], sorted(dados["objectIds"])
    feicoes: list[dict[str, Any]] = []
    for i in range(0, len(ids), PAGINA):
        fim = ids[min(i + PAGINA, len(ids)) - 1]
        faixa = f"{oid} >= {ids[i]} AND {oid} <= {fim}"
        resposta = cliente.get(
            consulta(
                camada,
                where=faixa if where == "1=1" else f"({where}) AND {faixa}",
                outFields="*",
                returnGeometry=str(geometria).lower(),
                outSR=4326,
                orderByFields=oid,
                resultRecordCount=PAGINA,
                f="json",
            )
        )
        resposta.raise_for_status()
        feicoes += resposta.json()["features"]
    return feicoes


def comparar(
    publicado: pd.DataFrame, feicoes: list[dict[str, Any]], campos: dict[str, str], data: str
) -> dict[str, Any]:
    problemas: list[str] = []
    fids = [int(f) for f in publicado["fid"]]
    if len(fids) != len(set(fids)):
        problemas.append(f"{len(fids) - len(set(fids))} fid repetidos")
    oficiais_por_fid = {int(f["attributes"]["fid"]): f["attributes"] for f in feicoes}
    if set(fids) != set(oficiais_por_fid):
        problemas.append(
            f"fids: {len(set(oficiais_por_fid) - set(fids))} faltando, "
            f"{len(set(fids) - set(oficiais_por_fid))} sobrando"
        )
    divergentes = 0
    for registro in publicado.to_dict("records"):
        oficial = oficiais_por_fid.get(int(registro["fid"]))
        if oficial is None:
            continue
        esperado = {coluna: oficial[campo] for coluna, campo in campos.items()}
        esperado["ano_criacao"] = ano(oficial[data])
        observado = {coluna: nulo(registro[coluna]) for coluna in esperado}
        divergentes += observado != esperado
    if divergentes:
        problemas.append(f"{divergentes} linhas divergentes")
    return {
        "status": "ok" if not problemas else "mismatch",
        "problems": problemas,
        "linhas": len(publicado),
        "celulas": len(publicado) * (len(campos) + 1),
        "anos_publicados": int(publicado["ano_criacao"].notna().sum()),
    }


def comparar_ifn(
    publicado: pd.DataFrame,
    pontos: list[dict[str, Any]],
    lotes: list[dict[str, Any]],
) -> dict[str, Any]:
    problemas: list[str] = []
    cadastro = {f["attributes"]["co_lote"]: f["attributes"]["no_lote"] for f in lotes}
    if len(cadastro) != len(lotes):
        problemas.append("códigos duplicados no cadastro de lotes")
    esperados = {f["attributes"]["co_pontos_lote"]: f["attributes"] for f in pontos}
    if len(esperados) != len(pontos):
        problemas.append("IDs duplicados na fonte IFN")
    ids = [nulo(valor) for valor in publicado["id"]]
    if len(ids) != len(set(ids)):
        problemas.append("IDs IFN duplicados na saída")
    if set(ids) != set(esperados):
        problemas.append("IDs IFN diferem da fonte")
    campos = {
        "codigo_lote": "co_lote",
        "conglomerado": "no_conglomerado",
        "uf": "no_uf",
        "municipio": "no_municipio",
        "bioma": "no_bioma",
        "ciclo": "nu_ciclo_execucao",
    }
    divergentes = 0
    for registro in publicado.to_dict("records"):
        ponto = esperados.get(nulo(registro["id"]))
        if ponto is None:
            continue
        code = ponto["co_lote"]
        if code is not None and code not in cadastro:
            problemas.append(f"lote {code} ausente no cadastro oficial")
        esperado = {coluna: ponto[campo] for coluna, campo in campos.items()}
        esperado["lote"] = cadastro.get(code)
        divergentes += {coluna: nulo(registro[coluna]) for coluna in esperado} != esperado
    if divergentes:
        problemas.append(f"{divergentes} linhas IFN divergentes")
    if not pontos:
        problemas.append("recorte IFN/DF sem pontos publicados")
    return {
        "status": "ok" if not problemas else "mismatch",
        "problems": problemas,
        "linhas": len(publicado),
        "celulas": len(publicado) * 8,
    }


def sem_orientacao(aneis: list[list[list[float]]]) -> list[list[list[float]]]:
    return sorted(min(anel, anel[::-1]) for anel in aneis)


def comparar_geometria(publicado: Any, feicoes: list[dict[str, Any]]) -> dict[str, Any]:
    oficiais_por_fid = {
        int(f["attributes"]["fid"]): sem_orientacao(f["geometry"]["rings"]) for f in feicoes
    }
    divergentes = 0
    for fid, geometria in zip(publicado["fid"], publicado.geometry, strict=True):
        poligonos = getattr(geometria, "geoms", [geometria])
        aneis = [p.exterior for p in poligonos] + [i for p in poligonos for i in p.interiors]
        observado = sem_orientacao([[list(ponto) for ponto in anel.coords] for anel in aneis])
        divergentes += observado != oficiais_por_fid.get(int(fid))
    problemas = [f"{divergentes} geometrias divergentes"] if divergentes else []
    if len(publicado) != len(oficiais_por_fid):
        problemas.append(f"feições: agrobr {len(publicado)} × oficial {len(oficiais_por_fid)}")
    if publicado.crs is None or publicado.crs.to_epsg() != 4326:
        problemas.append(f"CRS {publicado.crs}")
    return {
        "status": "ok" if not problemas else "mismatch",
        "problems": problemas,
        "feicoes": len(publicado),
    }


def edicoes(cliente: httpx.Client) -> dict[str, Any]:
    resposta = cliente.get(f"{BASE}/Hosted", params={"f": "json"})
    resposta.raise_for_status()
    nomes = {s["name"] for s in resposta.json().get("services", []) if "cnfp" in s["name"].lower()}
    novas = sorted(nomes - EDICOES_CONHECIDAS)
    return {
        "status": "ok" if not novas else "mismatch",
        "problems": [f"edição do CNFP não conhecida: {n}" for n in novas],
        "publicadas": sorted(nomes),
    }


async def saidas() -> dict[str, Any]:
    from agrobr import sfb
    from agrobr.exceptions import SourceUnavailableError

    resultado: dict[str, Any] = {}
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        resultado["cnfp"] = await sfb.cnfp()
        resultado["concessoes"] = await sfb.concessoes()
        resultado["cnfp_geo_df"] = await sfb.cnfp_geo(uf="DF")
        resultado["concessoes_geo"] = await sfb.concessoes_geo()
        try:
            resultado["ifn"] = await sfb.ifn_conglomerados(uf="DF")
        except SourceUnavailableError as exc:
            resultado["ifn"] = exc
    return resultado


def run(saida: Path) -> int:
    publicados = asyncio.run(saidas())
    resultados: dict[str, Any] = {}
    with httpx.Client(
        headers={"User-Agent": AGENT}, timeout=httpx.Timeout(60, read=300), follow_redirects=True
    ) as cliente:
        resultados["edicoes"] = edicoes(cliente)
        resultados["cnfp"] = comparar(
            publicados["cnfp"], oficiais(cliente, CNFP), CAMPOS_CNFP, "anocriacao"
        )
        resultados["concessoes"] = comparar(
            publicados["concessoes"], oficiais(cliente, CONCESSOES), CAMPOS_CONCESSOES, "criacao"
        )
        resposta = cliente.get(
            consulta(
                CNFP,
                where="uf='DF'",
                outFields="fid",
                outSR=4326,
                orderByFields="fid",
                f="json",
            )
        )
        resposta.raise_for_status()
        resultados["cnfp_geo_df"] = comparar_geometria(
            publicados["cnfp_geo_df"], resposta.json()["features"]
        )
        resultados["concessoes_geo"] = comparar_geometria(
            publicados["concessoes_geo"], oficiais(cliente, CONCESSOES, geometria=True)
        )
        ifn = publicados["ifn"]
        if isinstance(ifn, Exception):
            resultados["ifn"] = {"status": "indisponivel", "problems": [str(ifn)[:200]]}
        else:
            pontos = oficiais(cliente, IFN_PONTOS, where="no_uf='DF'")
            codes = sorted(
                {
                    f["attributes"]["co_lote"]
                    for f in pontos
                    if f["attributes"]["co_lote"] is not None
                }
            )
            lotes = (
                oficiais(
                    cliente,
                    IFN_LOTES,
                    where=f"co_lote IN ({','.join(map(str, codes))})",
                )
                if codes
                else []
            )
            resultados["ifn"] = comparar_ifn(ifn, pontos, lotes)
    for nome, check in resultados.items():
        print(nome, check["status"], check["problems"][:3], flush=True)
    relatorio = {
        "checked_at": datetime.now(UTC).isoformat(),
        "scope": (
            "CNFP e concessões inteiros: saída pública do agrobr × leitura independente por faixas de fid "
            "(orderByFields, sem geometria), com a regra do ano de criação; geometria do CNFP/DF e das "
            "concessões × anéis ESRI; edições do CNFP publicadas no ArcGIS; IFN/DF: IDs, "
            "atributos e nomes de lote por junção independente em co_lote"
        ),
        "checks": resultados,
    }
    saida.parent.mkdir(parents=True, exist_ok=True)
    saida.write_text(
        json.dumps(relatorio, indent=2, ensure_ascii=False, default=str), encoding="utf-8"
    )
    falhas = sum(check["status"] == "mismatch" for check in resultados.values())
    indisponiveis = sum(check["status"] == "indisponivel" for check in resultados.values())
    print(
        f"{len(resultados) - falhas - indisponiveis} ok / {falhas} mismatch / {indisponiveis} indisponível"
    )
    return int(falhas > 0)


def main() -> int:
    parser = argparse.ArgumentParser(description="Confere ao vivo as camadas do SFB")
    parser.add_argument("--output", required=True, type=Path)
    return run(parser.parse_args().output)


if __name__ == "__main__":
    raise SystemExit(main())
