from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import re
import warnings
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import pandas as pd

MEDIA = "https://fundacaorioverde.com.br/wp-json/wp/v2/media"
ORACULO = (
    Path(__file__).resolve().parents[1]
    / "tests"
    / "golden_data"
    / "rio_verde"
    / "oraculo_20260923.json"
)
AGENT = "agrobr-reconciliacao/1.0 (+https://github.com/bruno-portfolio/agrobr)"
SOJA = re.compile(r"competicao-de-cultivares-de-soja", re.IGNORECASE)


def safra_do_arquivo(url: str) -> str | None:
    nome = url.rsplit("/", 1)[-1]
    if not SOJA.search(nome) or not nome.lower().endswith(".pdf"):
        return None
    resto = re.split(r"safra", nome, flags=re.IGNORECASE)[-1]
    grupos = re.findall(r"\d+", resto)
    if len(grupos) == 1 and len(grupos[0]) == 6:
        inicio = int(grupos[0][:4])
    elif len(grupos) == 2:
        inicio = int(grupos[0]) if len(grupos[0]) == 4 else 2000 + int(grupos[0])
    else:
        return None
    return f"{inicio}/{inicio + 1}"


def publicadas(cliente: httpx.Client) -> dict[str, set[str]]:
    safras: dict[str, set[str]] = {}
    for termo in ("Competicao", "Cultivares de Soja"):
        resposta = cliente.get(MEDIA, params={"search": termo, "per_page": "100"})
        resposta.raise_for_status()
        for item in resposta.json():
            url = str(item.get("source_url", ""))
            if item.get("mime_type") != "application/pdf":
                continue
            safra = safra_do_arquivo(url)
            if safra is not None:
                safras.setdefault(safra, set()).add(url)
    return safras


def numero(texto: str) -> float | None:
    return None if texto == "-" else float(texto.replace(",", "."))


def esperado(safra: str, linhas: list[dict[str, str]]) -> list[dict[str, Any]]:
    return [
        {
            "safra": safra,
            "empresa": linha["empresa"],
            "cultivar": linha["cultivar"],
            "grupo_maturacao": linha["gm"],
            "ciclo_dias": int(linha["ciclo"]),
            "produtividade_1_epoca_sc_ha": numero(linha["e1"]),
            "produtividade_2_epoca_sc_ha": numero(linha["e2"]),
            "produtividade_3_epoca_sc_ha": numero(linha["e3"]),
            "produtividade_4_epoca_sc_ha": numero(linha["e4"]),
            "produtividade_media_sc_ha": numero(linha["media"]),
        }
        for linha in linhas
    ]


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
    return linhas


async def saida_agrobr(safra: str) -> tuple[pd.DataFrame, Any]:
    from agrobr import rio_verde

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return await rio_verde.ensaio_soja(safra, return_meta=True)


def comparar(cliente: httpx.Client, safra: str, dados: dict[str, Any]) -> dict[str, Any]:
    resposta = cliente.get(dados["url"])
    resposta.raise_for_status()
    problemas: list[str] = []
    sha = hashlib.sha256(resposta.content).hexdigest()
    if sha != dados["sha256"]:
        problemas.append(f"PDF republicado: SHA {sha[:16]} × oráculo {dados['sha256'][:16]}")
    frame, meta = asyncio.run(saida_agrobr(safra))
    if meta.source_url != dados["url"]:
        problemas.append(f"source_url {meta.source_url}")
    oficiais = esperado(safra, dados["linhas"])
    observadas = publicado(frame)
    if observadas != oficiais:
        divergentes = sum(1 for a, b in zip(observadas, oficiais, strict=False) if a != b)
        problemas.append(
            f"linhas: agrobr {len(observadas)} × oráculo {len(oficiais)}; {divergentes} divergentes"
        )
    return {
        "status": "ok" if not problemas else "mismatch",
        "problems": problemas,
        "linhas": len(oficiais),
        "celulas": len(oficiais) * 9,
        "sha256": sha,
        "last_modified": resposta.headers.get("last-modified"),
    }


def catalogo(cliente: httpx.Client) -> dict[str, Any]:
    from agrobr.rio_verde import models

    achadas = publicadas(cliente)
    conhecidas = set(models.SAFRAS_URLS) | set(models.SAFRAS_FORA_DO_LAYOUT)
    novas = sorted(set(achadas) - conhecidas)
    sumidas = sorted(set(models.SAFRAS_URLS) - set(achadas))
    problemas = [f"safra publicada fora do catálogo: {s} {sorted(achadas[s])}" for s in novas]
    problemas += [f"safra do catálogo sem PDF na API de mídia: {s}" for s in sumidas]
    return {
        "status": "ok" if not problemas else "mismatch",
        "problems": problemas,
        "publicadas": {s: sorted(u) for s, u in sorted(achadas.items())},
    }


def run(saida: Path) -> int:
    oraculo = json.loads(ORACULO.read_bytes())
    resultados: dict[str, Any] = {}
    with httpx.Client(
        headers={"User-Agent": AGENT}, timeout=httpx.Timeout(60, read=180), follow_redirects=True
    ) as cliente:
        resultados["catalogo"] = catalogo(cliente)
        print(
            "catalogo",
            resultados["catalogo"]["status"],
            resultados["catalogo"]["problems"][:3],
            flush=True,
        )
        for safra, dados in oraculo["safras"].items():
            resultados[safra] = comparar(cliente, safra, dados)
            print(safra, resultados[safra]["status"], resultados[safra]["problems"][:3], flush=True)
    relatorio = {
        "checked_at": datetime.now(UTC).isoformat(),
        "scope": (
            "PDF oficial de cada safra (SHA) × oráculo transcrito por posição de palavra × saída pública do agrobr; "
            "API de mídia do WordPress da fundação × SAFRAS_URLS e SAFRAS_FORA_DO_LAYOUT"
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
        description="Confere ao vivo os ensaios de soja da Fundação Rio Verde"
    )
    parser.add_argument("--output", required=True, type=Path)
    return run(parser.parse_args().output)


if __name__ == "__main__":
    raise SystemExit(main())
