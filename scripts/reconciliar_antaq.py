from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests

from agrobr.antaq import client, models

ROOT = Path(__file__).resolve().parents[1]
GOLDEN = ROOT / "tests/golden_data/reconciliacao_r13_20260918"
FONTE = ROOT / "tests/golden_data/antaq/movimentacao_sample"
MANIFEST = GOLDEN / "manifest.json"
ORACLE = GOLDEN / "oracle.json"
ANO = 2024

COLUNAS_DECLARADAS = {
    "2024Atracacao.txt": models.COLUNAS_ATRACACAO,
    "2024Carga.txt": models.COLUNAS_CARGA,
    "Mercadoria.txt": models.COLUNAS_MERCADORIA,
}


def cabecalho(nome: str) -> list[str]:
    texto = (FONTE / nome).read_bytes().decode("utf-8")
    return next(csv.reader(io.StringIO(texto), delimiter=";"))


def comparar_membro(membro: str, arquivo: str, inventario: list[dict[str, Any]]) -> dict[str, Any]:
    publicadas = cabecalho(arquivo)
    declaradas = COLUNAS_DECLARADAS[membro]
    decididas = [
        item["campo"] for item in inventario if item["locator"]["archive_member"] == membro
    ]
    problemas: list[str] = []
    for nome in publicadas:
        if nome not in decididas:
            problemas.append(f"campo publicado sem decisao no manifesto: {nome}")
    for nome in decididas:
        if nome not in publicadas:
            problemas.append(f"decisao sem campo correspondente na fonte: {nome}")
    if decididas != publicadas and not problemas:
        problemas.append("ordem das decisoes diverge do cabecalho publicado")
    for nome in declaradas:
        if nome not in publicadas:
            problemas.append(f"coluna exigida pelo parser ausente na fonte: {nome}")
    return {
        "status": "mismatch" if problemas else "ok",
        "problems": problemas,
        "campos_publicados": len(publicadas),
        "campos_decididos": len(decididas),
        "colunas_consumidas_pelo_parser": len(declaradas),
    }


def conferir_corpos(manifesto: dict[str, Any]) -> dict[str, Any]:
    problemas: list[str] = []
    for entrada in manifesto["files"]:
        caminho = (GOLDEN / entrada["file"]).resolve()
        if not caminho.exists():
            problemas.append(f"corpo ausente: {entrada['file']}")
            continue
        dados = caminho.read_bytes()
        if hashlib.sha256(dados).hexdigest() != entrada["sha256"]:
            problemas.append(f"sha256 divergente: {entrada['file']}")
        if len(dados) != entrada["bytes"]:
            problemas.append(f"tamanho divergente: {entrada['file']}")
    return {
        "status": "mismatch" if problemas else "ok",
        "problems": problemas,
        "corpos": len(manifesto["files"]),
    }


def conferir_oraculo(manifesto: dict[str, Any], oraculo: dict[str, Any]) -> dict[str, Any]:
    problemas: list[str] = []
    caso = next(item for item in manifesto["cases"] if item["id"] == "fonte_2024_completo")
    if caso["columns"] != oraculo["fonte"]["colunas"]:
        problemas.append("colunas do caso divergem do oraculo")
    if len(oraculo["fonte"]["linhas"]) != caso["period"]["rows"]:
        problemas.append("contagem de linhas do oraculo diverge do manifesto")
    for identificador, dados in oraculo["filtros"].items():
        esperado = next(item for item in manifesto["cases"] if item["id"] == identificador)
        if len(dados["linhas"]) != esperado["period"]["rows"]:
            problemas.append(f"filtro {identificador}: contagem divergente")
    return {
        "status": "mismatch" if problemas else "ok",
        "problems": problemas,
        "linhas_fonte": len(oraculo["fonte"]["linhas"]),
        "linhas_dataset": len(oraculo["dataset"]["linhas"]),
        "celulas_comparaveis": len(oraculo["fonte"]["linhas"]) * len(oraculo["fonte"]["colunas"])
        + len(oraculo["dataset"]["linhas"]) * len(oraculo["dataset"]["colunas"]),
        "filtros": len(oraculo["filtros"]),
    }


def sondar_live(ano: int) -> dict[str, Any]:
    """Uma única requisição, sem retry: registra a pendência nominal da fonte fora do ar."""
    url = f"{client.BULK_TXT_BASE}/{ano}.zip"
    registro: dict[str, Any] = {
        "case": "antaq_zip_anual_live",
        "requested_url": url,
        "probed_at": datetime.now(UTC).isoformat(),
        "retries": 0,
    }
    try:
        download = client._get_sync(url)
    except requests.exceptions.RequestException as erro:
        registro.update(
            status="pendente",
            problems=[f"{type(erro).__name__}: {erro}"],
            aquisicao="falhou",
        )
        return registro
    zip_valido = download.content.startswith(b"PK\x03\x04")
    registro.update(
        final_url=download.final_url,
        content_type=download.content_type,
        bytes=len(download.content),
        sha256=hashlib.sha256(download.content).hexdigest(),
        is_zip=zip_valido,
        status="ok" if zip_valido else "pendente",
        problems=[]
        if zip_valido
        else [
            "resposta nao e ZIP (assinatura PK ausente): a ANTAQ segue fora do ar desde 2026-06-23"
        ],
        aquisicao="concluida" if zip_valido else "indisponivel",
    )
    return registro


def executar(output: Path, *, live: bool, ano: int) -> int:
    manifesto = json.loads(MANIFEST.read_text(encoding="utf-8"))
    oraculo = json.loads(ORACLE.read_text(encoding="utf-8"))
    inventario = next(item for item in manifesto["cases"] if item["id"] == "fonte_2024_completo")[
        "structure"
    ]

    relatorio: dict[str, Any] = {
        "fetched_at": datetime.now(UTC).isoformat(),
        "modo": "live" if live else "offline",
        "structure": [
            {"case": f"n1_{membro}", **comparar_membro(membro, arquivo, inventario)}
            for membro, arquivo in (
                ("2024Atracacao.txt", "atracacao.txt"),
                ("2024Carga.txt", "carga.txt"),
                ("Mercadoria.txt", "mercadoria.txt"),
            )
        ],
    }
    relatorio["structure"].append({"case": "corpos_preservados", **conferir_corpos(manifesto)})
    relatorio["structure"].append({"case": "oraculo_n2", **conferir_oraculo(manifesto, oraculo)})
    if live:
        relatorio["structure"].append(sondar_live(ano))

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(relatorio, indent=1, ensure_ascii=False), encoding="utf-8")
    pendentes = [item for item in relatorio["structure"] if item["status"] != "ok"]
    for item in relatorio["structure"]:
        print(item["status"], item["case"], "; ".join(item.get("problems", [])))
    print(
        f"{len(relatorio['structure']) - len(pendentes)} ok / {len(pendentes)} pendente -> {output}"
    )
    return 1 if pendentes else 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Reconciliação ANTAQ: N1 dos TXT preservados contra o manifesto e o parser; "
            "--live faz uma única sondagem do ZIP anual e registra a pendência nominal"
        )
    )
    parser.add_argument("--live", action="store_true", help="sonda a fonte uma vez, sem retry")
    parser.add_argument("--ano", type=int, default=ANO)
    parser.add_argument(
        "--output",
        type=Path,
        default=ROOT / f"reports/reconciliacao_antaq_{datetime.now(UTC):%Y%m%d}.json",
    )
    argumentos = parser.parse_args()
    return executar(argumentos.output, live=argumentos.live, ano=argumentos.ano)


if __name__ == "__main__":
    sys.exit(main())
