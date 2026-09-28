from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
from collections import Counter
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
ARGUMENTOS: dict[str, tuple[str, ...]] = {
    "acervo_fundiario": ("--capture-dir", "{captura}"),
    "antaq": ("--live",),
    "antt_pedagio": ("--capture-dir", "{captura}"),
    "incra": ("--capture-dir", "{captura}"),
}
FORA_DO_SEMANAL: dict[str, str] = {
    "agrofit": "sem modo ao vivo (corpos capturados antes)",
    "anp_precos": "sem modo ao vivo (corpos capturados antes)",
    "psr": "sem modo ao vivo (corpos capturados antes)",
    "rnc": "sem modo ao vivo (corpos capturados antes)",
    "zarc": "sem modo ao vivo (corpos capturados antes)",
    "boletins": "perfil fixo de uma edição (toda edição nova diverge)",
    "icmbio": "sem modo ao vivo (golden local)",
    "lista_suja": "sem modo ao vivo (golden local)",
    "sicar": "sem modo ao vivo (golden local)",
}
CREDENCIAIS: dict[str, str] = {
    "usda": "AGROBR_USDA_API_KEY",
    "mapbiomas_alerta": "AGROBR_MAPBIOMAS_ALERTA_TOKEN",
}
TIMEOUT_S = 1800
ORDEM = ("mismatch", "erro do script", "indisponível", "não verificado", "ok")
FALHAS = frozenset({"mismatch", "erro do script"})
ESTADOS_BRUTOS = {
    "ok": "ok",
    "mismatch": "mismatch",
    "indisponivel": "indisponível",
    "pendente": "indisponível",
}
FONTE_FORA = frozenset(
    {
        "ConnectError",
        "ConnectTimeout",
        "ConnectionError",
        "PoolTimeout",
        "ReadError",
        "ReadTimeout",
        "RemoteProtocolError",
        "SourceUnavailableError",
        "Timeout",
        "WriteTimeout",
    }
)
HTTP_CODIGO = re.compile(r"'(\d{3}) ")
HTTP_FORA = frozenset({"403", "429"})
EXCECAO = re.compile(
    r"(?P<classe>[A-Za-z_][\w.]*(?:Error|Exception|Timeout))(?:: (?P<mensagem>.*))?"
)
CONTEINERES = ("checks", "structure", "results")
ROTULOS = ("case", "id", "product", "file")
CASO_SEGURO = re.compile(r"[\w.:-]{1,80}")
DOCUMENTO = re.compile(r"\d{11}")

Executor = Callable[..., subprocess.CompletedProcess[str]]


def titulo(fonte: str) -> str:
    return f"Reconciliação semanal: mismatch em {fonte}"


def _codigo_http(classe: str, mensagem: str) -> str:
    achado = HTTP_CODIGO.search(mensagem) if classe.endswith("HTTPStatusError") else None
    return achado[1] if achado else ""


def _fonte_fora(classe: str, mensagem: str) -> bool:
    codigo = _codigo_http(classe, mensagem)
    return classe.rsplit(".", 1)[-1] in FONTE_FORA or codigo.startswith("5") or codigo in HTTP_FORA


def _motivo(classe: str, mensagem: str) -> str:
    return " ".join(filter(None, (classe.rsplit(".", 1)[-1], _codigo_http(classe, mensagem))))


def _seguro(rotulo: object) -> bool:
    return (
        isinstance(rotulo, str)
        and CASO_SEGURO.fullmatch(rotulo) is not None
        and DOCUMENTO.search(rotulo) is None
    )


def _estado_do_caso(caso: dict[str, Any]) -> str:
    if caso["status"] == "error":
        tipo, mensagem = str(caso.get("error_type", "")), str(caso.get("error", ""))
        return "indisponível" if _fonte_fora(tipo, mensagem) else "erro do script"
    return ESTADOS_BRUTOS.get(caso["status"], "erro do script")


def casos(relatorio: Any) -> list[dict[str, str]]:
    achados = []
    for conteiner in CONTEINERES:
        itens = relatorio.get(conteiner) if isinstance(relatorio, dict) else None
        if isinstance(itens, dict):
            pares = [([chave], caso) for chave, caso in itens.items()]
        elif isinstance(itens, list):
            pares = [
                ([caso.get(c) for c in ROTULOS], caso) for caso in itens if isinstance(caso, dict)
            ]
        else:
            continue
        for posicao, (candidatos, caso) in enumerate(pares):
            if isinstance(caso, dict) and isinstance(caso.get("status"), str):
                rotulo = next((c for c in candidatos if _seguro(c)), f"{conteiner}[{posicao}]")
                achados.append({"caso": rotulo, "estado": _estado_do_caso(caso)})
    return achados


def _ultima_excecao(stderr: str) -> tuple[str, str]:
    for linha in reversed(stderr.strip().splitlines()):
        achado = EXCECAO.fullmatch(linha.strip())
        if achado:
            return achado["classe"], achado["mensagem"] or ""
    return "", ""


def classificar(
    codigo: int, relatorio: Any | None, stderr: str
) -> tuple[str, list[dict[str, str]], str]:
    if relatorio is None:
        classe, mensagem = _ultima_excecao(stderr)
        estado = "indisponível" if _fonte_fora(classe, mensagem) else "erro do script"
        return estado, [], _motivo(classe, mensagem) if classe else f"saída {codigo} sem relatório"
    achados = casos(relatorio)
    if not achados:
        return "erro do script", [], "relatório sem casos"
    estado = next(e for e in ORDEM if any(caso["estado"] == e for caso in achados))
    if codigo not in (0, 1) or (codigo == 1 and estado == "ok"):
        return "erro do script", achados, f"saída {codigo} com relatório {estado}"
    return estado, achados, ""


def rodar(script: Path, pasta: Path, ambiente: dict[str, str] | None = None) -> dict[str, Any]:
    fonte = script.stem.removeprefix("reconciliar_")
    ambiente = dict(os.environ if ambiente is None else ambiente)
    resultado: dict[str, Any] = {"fonte": fonte, "casos": [], "duracao_s": 0.0}
    if fonte in FORA_DO_SEMANAL:
        return resultado | {"estado": "não verificado", "motivo": FORA_DO_SEMANAL[fonte]}
    credencial = CREDENCIAIS.get(fonte)
    if credencial and not ambiente.get(credencial):
        return resultado | {"estado": "não verificado", "motivo": f"sem {credencial}"}
    saida = pasta / f"{fonte}.json"
    argumentos = [a.format(captura=pasta / f"{fonte}_captura") for a in ARGUMENTOS.get(fonte, ())]
    limite = TIMEOUT_S
    inicio = time.monotonic()
    try:
        processo = subprocess.run(
            [
                sys.executable,
                "-m",
                f"{script.parent.name}.{script.stem}",
                *argumentos,
                "--output",
                str(saida),
            ],
            cwd=script.parent.parent,
            env=ambiente
            | {
                "AGROBR_CACHE_CACHE_DIR": str(pasta / f"{fonte}_cache"),
                "PYTHONIOENCODING": "utf-8",
            },
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=limite,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return resultado | {
            "estado": "erro do script",
            "motivo": f"timeout de {limite} s",
            "duracao_s": round(time.monotonic() - inicio, 1),
        }
    try:
        relatorio = json.loads(saida.read_text(encoding="utf-8")) if saida.exists() else None
    except ValueError:
        relatorio = {}
    estado, achados, motivo = classificar(processo.returncode, relatorio, processo.stderr)
    return resultado | {
        "estado": estado,
        "casos": achados,
        "motivo": motivo,
        "duracao_s": round(time.monotonic() - inicio, 1),
    }


def resumir(resultados: list[dict[str, Any]]) -> dict[str, Any]:
    fontes = []
    for resultado in resultados:
        contagem = Counter(caso["estado"] for caso in resultado["casos"])
        fontes.append(resultado | {"contagem": {e: contagem[e] for e in ORDEM if contagem[e]}})
    total = Counter(fonte["estado"] for fonte in fontes)
    return {
        "gerado_em": datetime.now(UTC).isoformat(timespec="seconds"),
        "contagem": {e: total[e] for e in ORDEM if total[e]},
        "fontes": fontes,
    }


def markdown(resumo: dict[str, Any]) -> str:
    linhas = [
        "# Reconciliação semanal",
        "",
        f"Gerado em {resumo['gerado_em']}: "
        + ", ".join(f"{n} {e}" for e, n in resumo["contagem"].items()),
        "",
        "| Fonte | Estado | Casos | Motivo | Duração (s) |",
        "| --- | --- | --- | --- | --- |",
    ]
    for fonte in resumo["fontes"]:
        contagem = ", ".join(f"{n} {e}" for e, n in fonte["contagem"].items()) or "—"
        linhas.append(
            f"| {fonte['fonte']} | {fonte['estado']} | {contagem} | {fonte['motivo'] or '—'} "
            f"| {fonte['duracao_s']} |"
        )
    return "\n".join(linhas) + "\n"


def texto_da_issue(fonte: dict[str, Any], artefato: str) -> str:
    divergentes = [caso["caso"] for caso in fonte["casos"] if caso["estado"] == "mismatch"]
    contagem = ", ".join(f"{n} {e}" for e, n in fonte["contagem"].items())
    return "\n".join(
        [
            f"**Estado:** {fonte['estado']}",
            f"**Contagem:** {contagem}",
            f"**Casos com mismatch:** {', '.join(f'`{caso}`' for caso in divergentes)}",
            f"**Resumo da execução:** {artefato}",
            "",
            f"Para reproduzir: `python -m scripts.reconciliacao_semanal {fonte['fonte']}`. O JSON do"
            f" script fica em `reports/reconciliacao_semanal/{fonte['fonte']}.json`.",
        ]
    )


def issue_aberta(fonte: str, rodar_gh: Executor = subprocess.run) -> int | None:
    procurado = titulo(fonte)
    saida = rodar_gh(
        [
            "gh",
            "issue",
            "list",
            "--state",
            "open",
            "--search",
            f'"{procurado}" in:title',
            "--json",
            "number,title",
            "--limit",
            "50",
        ],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    return next((item["number"] for item in json.loads(saida) if item["title"] == procurado), None)


def comando_da_issue(fonte: str, corpo: str, numero: int | None) -> list[str]:
    if numero is None:
        return ["gh", "issue", "create", "--title", titulo(fonte), "--body", corpo]
    return ["gh", "issue", "comment", str(numero), "--body", corpo]


def abrir_issues(
    resumo: dict[str, Any], artefato: str, rodar_gh: Executor = subprocess.run
) -> list[list[str]]:
    comandos = []
    for fonte in resumo["fontes"]:
        if fonte["estado"] != "mismatch":
            continue
        comando = comando_da_issue(
            fonte["fonte"], texto_da_issue(fonte, artefato), issue_aberta(fonte["fonte"], rodar_gh)
        )
        rodar_gh(comando, check=True)
        comandos.append(comando)
    return comandos


def reconciliar(raiz: Path, pasta: Path, fontes: list[str]) -> dict[str, Any]:
    pasta.mkdir(parents=True, exist_ok=True)
    scripts = sorted((raiz / "scripts").glob("reconciliar_*.py"))
    escolhidos = [s for s in scripts if not fontes or s.stem.removeprefix("reconciliar_") in fontes]
    resultados = []
    for script in escolhidos:
        resultado = rodar(script, pasta)
        print(resultado["fonte"], resultado["estado"], resultado["motivo"], flush=True)
        resultados.append(resultado)
    resumo = resumir(resultados)
    (pasta / "resumo.json").write_text(
        json.dumps(resumo, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    (pasta / "resumo.md").write_text(markdown(resumo), encoding="utf-8")
    return resumo


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Roda os scripts/reconciliar_*.py ao vivo e consolida os estados"
    )
    parser.add_argument("fontes", nargs="*", help="só estas fontes (padrão: todas)")
    parser.add_argument("--saida", type=Path, default=ROOT / "reports/reconciliacao_semanal")
    parser.add_argument("--issues", type=Path, help="resumo.json de uma execução anterior")
    parser.add_argument("--artefato", default="", help="link do artefato com o resumo")
    argumentos = parser.parse_args(argv)
    if argumentos.issues:
        resumo = json.loads(argumentos.issues.read_text(encoding="utf-8"))
        abrir_issues(resumo, argumentos.artefato)
        return 0
    resumo = reconciliar(ROOT, argumentos.saida, argumentos.fontes)
    return int(any(fonte["estado"] in FALHAS for fonte in resumo["fontes"]))


if __name__ == "__main__":
    raise SystemExit(main())
