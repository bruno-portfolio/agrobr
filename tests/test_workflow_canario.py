from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

yaml = pytest.importorskip("yaml")
WORKFLOWS = Path(__file__).parents[1] / ".github/workflows"
CANARIO = WORKFLOWS / "canario.yml"


def _carregar(path: Path) -> dict:
    if not path.exists():
        pytest.skip("Workflows não são distribuídos no sdist")
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _comandos(job: dict) -> str:
    return "\n".join(passo.get("run", "") for passo in job["steps"])


def test_canario_semanal_so_le_o_repositorio_fora_do_job_de_issue():
    workflow = _carregar(CANARIO)
    gatilhos = workflow[True]
    canario, issue = workflow["jobs"]["canario"], workflow["jobs"]["issue"]

    assert set(gatilhos) == {"schedule", "workflow_dispatch"}
    assert [item["cron"] for item in gatilhos["schedule"]] == ["0 9 * * 3"]
    assert workflow["permissions"] == {"contents": "read"}
    assert "permissions" not in canario
    assert issue["permissions"] == {"actions": "read", "issues": "write"}
    assert "secrets." not in CANARIO.read_text(encoding="utf-8")
    checkouts = [
        passo
        for job in workflow["jobs"].values()
        for passo in job["steps"]
        if passo.get("uses", "").startswith("actions/checkout@")
    ]
    assert checkouts
    assert all(passo["with"]["persist-credentials"] is False for passo in checkouts)


def test_canario_roda_estavel_e_pre_com_a_suite_da_ci_sem_bloquear():
    workflow = _carregar(CANARIO)
    canario = workflow["jobs"]["canario"]
    comandos = _comandos(canario)
    linha_da_ci = next(
        linha.strip()
        for linha in _comandos(
            _carregar(WORKFLOWS / "tests.yml")["jobs"]["minimum-compatibility"]
        ).splitlines()
        if "pytest tests/" in linha
    )

    assert canario["strategy"]["fail-fast"] is False
    assert canario["strategy"]["matrix"]["include"] == [
        {"modo": "estavel", "python-version": "3.11"},
        {"modo": "estavel", "python-version": "3.14"},
        {"modo": "pre", "python-version": "3.14"},
    ]
    assert 'pip install --upgrade $pre -e ".[dev,pdf,geo,polars]"' in comandos
    assert 'if [ "$MODO" = "pre" ]; then pre="--pre"; fi' in comandos
    assert linha_da_ci in comandos
    registro = [passo for passo in canario["steps"] if "pip freeze" in passo.get("run", "")]
    envio = [
        passo for passo in canario["steps"] if passo.get("uses", "").startswith("actions/upload")
    ]
    assert [passo["if"] for passo in registro + envio] == ["always()", "always()"]
    assert envio[0]["with"]["path"] == "canario/"


def test_issue_por_modo_comenta_a_aberta_e_nao_corre_em_execucao_cancelada():
    issue = _carregar(CANARIO)["jobs"]["issue"]
    comandos = _comandos(issue)

    assert issue["needs"] == "canario"
    assert issue["if"] == "${{ !cancelled() && needs.canario.result != 'success' }}"
    assert issue["steps"][-1]["env"]["GH_TOKEN"] == "${{ github.token }}"
    assert "for modo in estavel pre; do" in comandos
    assert comandos.count('titulo="Canário de dependências: ') == 2
    assert 'gh issue comment "$numero" --body-file "$corpo"' in comandos
    assert 'gh issue create --title "$titulo" --body-file "$corpo"' in comandos


@pytest.mark.skipif(sys.platform == "win32", reason="script Bash do runner Linux")
def test_issue_atribui_as_versoes_so_ao_job_que_gravou_o_artefato(tmp_path):
    workflow = _carregar(CANARIO)
    canario, issue = workflow["jobs"]["canario"], workflow["jobs"]["issue"]
    registro = next(passo for passo in canario["steps"] if "pip freeze" in passo.get("run", ""))
    envio = next(
        passo for passo in canario["steps"] if passo.get("uses", "").startswith("actions/upload")
    )
    download = next(passo for passo in issue["steps"] if "uses" in passo)
    assert download["with"]["merge-multiple"] is True
    pasta = (
        registro["env"]["DESTINO"]
        .removeprefix(envio["with"]["path"])
        .replace("${{ matrix.modo }}", "estavel")
        .replace("${{ matrix.python-version }}", "3.11")
    )
    artefato = tmp_path / download["with"]["path"] / pasta
    artefato.mkdir(parents=True)
    (artefato / "nucleo.txt").write_text("httpx==0.28.1\npandas==3.0.5\n", encoding="utf-8")
    stubs = (
        "gh() {\n"
        '  case "$1 $2" in\n'
        '    "api repos/"*) printf "canario (estavel, 3.11)\\tfailure\\n'
        'canario (estavel, 3.14)\\tcancelled\\ncanario (pre, 3.14)\\tcancelled\\n" ;;\n'
        '    "issue list") echo "[]" ;;\n'
        '    *) echo "$*" >> chamadas.txt ;;\n'
        "  esac\n"
        "}\n"
        "jq() { cat > /dev/null; }\n"
    )
    script = tmp_path / "issue.sh"
    script.write_text(stubs + _comandos(issue), encoding="utf-8", newline="\n")
    subprocess.run(
        ["bash", script.name],
        cwd=tmp_path,
        env={**os.environ, "GH_REPO": "dono/agrobr", "RUN_ID": "1", "RUN_URL": "https://execucao"},
        check=True,
    )
    estavel = (tmp_path / "corpo-estavel.md").read_text(encoding="utf-8")
    pre = (tmp_path / "corpo-pre.md").read_text(encoding="utf-8")

    assert "### Python 3.11: `failure`\n\n```\nhttpx==0.28.1\npandas==3.0.5\n```" in estavel
    assert "### Python 3.14: `cancelled`\n\nSem versões registradas" in estavel
    assert "httpx" not in pre
    assert "### Python 3.14: `cancelled`\n\nSem versões registradas" in pre
    chamadas = (tmp_path / "chamadas.txt").read_text(encoding="utf-8").splitlines()
    assert [chamada.split(" --title")[0] for chamada in chamadas] == ["issue create"] * 2


def test_canario_usa_as_versoes_das_actions_dos_outros_workflows():
    usados = {
        passo["uses"]
        for job in _carregar(CANARIO)["jobs"].values()
        for passo in job["steps"]
        if "uses" in passo
    }
    outros = {
        passo["uses"]
        for path in sorted(WORKFLOWS.glob("*.yml"))
        if path != CANARIO
        for job in _carregar(path)["jobs"].values()
        for passo in job.get("steps", [])
        if "uses" in passo
    }
    assert usados <= outros, json.dumps(sorted(usados - outros))
