from __future__ import annotations

import json
from pathlib import Path

import pytest

yaml = pytest.importorskip("yaml")
WORKFLOWS = Path(__file__).parents[1] / ".github/workflows"


@pytest.mark.parametrize("workflow", ["landing_data.yml", "explorer_data.yml"])
def test_checkout_nao_persiste_credencial_e_token_so_aparece_no_push(workflow):
    path = WORKFLOWS / workflow
    if not path.exists():
        pytest.skip("Workflows não são distribuídos no sdist")
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    steps = payload["jobs"]["update"]["steps"]
    checkouts = [step for step in steps if step.get("uses", "").startswith("actions/checkout@")]
    assert checkouts
    for step in checkouts:
        assert step.get("with", {}).get("persist-credentials") is False
        assert "token" not in step.get("with", {})

    com_token = [step for step in steps if "secrets.LANDING_PUSH_TOKEN" in json.dumps(step)]
    assert len(com_token) == 1
    push = com_token[0]
    assert push["env"] == {"GH_TOKEN": "${{ secrets.LANDING_PUSH_TOKEN }}"}
    assert " push origin HEAD:main" in push["run"]
    assert "credential.helper=!gh auth git-credential" in push["run"]
    assert "git config" not in push["run"]
    assert "steps.commit.outputs.changed" in push["if"]
    assert "secrets.LANDING_PUSH_TOKEN" not in json.dumps(payload.get("env", {}))
    assert "secrets.LANDING_PUSH_TOKEN" not in json.dumps(payload["jobs"]["update"].get("env", {}))


@pytest.mark.parametrize("workflow", ["landing_data.yml", "explorer_data.yml"])
def test_publicacao_dos_dados_preserva_rebase_e_docs_condicionais(workflow):
    path = WORKFLOWS / workflow
    if not path.exists():
        pytest.skip("Workflows não são distribuídos no sdist")
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    steps = payload["jobs"]["update"]["steps"]
    commits = [step for step in steps if step.get("id") == "commit"]
    assert len(commits) == 1
    commit = commits[0]
    assert "git diff --staged --quiet" in commit["run"]
    assert "git pull --rebase origin main" in commit["run"]
    assert 'echo "changed=true" >> "$GITHUB_OUTPUT"' in commit["run"]
    docs = [step for step in steps if "gh workflow run docs.yml --ref main" in step.get("run", "")]
    assert len(docs) == 1
    assert "steps.commit.outputs.changed" in docs[0].get("if", "")
    assert docs[0]["env"] == {"GH_TOKEN": "${{ github.token }}"}
