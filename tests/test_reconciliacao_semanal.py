from __future__ import annotations

import json
import os
import textwrap
from pathlib import Path

import yaml

from scripts import reconciliacao_semanal as semanal

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github/workflows/reconciliacao.yml"
CABECALHO = """\
import argparse
import json
import os
import sys
import time
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--output", type=Path, required=True)
parser.add_argument("--capture-dir", type=Path)
parser.add_argument("--live", action="store_true")
args = parser.parse_args()


def gravar(relatorio):
    args.output.write_text(json.dumps(relatorio), encoding="utf-8")


def excecao(modulo, nome):
    classe = type(nome, (Exception,), {})
    classe.__module__ = modulo
    return classe

"""


def _raiz(tmp_path: Path, scripts: dict[str, str]) -> Path:
    pacote = tmp_path / "raiz" / "scripts"
    pacote.mkdir(parents=True)
    (pacote / "__init__.py").write_text("", encoding="utf-8")
    for nome, corpo in scripts.items():
        (pacote / f"reconciliar_{nome}.py").write_text(
            CABECALHO + textwrap.dedent(corpo), encoding="utf-8"
        )
    return pacote.parent


def _por_fonte(resumo: dict) -> dict[str, dict]:
    return {fonte["fonte"]: fonte for fonte in resumo["fontes"]}


def test_reconciliar_consolida_os_cinco_estados(tmp_path, monkeypatch):
    monkeypatch.delenv("AGROBR_USDA_API_KEY", raising=False)
    raiz = _raiz(
        tmp_path,
        {
            "ok": """
                print("✓ catálogo conferido")
                gravar({"checks": [{"case": "catalogo", "status": "ok"}]})
            """,
            "divergente": """
                gravar({"structure": [
                    {"case": "soja", "status": "mismatch", "problems": ["cabeçalho mudou"]},
                    {"case": "milho", "status": "ok"},
                ]})
                sys.exit(1)
            """,
            "pendente": """
                gravar({"structure": [{"case": "zip_live", "status": "pendente"}]})
                sys.exit(1)
            """,
            "fora_do_ar": 'raise excecao("httpx", "ConnectError")("[Errno -2] Name or service not known")',
            "quebrado": 'raise KeyError("features")',
            "usda": 'Path(args.output).with_name("rodou").write_text("x")',
            "icmbio": 'gravar({"checks": [{"status": "ok"}]})',
        },
    )
    pasta = tmp_path / "saida"

    resumo = semanal.reconciliar(raiz, pasta, [])

    fontes = _por_fonte(resumo)
    assert {nome: fonte["estado"] for nome, fonte in fontes.items()} == {
        "divergente": "mismatch",
        "fora_do_ar": "indisponível",
        "icmbio": "não verificado",
        "ok": "ok",
        "pendente": "indisponível",
        "quebrado": "erro do script",
        "usda": "não verificado",
    }
    assert fontes["divergente"]["contagem"] == {"mismatch": 1, "ok": 1}
    assert fontes["divergente"]["casos"] == [
        {"caso": "soja", "estado": "mismatch"},
        {"caso": "milho", "estado": "ok"},
    ]
    assert fontes["fora_do_ar"]["motivo"] == "ConnectError"
    assert fontes["quebrado"]["motivo"] == "KeyError"
    assert fontes["usda"]["motivo"] == "sem AGROBR_USDA_API_KEY"
    assert fontes["icmbio"]["motivo"] == "sem modo ao vivo (golden local)"
    assert not (pasta / "rodou").exists()
    assert resumo["contagem"] == {
        "mismatch": 1,
        "erro do script": 1,
        "indisponível": 2,
        "não verificado": 2,
        "ok": 1,
    }
    assert json.loads((pasta / "resumo.json").read_text(encoding="utf-8")) == resumo
    tabela = (pasta / "resumo.md").read_text(encoding="utf-8")
    assert "| divergente | mismatch | 1 mismatch, 1 ok | — |" in tabela
    assert "| usda | não verificado | — | sem AGROBR_USDA_API_KEY |" in tabela
    assert "cabeçalho mudou" not in tabela + json.dumps(resumo)


def test_rodar_erros_do_script_e_http(tmp_path):
    raiz = _raiz(
        tmp_path,
        {
            "servidor": """
                raise excecao("httpx", "HTTPStatusError")(
                    "Server error '503 Service Unavailable' for url 'https://x.gov.br/?token=abc'"
                )
            """,
            "bloqueio": """
                raise excecao("httpx", "HTTPStatusError")(
                    "Client error '403 Forbidden' for url 'https://x.gov.br/'"
                )
            """,
            "nao_encontrado": """
                raise excecao("httpx", "HTTPStatusError")(
                    "Client error '404 Not Found' for url 'https://x.gov.br/'"
                )
            """,
            "fonte_propria": """
                raise excecao("agrobr.exceptions", "SourceUnavailableError")("fora")
            """,
            "rodape": """
                import atexit
                atexit.register(lambda: print("sessão encerrada", file=sys.stderr))
                raise excecao("httpx", "ReadTimeout")("timed out")
            """,
            "encadeada": """
                try:
                    raise KeyError("features")
                except KeyError:
                    raise excecao("httpx", "ConnectTimeout")("timed out")
            """,
            "argumento": "sys.exit(2)",
            "incoerente": """
                gravar({"checks": [{"case": "a", "status": "ok"}]})
                sys.exit(1)
            """,
            "vazio": 'gravar({"resumo": 1})',
            "invalido": 'args.output.write_text("{", encoding="utf-8")',
            "desconhecido": """
                gravar({"checks": [{"status": "talvez"}, {"case": "sem_estado"}], "structure": "x"})
            """,
            "serie": """
                gravar({"results": [
                    {"product": "soja", "status": "error", "error_type": "ConnectTimeout"},
                    {"product": "milho", "status": "error", "error_type": "HTTPStatusError",
                     "error": "Server error '502 Bad Gateway' for url 'https://x'"},
                    {"product": "trigo", "status": "error", "error_type": "ParseError"},
                ]})
                sys.exit(1)
            """,
        },
    )

    fontes = _por_fonte(semanal.reconciliar(raiz, tmp_path / "saida", []))

    assert {nome: (fonte["estado"], fonte["motivo"]) for nome, fonte in fontes.items()} == {
        "argumento": ("erro do script", "saída 2 sem relatório"),
        "bloqueio": ("indisponível", "HTTPStatusError 403"),
        "desconhecido": ("erro do script", ""),
        "encadeada": ("indisponível", "ConnectTimeout"),
        "fonte_propria": ("indisponível", "SourceUnavailableError"),
        "incoerente": ("erro do script", "saída 1 com relatório ok"),
        "invalido": ("erro do script", "relatório sem casos"),
        "nao_encontrado": ("erro do script", "HTTPStatusError 404"),
        "rodape": ("indisponível", "ReadTimeout"),
        "serie": ("erro do script", ""),
        "servidor": ("indisponível", "HTTPStatusError 503"),
        "vazio": ("erro do script", "relatório sem casos"),
    }
    assert fontes["desconhecido"]["casos"] == [{"caso": "checks[0]", "estado": "erro do script"}]
    assert fontes["serie"]["casos"] == [
        {"caso": "soja", "estado": "indisponível"},
        {"caso": "milho", "estado": "indisponível"},
        {"caso": "trigo", "estado": "erro do script"},
    ]


def test_rodar_timeout(tmp_path, monkeypatch):
    monkeypatch.setattr(semanal, "TIMEOUT_S", 1)
    raiz = _raiz(tmp_path, {"lento": "time.sleep(30)"})

    resultado = semanal.rodar(raiz / "scripts/reconciliar_lento.py", tmp_path / "saida")

    assert resultado["estado"] == "erro do script"
    assert resultado["motivo"] == "timeout de 1 s"
    assert 1 <= resultado["duracao_s"] < 30


def test_rodar_credencial_presente_e_argumentos_ao_vivo(tmp_path, monkeypatch):
    monkeypatch.setattr(
        semanal, "ARGUMENTOS", {"captura": ("--live", "--capture-dir", "{captura}")}
    )
    raiz = _raiz(
        tmp_path,
        {
            "usda": 'gravar({"checks": [{"case": "psd", "status": "ok"}]})',
            "captura": """
                assert args.live and args.capture_dir.name == "captura_captura"
                assert Path(os.environ["AGROBR_CACHE_DIR"]).name == "captura_cache"
                assert "AGROBR_CACHE_CACHE_DIR" not in os.environ
                gravar({"checks": {"capturado": {"status": "ok"}}})
            """,
        },
    )
    pasta = tmp_path / "saida"
    pasta.mkdir()

    com_chave = semanal.rodar(
        raiz / "scripts/reconciliar_usda.py", pasta, {**os.environ, "AGROBR_USDA_API_KEY": "x"}
    )
    capturado = semanal.rodar(raiz / "scripts/reconciliar_captura.py", pasta)

    assert (com_chave["estado"], com_chave["casos"]) == ("ok", [{"caso": "psd", "estado": "ok"}])
    assert (capturado["estado"], capturado["casos"]) == (
        "ok",
        [{"caso": "capturado", "estado": "ok"}],
    )


def test_rodar_isola_o_cache_mesmo_com_a_pasta_do_usuario(tmp_path):
    raiz = _raiz(
        tmp_path,
        {
            "cache": """
                from agrobr.constants import CacheSettings
                pasta = CacheSettings().cache_dir
                assert pasta.name == "cache_cache", pasta
                gravar({"checks": {"cache": {"status": "ok"}}})
            """,
        },
    )
    pasta = tmp_path / "saida"
    pasta.mkdir()
    ambiente = {**os.environ, "AGROBR_CACHE_DIR": str(tmp_path / "usuario")}

    resultado = semanal.rodar(raiz / "scripts/reconciliar_cache.py", pasta, ambiente)

    assert (resultado["estado"], resultado["casos"]) == ("ok", [{"caso": "cache", "estado": "ok"}])


def test_texto_da_issue_sem_pii(tmp_path):
    raiz = _raiz(
        tmp_path,
        {
            "lista": """
                gravar({"checks": [
                    {"case": "Fulano de Tal", "status": "mismatch",
                     "problems": ["CPF 123.456.789-09 de Fulano de Tal", "token=ghp_segredo"]},
                    {"file": "12345678909.csv", "status": "mismatch",
                     "url": "https://x.gov.br/dados?token=segredo"},
                    {"id": "cnpj_12345678000199", "status": "mismatch"},
                    {"case": "cabecalho_csv", "status": "mismatch"},
                    {"case": "catalogo", "status": "ok"},
                ]})
                sys.exit(1)
            """,
        },
    )
    fonte = semanal.reconciliar(raiz, tmp_path / "saida", [])["fontes"][0]

    corpo = semanal.texto_da_issue(fonte, "https://github.com/o/r/actions/runs/1/artifacts/2")

    for proibido in (
        "Fulano",
        "123.456.789-09",
        "12345678909",
        "12345678000199",
        "ghp_",
        "segredo",
    ):
        assert proibido not in corpo
    assert (
        "**Casos com mismatch:** `checks[0]`, `checks[1]`, `checks[2]`, `cabecalho_csv`\n" in corpo
    )
    assert "**Contagem:** 4 mismatch, 1 ok" in corpo
    assert "https://github.com/o/r/actions/runs/1/artifacts/2" in corpo
    assert "`python -m scripts.reconciliacao_semanal lista`" in corpo


def test_abrir_issues_comenta_na_aberta_e_cria_a_nova():
    chamadas: list[list[str]] = []

    class Resposta:
        def __init__(self, stdout: str) -> None:
            self.stdout = stdout

    def rodar_gh(comando, **_opcoes):
        chamadas.append(comando)
        if comando[:3] != ["gh", "issue", "list"]:
            return Resposta("")
        abertas = [
            {"number": 7, "title": semanal.titulo("imea")},
            {"number": 9, "title": semanal.titulo("imea") + " (antiga)"},
        ]
        return Resposta(json.dumps(abertas))

    resumo = {
        "fontes": [
            {
                "fonte": fonte,
                "estado": estado,
                "contagem": {estado: 1},
                "casos": [{"caso": "soja", "estado": estado}],
            }
            for fonte, estado in (("imea", "mismatch"), ("ana", "mismatch"), ("sfb", "ok"))
        ]
    }

    comandos = semanal.abrir_issues(resumo, "https://link", rodar_gh)

    corpo_imea = semanal.texto_da_issue(resumo["fontes"][0], "https://link")
    corpo_ana = semanal.texto_da_issue(resumo["fontes"][1], "https://link")
    assert comandos == [
        ["gh", "issue", "comment", "7", "--body", corpo_imea],
        ["gh", "issue", "create", "--title", semanal.titulo("ana"), "--body", corpo_ana],
    ]
    assert [c for c in chamadas if c[:3] == ["gh", "issue", "list"]] == [
        [
            "gh",
            "issue",
            "list",
            "--state",
            "open",
            "--search",
            f'"{semanal.titulo(fonte)}" in:title',
            "--json",
            "number,title",
            "--limit",
            "50",
        ]
        for fonte in ("imea", "ana")
    ]
    assert len(chamadas) == 4


def test_main_codigo_de_saida_e_modo_issues(tmp_path, monkeypatch):
    raiz = _raiz(
        tmp_path,
        {
            "ok": 'gravar({"checks": [{"status": "ok"}]})',
            "pendente": 'gravar({"checks": [{"status": "indisponivel"}]})',
            "divergente": 'gravar({"checks": [{"status": "mismatch"}]})',
            "quebrado": 'raise KeyError("features")',
        },
    )
    monkeypatch.setattr(semanal, "ROOT", raiz)
    recebidos = []
    monkeypatch.setattr(semanal, "abrir_issues", lambda *args: recebidos.append(args))

    sem_falha = semanal.main(["ok", "pendente", "--saida", str(tmp_path / "a")])
    com_erro = semanal.main(["ok", "quebrado", "--saida", str(tmp_path / "c")])
    com_falha = semanal.main(["--saida", str(tmp_path / "b")])
    issues = semanal.main(
        ["--issues", str(tmp_path / "b" / "resumo.json"), "--artefato", "https://link"]
    )

    assert (sem_falha, com_erro, com_falha, issues) == (0, 1, 1, 0)
    resumo = json.loads((tmp_path / "a/resumo.json").read_bytes())
    assert [(f["fonte"], f["estado"]) for f in resumo["fontes"]] == [
        ("ok", "ok"),
        ("pendente", "indisponível"),
    ]
    assert resumo["contagem"] == {"indisponível": 1, "ok": 1}
    assert recebidos == [
        (json.loads((tmp_path / "b/resumo.json").read_text(encoding="utf-8")), "https://link")
    ]


def test_workflow_reconciliacao():
    texto = WORKFLOW.read_text(encoding="utf-8")
    workflow = yaml.safe_load(texto)
    gatilhos = workflow[True]
    passos = workflow["jobs"]["reconciliar"]["steps"]
    comandos = "\n".join(passo.get("run", "") for passo in passos)

    assert set(gatilhos) == {"schedule", "workflow_dispatch"}
    assert [item["cron"] for item in gatilhos["schedule"]] == ["0 9 * * 1"]
    assert workflow["permissions"] == {"contents": "read", "issues": "write"}
    assert "secrets." not in texto
    assert "python -m scripts.reconciliacao_semanal --saida reconciliacao" in comandos
    assert "python -m scripts.reconciliacao_semanal --issues reconciliacao/resumo.json" in comandos
    envio = next(p for p in passos if p.get("uses", "").startswith("actions/upload-artifact"))
    assert envio["with"]["path"] == "reconciliacao/resumo.*"
