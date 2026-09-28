from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

from agrobr import datasets, ibge
from agrobr.ibge import agregados
from tests.helpers import assert_replay_served, install_replay_http, sem_excecao

GOLDEN = Path(__file__).resolve().parents[1] / "golden_data"
PERIODOS = GOLDEN / "ibge/periodos_20260926"
PAM = GOLDEN / "ibge/pam_localidade_cod_20260925"
URL_PERIODOS = "https://servicodados.ibge.gov.br/api/v3/agregados/5457/periodos"
SIDRA = (
    "https://apisidra.ibge.gov.br/values/t/5457/n6/in N3 {uf}/h/n/p/2024/v/{variaveis}/c782/40124"
)
CODIGO_UF = {"DF": "53", "RR": "14"}
QUATRO = "8331,216,214,112"
CINCO = "8331,216,214,112,215"


@pytest.fixture
def sem_periodos_ibge() -> None:
    """Liga o pedido `/periodos` que o `conftest.py` desliga nos replays antigos."""


def _periodos_oficiais() -> list[dict[str, Any]]:
    manifesto = json.loads((PERIODOS / "manifest.json").read_bytes())["arquivos"]["5457.json"]
    corpo = (PERIODOS / "5457.json").read_bytes()
    assert (hashlib.sha256(corpo).hexdigest(), len(corpo)) == (
        manifesto["sha256"],
        manifesto["bytes"],
    )
    return json.loads(corpo)


def _servir(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    uf: str,
    variaveis: str,
    status_periodos: int = 200,
    corpo_periodos: bytes | None = None,
) -> dict:
    registros = json.loads((PAM / f"{uf}.json").read_text(encoding="utf-8"))[1:]
    if variaveis == QUATRO:
        registros = [registro for registro in registros if registro["D2C"] != "215"]
    no_formato_do_cliente = [
        {
            **registro,
            "D2C": registro["D3C"],
            "D2N": registro["D3N"],
            "D3C": registro["D2C"],
            "D3N": registro["D2N"],
        }
        for registro in registros
    ]
    (tmp_path / f"{uf}.json").write_text(json.dumps(no_formato_do_cliente), encoding="utf-8")
    (tmp_path / "5457.json").write_bytes(corpo_periodos or (PERIODOS / "5457.json").read_bytes())
    pedidos = [
        {
            "match": {
                "path": SIDRA.format(uf=CODIGO_UF[uf], variaveis=variaveis),
                "params": {},
                "skip": 0,
            },
            "file": f"{uf}.json",
            "content_type": "application/json",
        },
        {
            "match": {"path": URL_PERIODOS, "params": {}, "skip": 0},
            "file": "5457.json",
            "content_type": "application/json",
            "status": status_periodos,
        },
    ]
    return install_replay_http(monkeypatch, {"requests": pedidos}, tmp_path)


def _modificacao_de_2024() -> str:
    [periodo] = [item for item in _periodos_oficiais() if item["id"] == "2024"]
    dia, mes, ano = periodo["modificacao"].split("/")
    return f"{ano}-{mes}-{dia}"


async def test_periodos_publicam_a_data_em_iso_e_o_sentinela_nulo(monkeypatch, tmp_path):
    visto = _servir(monkeypatch, tmp_path, "DF", CINCO)
    with sem_excecao():
        datas = await agregados.fetch_periodos_modificacao("5457")
    oficiais = {item["id"]: item["modificacao"] for item in _periodos_oficiais()}
    assert list(datas) == list(oficiais)
    assert (oficiais["1974"], datas["1974"]) == ("17/04/2017", "2017-04-17")
    assert (oficiais["1975"], datas["1975"]) == ("01/01/0001", None)
    assert (oficiais["2024"], datas["2024"]) == ("17/09/2026", "2026-09-17")
    assert visto["served"] and not visto["unmatched"]


async def test_pam_publica_a_modificacao_do_periodo_devolvido(monkeypatch, tmp_path):
    visto = _servir(monkeypatch, tmp_path, "DF", CINCO)
    variaveis = ["area_plantada", "area_colhida", "producao", "rendimento", "valor_producao"]
    with sem_excecao():
        _, meta = await ibge.pam(
            "soja", ano=2024, uf="DF", nivel="municipio", variaveis=variaveis, return_meta=True
        )
    assert_replay_served(visto)
    assert _modificacao_de_2024() == "2026-09-17"
    assert meta.source_details.get("periodos_modificacao") == {"5457": {"2024": "2026-09-17"}}
    [consulta] = meta.source_details["consultas"]
    assert (consulta.get("tabela"), consulta.get("periodos_modificacao")) == (
        "5457",
        {"2024": "2026-09-17"},
    )
    assert "periodos_modificacao_erro" not in consulta


async def test_producao_anual_herda_a_modificacao(monkeypatch, tmp_path):
    visto = _servir(monkeypatch, tmp_path, "RR", QUATRO)
    with sem_excecao():
        frame, meta = await datasets.producao_anual(
            "soja", ano=2024, nivel="municipio", uf="RR", return_meta=True
        )
    assert_replay_served(visto)
    assert len(frame) > 0
    assert meta.source_details.get("periodos_modificacao") == {"5457": {"2024": "2026-09-17"}}


async def test_falha_no_metadado_nao_derruba_a_consulta(monkeypatch, tmp_path):
    visto = _servir(monkeypatch, tmp_path, "DF", CINCO, status_periodos=404)
    variaveis = ["area_plantada", "area_colhida", "producao", "rendimento", "valor_producao"]
    with sem_excecao():
        frame, meta = await ibge.pam(
            "soja", ano=2024, uf="DF", nivel="municipio", variaveis=variaveis, return_meta=True
        )
    assert len(visto["served"]) == 2
    assert len(frame) > 0
    [consulta] = meta.source_details["consultas"]
    assert consulta.get("tabela") == "5457"
    assert consulta["periodos_modificacao_erro"].startswith("SourceUnavailableError: ")
    assert "periodos_modificacao" not in meta.source_details
    assert "periodos_modificacao" not in consulta


@pytest.mark.parametrize(
    ("corpo", "esperado"),
    [
        (b"<html>manutencao</html>", "SourceUnavailableError: "),
        (b'{"id": "2024", "modificacao": "17/09/2026"}', "ParseError: "),
        (b"[]", {"2024": None}),
        (b'[{"id": "2024"}]', {"2024": None}),
        (b'[{"literals": ["2024"], "modificacao": "17/09/2026"}]', {"2024": None}),
        (b'[{"id": "2024", "modificacao": "setembro"}]', {"2024": None}),
    ],
    ids=["html", "objeto", "lista_vazia", "sem_modificacao", "sem_id", "data_ilegivel"],
)
async def test_corpo_ruim_do_periodos_nao_derruba_a_consulta(
    monkeypatch, tmp_path, corpo, esperado
):
    visto = _servir(monkeypatch, tmp_path, "DF", CINCO, corpo_periodos=corpo)
    variaveis = ["area_plantada", "area_colhida", "producao", "rendimento", "valor_producao"]
    with sem_excecao():
        frame, meta = await ibge.pam(
            "soja", ano=2024, uf="DF", nivel="municipio", variaveis=variaveis, return_meta=True
        )
    assert len(visto["served"]) == 2
    assert len(frame) > 0
    [consulta] = meta.source_details["consultas"]
    if isinstance(esperado, str):
        assert consulta.get("periodos_modificacao_erro", "").startswith(esperado)
        assert "periodos_modificacao" not in meta.source_details
    else:
        assert consulta.get("periodos_modificacao") == esperado
        assert meta.source_details.get("periodos_modificacao") == {"5457": esperado}
