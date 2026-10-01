from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import httpx
import pandas as pd
import pytest

from agrobr import exceptions
from scripts import reconciliacao_semanal
from scripts import reconciliar_acervo_fundiario as reconciliacao


@pytest.mark.parametrize(
    ("bruto", "publicado", "status"),
    [
        ("31/12/1899", None, "ok"),
        ("01/01/2100", None, "ok"),
        ("01/01/1900", "1900-01-01", "ok"),
        ("31/12/2099", "2099-12-31", "ok"),
        ("31/02/2023", None, "ok"),
        ("05/09/2023", "2023-09-05", "ok"),
        ("05/09/2023", "2023-05-09", "mismatch"),
        ("05/09/2023", None, "mismatch"),
    ],
)
def test_assentamentos_data_obtencao(monkeypatch, bruto, publicado, status):
    linha = dict.fromkeys(
        [
            "cd_sipra",
            "nome_proje",
            "municipio",
            "uf",
            "area_hecta",
            "capacidade",
            "num_famili",
            "fase",
            "data_de_cr",
            "forma_obte",
            "data_obten",
            "area_calc_",
            "sr",
            "descricao_",
        ],
        "",
    )
    linha["data_obten"] = bruto
    registro = dict.fromkeys(
        [
            "codigo_sipra",
            "nome_projeto",
            "municipio",
            "uf",
            "area_ha",
            "capacidade",
            "num_familias",
            "fase",
            "data_criacao",
            "forma_obtencao",
            "data_obtencao",
            "area_calc_ha",
            "sr",
            "descricao_fase",
        ]
    )
    registro["data_obtencao"] = pd.Timestamp(publicado) if publicado else pd.NaT
    frame = pd.DataFrame([registro])
    meta = SimpleNamespace(source_url=reconciliacao.url_of("assentamentos", None))
    geo = SimpleNamespace(geometry=[None], crs=SimpleNamespace(to_epsg=lambda: 4674))
    monkeypatch.setattr(reconciliacao, "dbf", lambda _arquivo: [linha])
    monkeypatch.setattr(reconciliacao, "shapes", lambda _arquivo: [None])
    monkeypatch.setattr(reconciliacao, "member", lambda *_args: reconciliacao.SIRGAS_2000.encode())

    resultado = reconciliacao.compare("assentamentos", None, b"", (frame, meta, geo, []))

    assert resultado["status"] == status
    assert resultado["problems"] == (
        [] if status == "ok" else ["data_obtencao: 1 células divergentes"]
    )


@pytest.mark.parametrize("bruto", ["", "00000000", "00010101", "18991231", "21000101", "20230231"])
def test_data_dbf_nula_ou_fora_da_regra(bruto):
    assert reconciliacao.dbf_date(bruto) is None


def test_sigef_datas_dbf_publicadas():
    golden = Path(__file__).parent / "golden_data/acervo_fundiario/sigef_ap_20260922/response.zip"
    linha = reconciliacao.dbf(golden.read_bytes())[0]
    assert (linha["data_submi"], linha["data_aprov"], linha["registro_d"]) == (
        "20191209",
        "20191209",
        "00000000",
    )

    registro = reconciliacao.sigef(linha)

    assert registro["data_submissao"] == pd.Timestamp(2019, 12, 9)
    assert registro["data_aprovacao"] == pd.Timestamp(2019, 12, 9)
    assert registro["registro_data"] is None


@pytest.mark.parametrize("etapa", ["agrobr", "captura"])
@pytest.mark.parametrize("divergencia", [False, True])
def test_run_404_snci_preserva_outros_resultados(monkeypatch, tmp_path, etapa, divergencia):
    visitadas = []
    comparadas = []

    async def saida(tema, uf):
        visitadas.append((tema, uf))
        if (tema, uf) == ("snci", "RR") and etapa == "agrobr":
            raise exceptions.SourceUnavailableError(
                source="acervo_fundiario",
                url=reconciliacao.url_of(tema, uf),
                last_error="HTTP 404 — recurso não disponível no servidor INCRA",
            )
        return pd.DataFrame(), None, None, []

    def head(_cliente, url):
        status = 404 if url == reconciliacao.url_of("snci", "RR") else 200
        return httpx.Response(status, request=httpx.Request("HEAD", url))

    def captura(_cliente, url, path):
        status = 404 if url == reconciliacao.url_of("snci", "RR") else 200
        path.write_bytes(b"arquivo")
        return {"status": status, "sha256": "hash"}

    def comparar(tema, uf, _arquivo, _saida):
        comparadas.append((tema, uf))
        falha = divergencia and (tema, uf) == ("snci", "AL")
        return {
            "status": "mismatch" if falha else "ok",
            "problems": ["data divergente"] if falha else [],
        }

    monkeypatch.setattr(httpx.Client, "head", head)
    monkeypatch.setattr(reconciliacao, "agrobr_outputs", saida)
    monkeypatch.setattr(reconciliacao, "fetch", captura)
    monkeypatch.setattr(reconciliacao, "compare", comparar)
    monkeypatch.setattr(reconciliacao, "cached_sha", lambda *_args: "hash")
    monkeypatch.setattr(
        reconciliacao.tempfile, "mkdtemp", lambda **_kwargs: str(tmp_path / "cache")
    )
    monkeypatch.setenv("AGROBR_CACHE_CACHE_DIR", str(tmp_path / "cache"))
    output = tmp_path / "resultado.json"

    codigo = reconciliacao.run(tmp_path / "capturas", output, [], ["SC", "RR", "AL"])
    checks = json.loads(output.read_text(encoding="utf-8"))["checks"]

    assert visitadas == [("snci", "SC"), ("snci", "RR"), ("snci", "AL"), ("assentamentos", None)]
    assert comparadas == [("snci", "SC"), ("snci", "AL"), ("assentamentos", None)]
    assert checks["snci_RR"]["status"] == "indisponivel"
    assert checks["cobertura"]["status"] == "indisponivel"
    assert checks["snci_SC"]["status"] == checks["assentamentos"]["status"] == "ok"
    assert checks["snci_AL"]["status"] == ("mismatch" if divergencia else "ok")
    assert codigo == int(divergencia)
    estado, _, _ = reconciliacao_semanal.classificar(codigo, {"checks": checks}, "")
    assert estado == ("mismatch" if divergencia else "indisponível")


def test_cobertura_erro_distinto_de_404_segue_mismatch(monkeypatch):
    monkeypatch.setattr(
        httpx.Client,
        "head",
        lambda _cliente, url: httpx.Response(
            500 if url == reconciliacao.url_of("snci", "RR") else 200,
            request=httpx.Request("HEAD", url),
        ),
    )
    with httpx.Client() as cliente:
        resultado = reconciliacao.coverage(cliente)
    assert resultado["status"] == "mismatch"
    assert resultado["problems"] == ["snci RR: HTTP 500"]


def test_run_nao_oculta_erro_distinto_de_404(monkeypatch, tmp_path):
    async def saida(_tema, _uf):
        raise exceptions.SourceUnavailableError(source="acervo_fundiario", last_error="HTTP 500")

    monkeypatch.setattr(reconciliacao, "agrobr_outputs", saida)
    monkeypatch.setattr(reconciliacao, "coverage", lambda _cliente: {"status": "ok"})
    monkeypatch.setattr(
        reconciliacao.tempfile, "mkdtemp", lambda **_kwargs: str(tmp_path / "cache")
    )
    monkeypatch.setenv("AGROBR_CACHE_CACHE_DIR", str(tmp_path / "cache"))

    with pytest.raises(exceptions.SourceUnavailableError, match="HTTP 500"):
        reconciliacao.run(tmp_path / "capturas", tmp_path / "resultado.json", [], ["RR"])
