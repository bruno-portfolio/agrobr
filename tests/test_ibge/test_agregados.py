from __future__ import annotations

import json
import warnings
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, unquote, urlsplit

import httpx
import pandas as pd
import pytest

from agrobr import contracts, datasets, ibge
from agrobr.exceptions import SourceUnavailableError
from agrobr.ibge import agregados, client
from agrobr.utils.warnings import warn_once_reset
from tests.helpers import (
    assert_replay_samples,
    assert_replay_served,
    collect_failures,
    install_replay_http,
    sem_excecao,
)

GOLDEN = Path(__file__).resolve().parents[1] / "golden_data/reconciliacao_r15_20260918"
RECEIPTS = json.loads((GOLDEN / "agregados/receipts.json").read_text(encoding="utf-8"))
SUMMARY = {
    entry["call"]: entry
    for entry in json.loads((GOLDEN / "agregados/summary.json").read_text(encoding="utf-8"))
}
MANIFEST = json.loads((GOLDEN / "manifest.json").read_text(encoding="utf-8"))
ORACLE_CASES = {case["id"]: case for case in MANIFEST["cases"]}
LSPA_SIDRA = json.loads(
    (GOLDEN.parent / "ibge/lspa_catalogo_oficial/202607.json").read_text(encoding="utf-8")
)
CALLS = {
    "pam_soja_uf_2023": lambda: ibge.pam("soja", ano=2023, nivel="uf", return_meta=True),
    "pam_milho_brasil_2022_2023": lambda: ibge.pam(
        "milho", ano=[2022, 2023], nivel="brasil", return_meta=True
    ),
    "lspa_soja_202607": lambda: ibge.lspa("soja", ano=2026, mes=7, return_meta=True),
    "ppm_bovino_uf_2023": lambda: ibge.ppm("bovino", ano=2023, return_meta=True),
    "abate_bovino_2024T1": lambda: ibge.abate("bovino", trimestre="2024T1", return_meta=True),
    "silvicultura_2023": lambda: ibge.silvicultura("carvao", ano=2023, return_meta=True),
    "extracao_vegetal_2023": lambda: ibge.extracao_vegetal("acai", ano=2023, return_meta=True),
    "leite_2024T1": lambda: ibge.leite_trimestral(trimestre="2024T1", return_meta=True),
    "pib_agro_2024T1": lambda: ibge.pib_agro(trimestre="2024T1", return_meta=True),
    "censo_efetivo_2017_uf": lambda: ibge.censo_agro(
        "efetivo_rebanho", ano=2017, nivel="uf", return_meta=True
    ),
    "censo_historico_estab": lambda: ibge.censo_agro_historico(
        "estabelecimentos_area", ano=2006, return_meta=True
    ),
}


def _request(url: str, file: str, content_type: str, status: int = 200) -> dict[str, Any]:
    parts = urlsplit(unquote(url))
    return {
        "match": {
            "path": f"{parts.scheme}://{parts.netloc}{parts.path}",
            "params": dict(parse_qsl(parts.query, keep_blank_values=True)),
            "skip": 0,
        },
        "file": file,
        "content_type": content_type,
        "status": status,
    }


def _case(call: str) -> dict[str, Any]:
    receipts = {receipt["name"]: receipt for receipt in RECEIPTS}
    names = list(SUMMARY[call]["requests"])
    names += [
        receipt["name"]
        for receipt in RECEIPTS
        if receipt["requested_url"].endswith("/periodos") and receipt["name"] not in names
    ]
    requests = [
        _request(receipts[name]["requested_url"], f"agregados/{name}.json", "application/json")
        for name in names
    ]
    return {"id": call, "requests": requests}


@pytest.fixture(autouse=True)
def _reset_state(monkeypatch: pytest.MonkeyPatch):
    warn_once_reset("ibge_sidra_fallback")
    monkeypatch.setattr(agregados, "_periodos_cache", {})


def test_agregados_url_translates_sidra_selectors():
    url = agregados.agregados_url("5457", "3", "all", "8331,216,214,112", "2023", {"782": "40124"})
    assert url == (
        f"{agregados.BASE_URL}/5457/periodos/2023/variaveis/8331,216,214,112"
        "?localidades=N3[all]&classificacao=782[40124]"
    )
    assert agregados.periodos_segment(None) == "-1"
    assert agregados.periodos_segment("last") == "-1"
    assert agregados.periodos_segment("last 5") == "-5"
    assert agregados.periodos_segment(["2022", "2023"]) == "2022,2023"
    assert agregados.periodos_segment("all") == "all"
    assert agregados.variaveis_segment(None) == "all"
    assert agregados.variaveis_segment("allxp") == "all"
    assert agregados.localidades_param("6", "in N3 53") == "N6[N3[53]]"
    assert agregados.localidades_param("3", "51,52") == "N3[51,52]"
    assert agregados.classificacao_param({"12716": "115236", "18": ["992", "993"]}) == (
        "12716[115236]|18[992,993]"
    )
    assert agregados.classificacao_param(None) is None


async def test_cache_periodos_reutiliza_tabela_e_separa_identidades(monkeypatch):
    calls: list[str] = []
    payloads = {
        "5457": [{"id": "2023", "literals": ["2023"]}],
        "1092": [{"id": "202301", "literals": ["1º trimestre 2023"]}],
    }

    async def get_json(_http: httpx.AsyncClient, url: str) -> list[dict[str, Any]]:
        table = url.split("/")[-2]
        calls.append(table)
        return payloads[table]

    monkeypatch.setattr(agregados, "_get_json", get_json)
    async with httpx.AsyncClient() as http:
        for _ in range(2):
            assert await agregados.fetch_period_names(http, "5457") == {"2023": "2023"}
            assert await agregados.fetch_period_names(http, "1092") == {
                "202301": "1º trimestre 2023"
            }
    assert calls == ["5457", "1092"]


def test_to_sidra_frame_matches_sidra_publication_for_lspa_soy():
    payload = json.loads((GOLDEN / "agregados/agregados_004.json").read_text(encoding="utf-8"))
    names = {
        str(item["id"]): item["literals"][0]
        for item in json.loads(
            (GOLDEN / "agregados/agregados_005.json").read_text(encoding="utf-8")
        )
    }
    frame = agregados.to_sidra_frame(
        payload, variable=None, classifications={"48": "39443"}, period_names=names
    )
    assert list(frame.columns) == [
        "NC",
        "NN",
        "MC",
        "MN",
        "V",
        "D1C",
        "D1N",
        "D2C",
        "D2N",
        "D3C",
        "D3N",
        "D4C",
        "D4N",
    ]
    assert set(frame["D2N"]) == {"julho 2026"}
    assert set(frame["D3C"]) == {"39443"}
    sidra = {row["D4C"]: row for row in LSPA_SIDRA if row["D3C"] == "39443"}
    assert set(frame["D4C"]) == set(sidra)
    for _, row in frame.iterrows():
        published = sidra[row["D4C"]]
        assert row["V"] == published["V"], row["D4N"]
        assert row["MN"] == published["MN"]
        assert row["D4N"] == published["D4N"]
        assert row["D1N"] == published["D1N"]
        assert row["NN"] == published["NN"]


async def test_sidra_challenge_body_triggers_fallback(monkeypatch: pytest.MonkeyPatch):
    case = _case("lspa_soja_202607")
    sidra_url = client._sidra_url("6588", "1", "all", None, "202607", {"48": "39443"}, "n")
    case["requests"].append(
        _request(sidra_url, "sidra/sidra_403_cloudflare.html", "text/html", 403)
    )
    seen = install_replay_http(monkeypatch, case, GOLDEN)
    frame = await client.fetch_sidra("6588", "1", "all", None, "202607", {"48": "39443"})
    assert frame.attrs["canal"] == "servicodados"
    assert any("apisidra" in url for url in seen["served"])
    assert len(frame) == 4


async def test_both_channels_down_raise_combined_error(monkeypatch: pytest.MonkeyPatch):
    install_replay_http(monkeypatch, {"id": "vazio", "requests": []}, GOLDEN)

    async def _sidra_challenge(sidra_url: str, _table_code: str) -> pd.DataFrame:
        raise SourceUnavailableError(source="ibge", url=sidra_url, last_error="HTTP 403")

    monkeypatch.setattr(client, "_fetch_sidra_values", _sidra_challenge)
    with pytest.raises(SourceUnavailableError, match="SIDRA: HTTP 403; API de agregados"):
        await client.fetch_sidra("5457", "1", "all", "214", "2023", {"782": "40124"})


def _fallback_frame(call: str, monkeypatch: pytest.MonkeyPatch):
    case = _case(call)
    seen = install_replay_http(monkeypatch, case, GOLDEN)

    async def _sidra_challenge(sidra_url: str, _table_code: str) -> pd.DataFrame:
        raise SourceUnavailableError(source="ibge", url=sidra_url, last_error="HTTP 403")

    monkeypatch.setattr(client, "_fetch_sidra_values", _sidra_challenge)
    return seen


@pytest.mark.parametrize("call", sorted(ORACLE_CASES))
async def test_public_values_match_raw_aggregates_oracle(
    call: str, monkeypatch: pytest.MonkeyPatch
):
    seen = _fallback_frame(call, monkeypatch)
    frame, _ = await CALLS[call]()
    assert_replay_served(seen)
    case = ORACLE_CASES[call]
    assert len(frame) == case["rows"]
    if call.startswith("pam_"):
        assert set(frame.columns) == {*case["columns"], "valor_producao"}
        assert frame["valor_producao"].isna().all()
    else:
        assert list(frame.columns) == case["columns"]
    assert_replay_samples(frame, case)


@pytest.mark.parametrize(
    "call,fetch",
    [
        (
            "abate_bovino_2024T1",
            lambda: datasets.abate_trimestral("bovino", trimestre="2024T1"),
        ),
        ("ppm_bovino_uf_2023", lambda: datasets.pecuaria_municipal("bovino", ano=2023)),
        (
            "censo_efetivo_2017_uf",
            lambda: datasets.censo_agropecuario("efetivo_rebanho", ano=2017, nivel="uf"),
        ),
        (
            "censo_historico_estab",
            lambda: datasets.censo_agropecuario_historico("estabelecimentos_area", ano=2006),
        ),
    ],
    ids=lambda value: value if isinstance(value, str) else "",
)
async def test_datasets_sem_meta_conferem_oraculo_de_agregados(
    call: str, fetch, monkeypatch: pytest.MonkeyPatch
):
    seen = _fallback_frame(call, monkeypatch)
    frame = await fetch()
    assert_replay_served(seen)
    assert len(frame) == ORACLE_CASES[call]["rows"]
    assert_replay_samples(frame, ORACLE_CASES[call])


async def test_ppm_entrega_valor_float64_como_o_contrato(monkeypatch: pytest.MonkeyPatch):
    seen = _fallback_frame("ppm_bovino_uf_2023", monkeypatch)
    frame = await datasets.pecuaria_municipal("bovino", ano=2023)
    assert_replay_served(seen)
    flutuantes = [
        coluna.name
        for coluna in contracts.get_contract("pecuaria_municipal").columns
        if coluna.type is contracts.ColumnType.FLOAT
    ]
    assert flutuantes == ["valor"]
    assert str(frame["valor"].dtype) == "float64"
    assert_replay_samples(frame, ORACLE_CASES["ppm_bovino_uf_2023"])


@pytest.mark.parametrize("canal", ["sidra", "servicodados"])
async def test_pesquisas_sem_dado_devolvem_colunas_da_resposta_real_e_avisam(
    canal: str, monkeypatch: pytest.MonkeyPatch
):
    async def send(
        _client: httpx.AsyncClient, request: httpx.Request, **_kwargs: Any
    ) -> httpx.Response:
        recusa = canal == "servicodados" and request.url.host == "apisidra.ibge.gov.br"
        return httpx.Response(403 if recusa else 200, json=[], request=request)

    monkeypatch.setattr(httpx.AsyncClient, "send", send)
    chamadas = [
        "abate_bovino_2024T1",
        "leite_2024T1",
        "silvicultura_2023",
        "extracao_vegetal_2023",
        "pib_agro_2024T1",
        "ppm_bovino_uf_2023",
    ]
    with collect_failures() as check:
        for call in chamadas:
            with check(call), warnings.catch_warnings(record=True) as avisos:
                warnings.simplefilter("always")
                frame, _ = await CALLS[call]()
                assert list(frame.columns) == SUMMARY[call]["columns"]
                assert frame.empty
                assert any("sem dado" in str(aviso.message) for aviso in avisos)


@pytest.mark.parametrize("call", sorted(CALLS))
async def test_ibge_sem_cache_nao_promete_vencimento(call: str, monkeypatch: pytest.MonkeyPatch):
    _fallback_frame(call, monkeypatch)
    with sem_excecao(), warnings.catch_warnings():
        warnings.simplefilter("ignore")
        _, meta = await CALLS[call]()
    assert (meta.from_cache, meta.cache_expires_at) == (False, None)
