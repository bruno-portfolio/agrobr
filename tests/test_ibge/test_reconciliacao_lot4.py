from __future__ import annotations

import json
import re
import warnings
from functools import partial
from pathlib import Path

import pandas as pd
import pytest

from agrobr import datasets, ibge
from agrobr.exceptions import AgrobrError, ParseError, SourceUnavailableError
from agrobr.ibge import legacy_api
from tests import helpers

GOLDEN = Path(__file__).resolve().parents[1] / "golden_data"
MANIFEST = json.loads(
    (GOLDEN / "reconciliacao_censos_producao_ibge_conab_20260918/lot4_manifest.json").read_text(
        encoding="utf-8"
    )
)
STATE_FILE = (
    GOLDEN / "reconciliacao_censos_producao_ibge_conab_20260918" / MANIFEST["state_oracle"]["file"]
)
STATES = json.loads(STATE_FILE.read_text(encoding="utf-8"))
FILES = {(item.get("uf"), item.get("theme")): item for item in MANIFEST["files"] if item.get("uf")}
NATIONAL = MANIFEST["national_oracles"]
BODIES = [
    [item for item in STATES if FILES[item["uf"], item["theme"]]["sha256"] == digest]
    for digest in dict.fromkeys(FILES[item["uf"], item["theme"]]["sha256"] for item in STATES)
]
PA_TEXTO = (
    "o IBGE publicou a Tabela 6 (pessoal ocupado) no lugar da Tabela 7 em Para/Tab_7Mn.zip, "
    "e a tabela municipal de maquinaria do Pará não está no FTP."
)
PA_LACUNA = re.escape(PA_TEXTO)


@pytest.mark.parametrize(
    "body",
    BODIES,
    ids=lambda group: group[0]["uf"] + "-" + "+".join(item["theme"] for item in group),
)
async def test_legado_corpos_celulas_e_saidas_publicas(body):
    with helpers.collect_failures() as check:
        for oracle in body:
            case = (oracle["uf"], oracle["theme"])
            with check(case), helpers.isolated_dataset_case(case) as monkeypatch:
                entry = FILES[case]
                calls = helpers.install_reconciliacao_r5_legacy_http(monkeypatch, [entry])
                if oracle["expected_outcome"] != "data":
                    assert entry["sha256"] == FILES["PA", "pessoal_ocupado"]["sha256"]
                    assert entry["url"].endswith("/Para/Tab_7Mn.zip")
                    assert FILES["PA", "pessoal_ocupado"]["url"].endswith("/Para/Tab_6Mn.zip")
                    with pytest.raises(AgrobrError) as error:
                        await ibge.censo_agro_legado(oracle["theme"], uf=oracle["uf"])
                    assert type(error.value) is SourceUnavailableError
                    assert re.search(PA_LACUNA, str(error.value))
                    assert isinstance(error.value.__cause__, ParseError)
                    assert "Cabeçalho não corresponde ao tema maquinas: Tabela 6. Pessoal" in (
                        error.value.__cause__.reason
                    )
                    with pytest.raises(SourceUnavailableError, match=PA_LACUNA):
                        await datasets.censo_agropecuario_legado(oracle["theme"], uf=oracle["uf"])
                    assert calls == [entry["url"], entry["url"]]
                    continue
                frames, urls = await legacy_api._fetch_tables(oracle["theme"], oracle["uf"])
                with check((case, "corpo")):
                    helpers.assert_censo_legacy_matrix(pd.concat(frames, ignore_index=True), oracle)

                monkeypatch.setattr(
                    legacy_api,
                    "_fetch_tables",
                    partial(
                        helpers.replay_censo_legacy_tables, case=case, frames=frames, urls=urls
                    ),
                )
                for level in ("uf", "municipio"):
                    with check((case, level)), helpers.isolated_dataset_case((case, level)):
                        frame, meta = await datasets.censo_agropecuario_legado(
                            oracle["theme"], uf=oracle["uf"], nivel=level, return_meta=True
                        )
                        helpers.assert_reconciliacao_r5_legacy_state(frame, oracle, level)
                        assert meta.selected_source == "ibge_censo_agro_legado"
                        assert meta.attempted_sources == ["ibge_censo_agro_legado"]
                        assert meta.parser_version == 2
                        assert meta.schema_version == meta.contract_version == "2.1"
                        assert meta.records_count == len(frame)
                        assert "cod_municipio" in frame
                        municipal = frame["localidade_cod"].ge(1_000_000).fillna(False).astype(bool)
                        codigos = frame.loc[municipal, "cod_municipio"]
                        assert codigos.astype(object).where(codigos.notna(), None).tolist() == (
                            frame.loc[municipal, "localidade_cod"].tolist()
                        )
                        assert frame.loc[~municipal, "cod_municipio"].isna().all()
                assert calls == [entry["url"]]


@pytest.mark.parametrize("theme", sorted({item["theme"] for item in NATIONAL}))
async def test_legado_nacional_replay_todas_medidas_e_duas_tabelas_financeiras(monkeypatch, theme):
    oracles = [item for item in NATIONAL if item["theme"] == theme]
    entries = [
        entry
        for entry in MANIFEST["files"]
        if entry["fixture"] in {item["fixture"] for item in oracles}
    ]
    calls = helpers.install_reconciliacao_r5_legacy_http(monkeypatch, entries)
    frame, meta = await datasets.censo_agropecuario_legado(theme, nivel="brasil", return_meta=True)
    assert set(calls) == {item["url"] for item in entries}
    assert len(calls) == len(entries)
    for oracle in oracles:
        helpers.assert_reconciliacao_r5_legacy_national(frame, oracle)
    assert len(frame) == sum(len(item["categories"]) * len(item["metrics"]) for item in oracles)
    assert not frame.duplicated(["ano", "localidade", "tema", "categoria", "variavel"]).any()
    assert meta.records_count == len(frame)
    assert meta.selected_source == "ibge_censo_agro_legado"


async def test_legado_dataset_sem_meta_confere_oraculo(monkeypatch):
    oracle = next(item for item in STATES if item["uf"] == "GO" and item["theme"] == "financeiro")
    helpers.install_reconciliacao_r5_legacy_http(monkeypatch, [FILES["GO", "financeiro"]])
    frame = await datasets.censo_agropecuario_legado("financeiro", uf="GO")
    helpers.assert_reconciliacao_r5_legacy_state(frame, oracle, "uf")


async def test_legado_sem_uf_deixa_o_para_fora_de_maquinas_com_aviso(monkeypatch):
    oracles = [item for item in STATES if item["theme"] == "maquinas"]
    entries = [FILES[item["uf"], "maquinas"] for item in oracles]
    calls = helpers.install_reconciliacao_r5_legacy_http(monkeypatch, entries)
    aviso = f"censo_agro_legado: PA fica fora do tema maquinas; {PA_TEXTO}"
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        frame, meta = await datasets.censo_agropecuario_legado("maquinas", return_meta=True)
    assert [str(item.message) for item in caught if item.category is UserWarning] == [aviso]
    assert sorted(calls) == sorted(entry["url"] for entry in entries)
    assert meta.validation_warnings == [aviso]
    assert sorted(frame["uf"].unique()) == sorted(
        item["uf"] for item in oracles if item["uf"] != "PA"
    )
    for oracle in oracles:
        if oracle["uf"] != "PA":
            state = frame.loc[frame["uf"].eq(oracle["uf"])].reset_index(drop=True)
            helpers.assert_reconciliacao_r5_legacy_state(state, oracle, "uf")


async def test_legado_sem_uf_levanta_com_titulo_errado_fora_da_lacuna(monkeypatch):
    goias = {**FILES["GO", "pessoal_ocupado"], "url": FILES["GO", "maquinas"]["url"]}
    entries = [
        goias if item["uf"] == "GO" else FILES[item["uf"], "maquinas"]
        for item in STATES
        if item["theme"] == "maquinas"
    ]
    helpers.install_reconciliacao_r5_legacy_http(monkeypatch, entries)
    with pytest.raises(ParseError, match="Cabeçalho não corresponde ao tema maquinas: Tabela 6"):
        await ibge.censo_agro_legado("maquinas")
