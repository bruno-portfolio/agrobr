from __future__ import annotations

import asyncio
import hashlib
import json
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

import pandas as pd
import pytest

from agrobr import contracts, defensivos
from agrobr.defensivos import api, cache, snapshot
from agrobr.exceptions import ContractViolationError, InvalidParameterError, ParseError

GOLDEN_DIR = Path(__file__).parent.parent / "golden_data" / "defensivos"


@pytest.fixture()
def _patch_cache(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(cache, "_cache_dir", lambda: tmp_path)


@pytest.fixture
def replay_captures(_patch_cache, monkeypatch):
    root = GOLDEN_DIR / "selecao_20260906"
    downloads = {}
    for kind in ("formulados", "tecnicos"):
        download = AsyncMock(return_value=(root / f"{kind}_curated_rows.csv").read_bytes())
        monkeypatch.setattr(api.client, f"download_{kind}", download)
        downloads[kind] = download
    return downloads


async def test_replay_preserves_published_authorizations_without_component_cross_product(
    replay_captures,
):
    products, product_meta = await defensivos.formulados(nr_registro="35523", return_meta=True)
    authorizations, auth_meta = await defensivos.autorizacoes(nr_registro="35523", return_meta=True)
    components = await defensivos.composicao(nr_registro="35523")
    oracles = json.loads(
        (GOLDEN_DIR / "selecao_20260906/formulados_curated_oracles.json").read_text(
            encoding="utf-8"
        )
    )
    rows = [item for item in oracles if item["values"]["NR_REGISTRO"] == "35523"]
    assert len(products) == 1
    assert len(authorizations) == len(rows) == 2
    assert authorizations["praga"].tolist() == [
        item["values"]["PRAGA_NOME_CIENTIFICO"].strip() for item in rows
    ]
    assert authorizations["praga"].nunique() == 2
    assert len(components) == rows[0]["components_in_original_order"]
    assert products["situacao"].tolist() == ["TRUE"]
    assert authorizations["situacao"].tolist() == ["TRUE", "TRUE"]
    assert product_meta.raw_content_hash == auth_meta.raw_content_hash
    replay_captures["formulados"].assert_awaited_once()
    replay_captures["tecnicos"].assert_not_awaited()


@pytest.mark.parametrize("name", ["formulados", "autorizacoes"])
@pytest.mark.usefixtures("replay_captures")
async def test_replay_situation_is_exact_case_insensitive_text(name):
    query = getattr(defensivos, name)
    matching = await query(situacao=" true ")
    assert not matching.empty
    assert matching["situacao"].eq("TRUE").all()
    assert matching["situacao"].map(lambda value: isinstance(value, str)).all()
    partial = await query(situacao="TRU")
    assert partial.empty
    assert partial.columns.tolist() == matching.columns.tolist()
    assert partial.dtypes.astype(str).tolist() == matching.dtypes.astype(str).tolist()


@pytest.mark.usefixtures("replay_captures")
async def test_replay_text_filter_does_not_match_null_cells():
    everything = await defensivos.autorizacoes()
    assert everything.loc[everything["nr_registro"].eq("5100"), "cultura"].isna().all()
    frame = await defensivos.autorizacoes(cultura="a")
    assert not frame.empty
    assert frame["cultura"].notna().all()
    assert "5100" not in frame["nr_registro"].tolist()


@pytest.mark.parametrize("value,expected", [("2,4-D (sal", ["001"]), ("2.4-D", ["003"])])
async def test_filter_ingrediente_ativo_literal(value, expected, _patch_cache):
    raw = (
        b"NR_REGISTRO;MARCA_COMERCIAL;INGREDIENTE_ATIVO;CULTURA\n"
        b"001;Produto A;2,4-D (sal dimetilamina;Soja\n"
        b"002;Produto B;2,4-D sal dimetilamina;Soja\n"
        b"003;Produto C;2.4-D;Soja\n"
    )
    with patch.object(api.client, "download_formulados", AsyncMock(return_value=raw)):
        df = await api.formulados(ingrediente_ativo=value, use_cache=False)
    assert df["nr_registro"].tolist() == expected


@pytest.mark.usefixtures("replay_captures")
async def test_replay_composition_preserves_order_and_literal_unusual_unit():
    components = await defensivos.composicao(tipo="tecnicos", nr_registro="00301")
    assert components["ordem_componente"].tolist() == [1, 2]
    assert components["concentracao_valor"].tolist() == [7.0, 6.8]
    assert components["concentracao_unidade"].tolist() == ["g/L", "g/L"]
    selected = await defensivos.composicao(
        tipo="tecnicos", nr_registro="00301", ingrediente_ativo="GIBERÉLICO"
    )
    assert selected["ordem_componente"].tolist() == [2]
    unusual = await defensivos.composicao(tipo="tecnicos", nr_registro="TC02523")
    assert unusual["concentracao_texto"].tolist() == ["950 Kg"]
    assert unusual["concentracao_valor"].tolist() == [950.0]
    assert unusual["concentracao_unidade"].tolist() == ["Kg"]


@pytest.mark.usefixtures("replay_captures")
async def test_replay_repeated_component_names_and_zero_are_not_deduplicated():
    repeated = await defensivos.composicao(nr_registro="08124")
    assert repeated["ordem_componente"].tolist() == [1, 2]
    assert repeated["ingrediente_ativo"].nunique() == 1
    assert repeated["concentracao_valor"].tolist() == [0.02, 0.02]
    zero = await defensivos.composicao(nr_registro="07201")
    assert zero["concentracao_valor"].tolist() == [0.99, 0.0]
    assert zero["concentracao_unidade"].tolist() == ["mg/mg", "mg/mg"]


async def test_replay_product_composition_text_survives_cache(replay_captures):
    frame, fresh = await defensivos.tecnicos(nr_registro="00301", return_meta=True)
    cached, meta = await defensivos.tecnicos(nr_registro="00301", return_meta=True)
    pd.testing.assert_frame_equal(frame.isna(), cached.isna())
    pd.testing.assert_frame_equal(
        frame.mask(frame.isna(), pd.NA), cached.mask(cached.isna(), pd.NA)
    )
    raw = replay_captures["tecnicos"].return_value
    assert "(7 g/L) + " in frame.iloc[0]["composicao_texto"]
    assert "(6.8 g/L)" in frame.iloc[0]["composicao_texto"]
    assert fresh.from_cache is False
    assert meta.from_cache is True
    assert meta.fetched_at == fresh.fetched_at
    assert meta.fetch_timestamp == fresh.fetch_timestamp == fresh.fetched_at
    assert meta.raw_content_hash == fresh.raw_content_hash == hashlib.sha256(raw).hexdigest()
    assert meta.source_details["resource"] == fresh.source_details["resource"]
    assert fresh.source_details["resource"]["fetched_at"] == fresh.fetched_at.isoformat()
    assert meta.schema_version == meta.contract_version == "1.1"
    assert meta.dataset == ""
    assert meta.columns == cached.columns.tolist()
    assert meta.records_count == 1
    meta.source_details["resource"]["sha256"] = "changed"
    _, untouched = await defensivos.tecnicos(return_meta=True)
    assert untouched.raw_content_hash == untouched.source_details["resource"]["sha256"]
    replay_captures["tecnicos"].assert_awaited_once()


@pytest.mark.parametrize(
    "name,arguments",
    [
        ("formulados", {"nr_registro": 35523}),
        ("autorizacoes", {"nr_registro": True}),
        ("tecnicos", {"nr_registro": " "}),
        ("formulados", {"situacao": True}),
        ("autorizacoes", {"situacao": ""}),
        ("tecnicos", {"situacao": "TRUE"}),
        ("composicao", {"tipo": "unknown"}),
        ("composicao", {"tipo": None}),
        ("composicao", {"ingrediente_ativo": ["amitraz"]}),
        ("composicao", {"situacao": "TRUE"}),
        ("formulados", {"cultura": "Soja"}),
        ("formulados", {"use_cache": 1}),
        ("tecnicos", {"marca": ""}),
        ("autorizacoes", {"cutura": "Soja"}),
    ],
)
async def test_invalid_queries_fail_before_cache_or_download(
    name, arguments, replay_captures, monkeypatch
):
    read = Mock(return_value=None)
    monkeypatch.setattr(snapshot, "read_snapshot", read)
    with pytest.raises((InvalidParameterError, ValueError, TypeError, AttributeError)) as caught:
        await getattr(defensivos, name)(**arguments)
    assert caught.type is InvalidParameterError
    read.assert_not_called()
    for download in replay_captures.values():
        download.assert_not_awaited()


async def test_concurrent_queries_share_acquisition_and_keep_filters_isolated(replay_captures):
    started = asyncio.Event()
    release = asyncio.Event()
    raw = replay_captures["tecnicos"].return_value

    async def paused_download():
        started.set()
        await release.wait()
        return raw

    replay_captures["tecnicos"].side_effect = paused_download
    first = asyncio.create_task(defensivos.tecnicos(nr_registro="0314"))
    await asyncio.wait_for(started.wait(), timeout=5)
    second = asyncio.create_task(defensivos.composicao(tipo="tecnicos", nr_registro="00301"))
    release.set()
    product, components = await asyncio.wait_for(asyncio.gather(first, second), timeout=10)
    assert product["nr_registro"].tolist() == ["0314"]
    assert components["nr_registro"].tolist() == ["00301", "00301"]
    replay_captures["tecnicos"].assert_awaited_once()


@pytest.mark.parametrize(
    "name,arguments,expected_rows,version",
    [
        ("formulados", {"nr_registro": "35523"}, 1, "1.1"),
        ("autorizacoes", {"nr_registro": "35523"}, 2, "1.1"),
        ("tecnicos", {"nr_registro": "0314"}, 1, "1.1"),
        ("composicao", {"tipo": "tecnicos", "nr_registro": "00301"}, 2, "1.0"),
    ],
)
@pytest.mark.parametrize("return_meta", [False, True])
@pytest.mark.usefixtures("replay_captures")
async def test_replay_polars_matches_pandas_and_metadata(
    name, arguments, expected_rows, version, return_meta
):
    pl = pytest.importorskip("polars")
    query = getattr(defensivos, name)
    pandas_frame = await query(**arguments)
    result = await query(**arguments, as_polars=True, return_meta=return_meta)
    frame = result[0] if return_meta else result
    assert isinstance(frame, pl.DataFrame)
    assert frame.height == expected_rows
    assert frame.to_dicts() == pandas_frame.astype(object).where(
        pandas_frame.notna(), None
    ).to_dict("records")
    if return_meta:
        meta = result[1]
        assert meta.columns == frame.columns
        assert meta.records_count == expected_rows
        assert meta.contract_version == meta.schema_version == version
        assert meta.from_cache is True
        assert meta.dataset == ""
    contracts.validate_dataset(pandas_frame, f"agrofit_{name}")


async def test_semantically_invalid_cached_product_triggers_refetch_without_destroying_bundle_on_failure(
    replay_captures, tmp_path
):
    await defensivos.tecnicos()
    acquired = snapshot.read_snapshot("tecnicos")
    assert acquired is not None
    acquired.tables["tecnicos"].loc[0, "nr_registro"] = " "
    acquired.tables["composicao"].loc[0, "nr_registro"] = " "
    snapshot.write_snapshot("tecnicos", acquired.tables, acquired.meta)
    path = tmp_path / "tecnicos.v3.zip"
    before = path.read_bytes()
    replay_captures["tecnicos"].return_value = b"wrong;schema\n1;2\n"
    with pytest.raises((ParseError, ContractViolationError)) as caught:
        await defensivos.tecnicos()
    assert caught.type is ParseError
    assert replay_captures["tecnicos"].await_count == 2
    assert path.read_bytes() == before


def test_replay_captures_match_manifest_hashes_and_declared_subsets():
    root = GOLDEN_DIR / "selecao_20260906"
    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    assert "NOT all authorizations" in manifest["sampling"]
    for item in manifest["families"].values():
        raw = (root / item["sample_file"]).read_bytes()
        assert hashlib.sha256(raw).hexdigest() == item["sample_sha256"]
        assert len(raw) == item["sample_bytes"]
        assert (
            hashlib.sha256((root / item["oracles_file"]).read_bytes()).hexdigest()
            == item["oracles_sha256"]
        )
        assert item["url"].startswith("https://dados.agricultura.gov.br/")
        assert len(item["full_file_sha256"]) == 64
    for item in manifest["additional_samples"]:
        assert (
            hashlib.sha256((root / item["sample_file"]).read_bytes()).hexdigest()
            == item["sample_sha256"]
        )
        assert (
            hashlib.sha256((root / item["oracles_file"]).read_bytes()).hexdigest()
            == item["oracles_sha256"]
        )
