from __future__ import annotations

import hashlib
from datetime import datetime

import pytest

from agrobr import contracts, mapbiomas
from agrobr.exceptions import InvalidParameterError


@pytest.mark.parametrize(
    "arguments",
    [
        {"geocodigo": "2703007"},
        {"estado": "AL"},
        {"municipio": 270300},
        {"municipio": "２７０３００７"},
        {"municipio": True},
        {"municipio": 2703007.0},
        {"municipio": " "},
        {"municipio": "Sorris"},
        {"municipio": "Redenção"},
        {"municipio": "Sorriso", "uf": "PA"},
        {"uf": True},
        {"bioma": 1},
        {"classe_id": True},
        {"classe_id": 3.1},
        {"classe_id": "3"},
        {"ano": True},
        {"nivel": []},
        {"as_polars": 1},
        {"return_meta": "false"},
        {"municipioo": "Sorriso"},
        {"periodo": "2020-2021"},
        {"classe_de_id": 3},
    ],
)
async def test_source_invalid_municipal_arguments_fail_before_http(arguments, replay_mapbiomas):
    requests = replay_mapbiomas()
    arguments = {"nivel": "municipio", **arguments}
    with pytest.raises(InvalidParameterError):
        await mapbiomas.cobertura(**arguments)
    assert not requests


@pytest.mark.parametrize("return_meta", [False, True])
async def test_source_polars_typed_empty_keeps_municipal_contract(return_meta, replay_mapbiomas):
    pl = pytest.importorskip("polars")
    replay_mapbiomas()
    result = await mapbiomas.cobertura(
        nivel="municipio", uf="MT", municipio="2703007", as_polars=True, return_meta=return_meta
    )
    frame = result[0] if return_meta else result
    assert frame.shape == (0, 11) and frame["geocodigo"].dtype == pl.Utf8
    assert frame["cod_municipio"].dtype == pl.Int64
    assert frame["ano"].dtype == frame["classe_id"].dtype == pl.Int64
    assert frame["area_ha"].dtype == pl.Float64
    if return_meta:
        assert result[1].records_count == 0 and result[1].columns == frame.columns


@pytest.mark.parametrize("ano", [None, 1985, 2025])
async def test_official_municipal_replay_preserves_every_selected_area_and_identity(
    ano, municipal_capture, replay_mapbiomas
):
    requests = replay_mapbiomas()
    frame, meta = await mapbiomas.cobertura(nivel="municipio", ano=ano, return_meta=True)
    years = [ano] if ano is not None else list(range(1985, 2026))
    states = {
        "Pará": "PA",
        "Mato Grosso": "MT",
        "Distrito Federal": "DF",
        "Alagoas": "AL",
        "Pernambuco": "PE",
    }
    expected = {}
    for row in municipal_capture["oracle"]["rows"]:
        cells = row["cells"]
        for letter, header in municipal_capture["oracle"]["header"].items():
            if header.startswith("y") and int(header[1:]) in years:
                key = (cells["C"], states[cells["E"]], cells["F"], int(cells["I"]), int(header[1:]))
                expected[key] = (cells["G"], cells["J"], float(cells[letter]), int(cells["A"]))
    assert len(frame) == len(expected) == 142 * len(years)
    for row in frame.itertuples(index=False):
        key = (row.bioma, row.uf, row.geocodigo, row.classe_id, row.ano)
        municipality, level, area, identifier = expected[key]
        assert row.municipio == municipality and row.nivel_0 == level
        assert row.id_registro == identifier
        assert row.area_ha == pytest.approx(area, rel=1e-14, abs=0)
        assert isinstance(row.classe, str) and row.classe
    assert contracts.get_contract("mapbiomas_cobertura_municipal").validate(frame) == (True, [])
    zero_class = frame[frame["classe_id"] == 0]
    expected_zero_rows = sum(
        int(row["cells"]["I"]) == 0 for row in municipal_capture["oracle"]["rows"]
    )
    assert len(zero_class) == expected_zero_rows * len(years)
    assert set(zero_class["classe"]) == {"Não observado"}
    assert meta.parser_version == 2 and meta.schema_version == meta.contract_version == "1.1"
    assert meta.raw_content_hash == hashlib.sha256(municipal_capture["zip"]).hexdigest()
    assert meta.raw_content_size == len(municipal_capture["zip"])
    acquisition = meta.source_details["acquisition"]
    assert acquisition["resource"]["sha256"] == meta.raw_content_hash
    assert acquisition["member"]["sha256"] == hashlib.sha256(municipal_capture["xlsx"]).hexdigest()
    assert acquisition["member"]["size_bytes"] == len(municipal_capture["xlsx"])
    assert acquisition["member"]["sha256"] != meta.raw_content_hash
    assert "content" not in acquisition
    assert (
        acquisition["confirmation"]["sha256"]
        == hashlib.sha256(municipal_capture["confirmation"]).hexdigest()
    )
    assert datetime.fromisoformat(acquisition["resource"]["fetched_at"]) == meta.fetched_at
    assert meta.selected_source == "mapbiomas_oficial" and meta.data_sources == [
        "mapbiomas_colecao_11"
    ]
    coverage = meta.source_details["coverage"]
    assert coverage["validated_rows"] == 142 and coverage["annual_cells"] == 5822
    assert coverage["output_rows"] == len(frame) and coverage["eof_reached"]
    assert coverage["scope"] == "published_workbook_population"
    assert meta.source_details["geocodes_with_multiple_states"] == [
        {"geocodigo": "2703007", "estados": ["AL", "PE"]}
    ]
    assert len(requests) == 2


@pytest.mark.parametrize(
    "municipio", ["Sorriso", " SORRISO ", "sorriso", 5107925, "5107925"], ids=repr
)
async def test_municipio_por_nome_inteiro_ou_codigo_seleciona_o_mesmo_recorte(
    municipio, replay_mapbiomas
):
    replay_mapbiomas()
    frame = await mapbiomas.cobertura(nivel="municipio", municipio=municipio, ano=2025)
    assert len(frame) == 32
    assert set(frame["geocodigo"]) == {"5107925"} and set(frame["municipio"]) == {"Sorriso"}


async def test_homonimo_resolve_pela_uf(replay_mapbiomas):
    replay_mapbiomas()
    frame = await mapbiomas.cobertura(nivel="municipio", municipio="Redenção", uf="PA", ano=2025)
    assert frame["geocodigo"].tolist() == ["1506138"] and frame["uf"].tolist() == ["PA"]


async def test_geocodigo_fora_do_recurso_levanta_depois_do_download(replay_mapbiomas):
    requests = replay_mapbiomas()
    with pytest.raises(InvalidParameterError, match="geocódigo ausente do recurso municipal"):
        await mapbiomas.cobertura(nivel="municipio", municipio="4300001")
    assert len(requests) == 2
