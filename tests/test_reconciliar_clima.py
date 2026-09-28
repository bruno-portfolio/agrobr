from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts import reconciliar_clima as reconciliation

GOLDEN = Path(__file__).parent / "golden_data"
CATALOG_A001 = [
    {
        "CD_ESTACAO": "A001",
        "SG_ESTADO": "DF",
        "VL_LATITUDE": "-15.78944444",
        "VL_LONGITUDE": "-47.92583332",
    }
]


def _a001_metadata() -> dict[str, str]:
    manifest = json.loads(
        (GOLDEN / "inmet/selecao_20260906/manifest.json").read_text(encoding="utf-8")
    )
    recorded = next(r for r in manifest["requests"] if r["url"].endswith("/2001.zip"))
    zip_bytes = (GOLDEN / "inmet/selecao_20260906" / recorded["body_file"]).read_bytes()
    member = next(m["member"] for m in recorded["members"] if m["estacao"] == "A001")
    return reconciliation.inmet_member_metadata(zip_bytes, member)


def test_compare_inmet_zip_identity_detects_republished_archive():
    recorded = {"headers": {"last-modified": "Wed, 02 Sep 2020 12:14:18 GMT", "etag": '"abc"'}}
    same = {"last-modified": "Wed, 02 Sep 2020 12:14:18 GMT", "etag": '"abc"'}
    result = reconciliation.compare_inmet_zip_identity(same, recorded)
    assert result["status"] == "ok"
    assert "identidade" in result["nota"]
    changed = {**same, "etag": '"def"'}
    assert reconciliation.compare_inmet_zip_identity(changed, recorded)["status"] == "mismatch"
    assert reconciliation.compare_inmet_zip_identity({}, recorded)["status"] == "mismatch"


def test_compare_inmet_station_against_csv_metadata():
    metadata = _a001_metadata()
    assert metadata == {
        "codigo": "A001",
        "uf": "DF",
        "latitude": "-15,78944444",
        "longitude": "-47,92583332",
    }
    assert reconciliation.compare_inmet_station(CATALOG_A001, metadata)["status"] == "ok"


@pytest.mark.parametrize(
    "mutation",
    ["latitude_deslocada", "sem_latitude", "sem_longitude", "latitude_nan", "uf", "ausente"],
)
def test_compare_inmet_station_rejects_structural_mutations(mutation: str):
    metadata = _a001_metadata()
    entry = dict(CATALOG_A001[0])
    if mutation == "latitude_deslocada":
        entry["VL_LATITUDE"] = "-15.79"
    elif mutation == "sem_latitude":
        del entry["VL_LATITUDE"]
    elif mutation == "sem_longitude":
        entry["VL_LONGITUDE"] = None
    elif mutation == "latitude_nan":
        entry["VL_LATITUDE"] = "nan"
    elif mutation == "uf":
        entry["SG_ESTADO"] = "GO"
    catalog = [] if mutation == "ausente" else [entry]
    result = reconciliation.compare_inmet_station(catalog, metadata)
    assert result["status"] == "mismatch", result


def test_compare_nasa_publication_against_golden():
    golden = json.loads(reconciliation.NASA_GOLDEN.read_text(encoding="utf-8"))
    assert reconciliation.compare_nasa_publication(golden, golden) == {
        "status": "ok",
        "problems": [],
        "dias": 365,
    }


@pytest.mark.parametrize(
    "mutation",
    [
        "valor",
        "unidade",
        "dia_removido",
        "longitude_deslocada",
        "latitude_deslocada",
        "time_standard",
        "parametro_novo",
        "definicao_sem_serie",
        "serie_sem_definicao",
        "chave_topo_nova",
        "sem_geometria",
    ],
)
def test_compare_nasa_publication_rejects_mutations(mutation: str):
    golden = json.loads(reconciliation.NASA_GOLDEN.read_text(encoding="utf-8"))
    live = json.loads(json.dumps(golden))
    if mutation == "valor":
        live["properties"]["parameter"]["T2M"]["20250101"] += 0.01
    elif mutation == "unidade":
        live["parameters"]["T2M"]["units"] = "K"
    elif mutation == "dia_removido":
        del live["properties"]["parameter"]["T2M"]["20251231"]
    elif mutation == "longitude_deslocada":
        live["geometry"]["coordinates"][0] += 1.0
    elif mutation == "latitude_deslocada":
        live["geometry"]["coordinates"][1] -= 1.0
    elif mutation == "time_standard":
        live["header"]["time_standard"] = "UTC"
    elif mutation == "parametro_novo":
        live["properties"]["parameter"]["PS"] = {"20250101": 95.0}
        live["parameters"]["PS"] = {"units": "kPa", "longname": "Surface Pressure"}
    elif mutation == "definicao_sem_serie":
        live["parameters"]["NEW_PARAMETER"] = {"units": "C", "longname": "Unclassified"}
    elif mutation == "serie_sem_definicao":
        del live["parameters"]["WS2M"]
    elif mutation == "chave_topo_nova":
        live["extra"] = {}
    else:
        live["geometry"]["coordinates"] = []
    result = reconciliation.compare_nasa_publication(live, golden)
    assert result["status"] == "mismatch", mutation
