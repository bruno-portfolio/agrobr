from __future__ import annotations

import csv
import hashlib
import io
import json
from collections import Counter
from decimal import Decimal
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from agrobr import datasets
from agrobr.alt.mapa_psr import api, models
from agrobr.exceptions import ParseError
from scripts import reconciliar_psr as reconciliation
from tests import helpers

ROOT = Path(__file__).resolve().parents[2]
ORIGINAL = ROOT / "tests/golden_data/mapa_psr/apolices_sample/response.csv"
GOLDEN = ROOT / "tests/golden_data/reconciliacao_registros_precos_zoneamento_seguro_20260918/psr"
MANIFEST = json.loads((GOLDEN / "manifest.json").read_text(encoding="utf-8"))
MOTIVOS = {
    "extra_field": r"Registro 795, linha física 796: largura 39, esperada 38",
    "missing_field": r"Registro 795, linha física 796: largura 37, esperada 38",
    "invalid_year": r"Registro 795, linha física 796: ANO_APOLICE inválido",
    "duplicate_header": "Cabeçalho CSV duplicado",
}


def install(monkeypatch: pytest.MonkeyPatch, path: Path) -> dict[str, list[str]]:
    request = {
        "match": {"path": models.get_csv_url("2006-2015"), "params": {}, "skip": 0},
        "file": str(path),
        "content_type": "text/csv",
    }
    return helpers.install_replay_http(monkeypatch, {"requests": [request]}, ROOT)


@pytest.mark.parametrize("layer", ["source", "dataset"])
@pytest.mark.parametrize(
    "mutation", ["extra_field", "missing_field", "invalid_year", "duplicate_header"]
)
async def test_invalid_csv_is_not_silently_truncated(
    layer: str, mutation: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    rows = list(
        csv.reader(io.StringIO(ORIGINAL.read_bytes().decode("windows-1252")), delimiter=";")
    )
    if mutation == "extra_field":
        rows[-1].append("UNCLASSIFIED_EXTRA")
    elif mutation == "missing_field":
        rows[-1].pop()
    elif mutation == "invalid_year":
        rows[-1][rows[0].index("ANO_APOLICE")] = "NOT_A_YEAR"
    else:
        for row in rows:
            row.append(row[0])
    text = io.StringIO(newline="")
    csv.writer(text, delimiter=";", lineterminator="\n").writerows(rows)
    path = tmp_path / "mutated.csv"
    path.write_bytes(text.getvalue().encode("windows-1252"))
    seen = install(monkeypatch, path)
    function = api.apolices if layer == "source" else datasets.seguro_rural
    with helpers.levanta_exatamente(ParseError, match=MOTIVOS[mutation]) as caught:
        await function(ano_inicio=2006, ano_fim=2015)
    if layer == "dataset":
        assert caught.value.errors and all(kind == "parse" for _, kind, _ in caught.value.errors)
        assert isinstance(caught.value.__cause__, ParseError)
    helpers.assert_replay_served(seen)


def install_publication(
    monkeypatch: pytest.MonkeyPatch,
    *,
    originals: Path | None = None,
    replacements: dict[str, Path] | None = None,
) -> dict[str, list[str]]:
    requests = []
    for resource in MANIFEST["resources"]:
        raw = resource["original"]
        path = originals / raw["file"] if originals else GOLDEN / resource["derived_file"]
        if replacements:
            path = replacements.get(resource["derived_file"], path)
        requests.append(
            {
                "match": {"path": raw["requested_url"], "params": {}, "skip": 0},
                "file": str(path),
                "content_type": "text/csv",
            }
        )
    return helpers.install_replay_http(monkeypatch, {"requests": requests}, GOLDEN)


def assert_publication(frame: pd.DataFrame, case: dict[str, Any]) -> None:
    assert set(frame.columns) == {*case["columns"], "cod_municipio", *models.COLUNAS_DATA}
    assert frame["cod_municipio"].astype(object).where(
        frame["cod_municipio"].notna(), None
    ).tolist() == [None if pd.isna(codigo) else int(codigo) for codigo in frame["cd_ibge"]]
    assert len(frame) == len(case["expected"]), (case["id"], len(frame), len(case["expected"]))
    columns = sorted(case["columns"])
    expected = Counter(tuple(row[column] for column in columns) for row in case["expected"])
    actual = Counter(
        tuple(None if pd.isna(value) else value for value in row)
        for row in frame[columns].itertuples(index=False, name=None)
    )
    assert actual == expected, (
        case["id"],
        "rows disagree with original CSV fields",
        sum((expected - actual).values()),
        sum((actual - expected).values()),
    )


@pytest.mark.parametrize("layer", ["source", "dataset"])
@pytest.mark.parametrize("case", MANIFEST["cases"], ids=lambda case: case["id"])
async def test_publicacao_psr_celulas_originais(
    layer: str, case: dict[str, Any], monkeypatch: pytest.MonkeyPatch
):
    seen = install_publication(monkeypatch)
    kwargs = dict(case["query"])
    kwargs["produto"] = kwargs.pop("cultura")
    with helpers.sem_excecao():
        if layer == "source":
            function = api.apolices if case["tipo"] == "apolices" else api.sinistros
            frame, meta = await function(**kwargs, return_meta=True)
        else:
            frame, meta = await datasets.seguro_rural(**kwargs, tipo=case["tipo"], return_meta=True)
    assert_publication(frame, case)
    helpers.assert_replay_served(seen)
    assert meta.selected_source == "mapa_psr"
    assert meta.attempted_sources == ["mapa_psr"]
    assert meta.records_count == len(frame)
    resource = next(
        item for item in MANIFEST["resources"] if item["original"]["family"] == case["family"]
    )
    assert meta.source_url == resource["original"]["requested_url"]


@pytest.mark.parametrize("resource", MANIFEST["resources"], ids=lambda item: item["derived_file"])
def test_psr_recorte_tem_ultima_linha_e_campos_pii_vazios(resource: dict[str, Any]):
    path = GOLDEN / resource["derived_file"]
    assert hashlib.sha256(path.read_bytes()).hexdigest() == resource["derived_sha256"]
    assert resource["last_original_record"] in resource["selected_original_records"]
    with path.open(encoding="utf-8", newline="") as stream:
        rows = list(csv.DictReader(stream, delimiter=";"))
    assert len(rows) == len(resource["selected_original_records"])
    for row in rows:
        assert row["NM_SEGURADO"] == row["NR_DOCUMENTO_SEGURADO"] == ""


@pytest.mark.parametrize("layer", ["source", "dataset"])
@pytest.mark.parametrize("token", ["NULL", "NA", "None", "N/A", ""])
async def test_psr_texto_literal_na_e_numerico_nulo(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, layer: str, token: str
):
    resource = next(
        item for item in MANIFEST["resources"] if item["original"]["family"] == "psr:2006-2015"
    )
    with (GOLDEN / resource["derived_file"]).open(encoding="utf-8", newline="") as stream:
        reader = csv.DictReader(stream, delimiter=";")
        headers = reader.fieldnames
        row = next(item for item in reader if item["NR_APOLICE"] == "NULL")
    columns = {
        "NR_APOLICE": "nr_apolice",
        "NM_MUNICIPIO_PROPRIEDADE": "municipio",
        "NM_CULTURA_GLOBAL": "cultura",
        "NM_CLASSIF_PRODUTO": "classificacao",
        "NM_RAZAO_SOCIAL": "seguradora",
        "CD_GEOCMU": "cd_ibge",
        "EVENTO_PREPONDERANTE": "evento",
    }
    for name in columns:
        row[name] = token
    row["VALOR_INDENIZAÇÃO"] = token
    path = tmp_path / "literal_tokens.csv"
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=headers, delimiter=";", lineterminator="\n")
        writer.writeheader()
        writer.writerow(row)
    seen = install(monkeypatch, path)
    fetch = api.apolices if layer == "source" else datasets.seguro_rural
    frame = await fetch(ano=2014)
    assert len(frame) == 1
    for field in columns.values():
        if field == "cd_ibge" and token == "":
            assert pd.isna(frame.iloc[0][field])
            continue
        expected = (
            token.upper()
            if field in {"municipio", "cultura", "classificacao"}
            else token.lower()
            if field == "evento"
            else token
        )
        assert frame.iloc[0][field] == expected
    assert pd.isna(frame.iloc[0]["valor_indenizacao"])
    helpers.assert_replay_served(seen)


def mutated_csv(mutation: str, target: Path) -> Path:
    resource = MANIFEST["resources"][0]
    with (GOLDEN / resource["derived_file"]).open(encoding="utf-8", newline="") as stream:
        rows = list(csv.reader(stream, delimiter=";"))
    if mutation == "value":
        column = rows[0].index("VL_PREMIO_LIQUIDO")
        value = Decimal(rows[-1][column].replace(".", "").replace(",", "."))
        rows[-1][column] = str(value + Decimal("0.01")).replace(".", ",")
    elif mutation == "period":
        rows[-1][rows[0].index("ANO_APOLICE")] = "2024"
    elif mutation == "key":
        rows[-1][rows[0].index("CD_GEOCMU")] += "0"
    elif mutation == "last_record":
        rows.pop()
    elif mutation == "header":
        rows[0][rows[0].index("NR_ANIMAL")] = "NOVA_COLUNA"
    elif mutation == "width":
        rows[-1].append("EXTRA")
    else:
        raise ValueError(mutation)
    with target.open("w", encoding="utf-8", newline="") as stream:
        csv.writer(stream, delimiter=";", lineterminator="\n").writerows(rows)
    return target


@pytest.mark.parametrize("layer", ["source", "dataset"])
@pytest.mark.parametrize("mutation", ["value", "period", "key", "last_record"])
async def test_psr_mutacao_corpo_altera_oraculo(
    layer: str, mutation: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    resource = MANIFEST["resources"][0]
    case = next(
        case
        for case in MANIFEST["cases"]
        if case["family"] == "psr:2025" and "last" in case["labels"] and case["tipo"] == "apolices"
    )
    path = mutated_csv(mutation, tmp_path / "mutated.csv")
    seen = install_publication(monkeypatch, replacements={resource["derived_file"]: path})
    kwargs = dict(case["query"])
    kwargs["produto"] = kwargs.pop("cultura")
    with pytest.raises((AssertionError, ParseError)):
        if layer == "source":
            frame = await api.apolices(**kwargs)
        else:
            frame = await datasets.seguro_rural(**kwargs)
        assert_publication(frame, case)
    helpers.assert_replay_served(seen)


@pytest.mark.parametrize("resource", MANIFEST["resources"], ids=lambda item: item["derived_file"])
def test_n1_psr_estrutura_conhecida(resource: dict[str, Any]):
    assert reconciliation.compare_csv(GOLDEN / resource["derived_file"], resource)["status"] == "ok"


@pytest.mark.parametrize("mutation", ["header", "width", "period"])
def test_n1_psr_deriva_e_temporalidade(mutation: str, tmp_path: Path):
    path = mutated_csv(mutation, tmp_path / "mutated.csv")
    assert reconciliation.compare_csv(path, MANIFEST["resources"][0])["status"] == "mismatch"


def test_n1_psr_catalogo_conhecido():
    original = json.loads((GOLDEN / "catalog.json").read_bytes())
    assert reconciliation.compare_catalog(original, original)["status"] == "ok"


@pytest.mark.parametrize("mutation", ["missing", "new", "renamed", "duplicate", "sem_success"])
def test_n1_psr_catalogo_desconhecido(mutation: str):
    body = (GOLDEN / "catalog.json").read_bytes()
    original, changed = json.loads(body), json.loads(body)
    resources = changed["result"]["resources"]
    if mutation == "missing":
        resources.pop()
    elif mutation == "new":
        resources.append({"id": "new", "url": "https://example.test/new.csv", "format": "CSV"})
    elif mutation == "renamed":
        resources[-1]["url"] = "https://example.test/replaced.xlsx"
    elif mutation == "sem_success":
        changed["success"] = False
    else:
        resources.append(resources[-1])
    assert reconciliation.compare_catalog(changed, original)["status"] == "mismatch"


def captura_reconciliacao(destino: Path, trocas: dict[str, bytes] | None = None) -> Path:
    destino.mkdir()
    recibos = []
    for resource in MANIFEST["resources"]:
        original = resource["original"]
        texto = (GOLDEN / resource["derived_file"]).read_text(encoding="utf-8")
        corpo = (trocas or {}).get(resource["derived_file"], texto.encode(resource["encoding"]))
        nome = Path(original["file"]).name
        (destino / nome).write_bytes(corpo)
        recibos.append(
            {
                "requested_url": original["requested_url"],
                "file": f"captura/{nome}",
                "sha256": hashlib.sha256(corpo).hexdigest(),
                "fetched_at": original["fetched_at"],
                "family": original["family"],
            }
        )
    catalogo = (GOLDEN / "catalog.json").read_bytes()
    (destino / "catalog.json").write_bytes(catalogo)
    recibos.append(
        {
            "requested_url": "https://dados.agricultura.gov.br/api/3/action/package_show",
            "file": "captura/catalog.json",
            "sha256": hashlib.sha256(catalogo).hexdigest(),
            "fetched_at": "2026-09-18T14:48:49+00:00",
            "family": "psr:catalog",
        }
    )
    (destino / "receipts.json").write_text(json.dumps(recibos), encoding="utf-8")
    return destino


def test_n1_psr_run_confere_a_captura_inteira(tmp_path: Path):
    captura = captura_reconciliacao(tmp_path / "captura")
    with helpers.sem_excecao():
        codigo = reconciliation.run(captura, tmp_path / "relatorio.json")
    relatorio = json.loads((tmp_path / "relatorio.json").read_text(encoding="utf-8"))
    assert codigo == 0
    assert [check["status"] for check in relatorio["checks"]] == ["ok"] * 4
    assert [check["rows"] for check in relatorio["checks"][:3]] == [
        len(resource["selected_original_records"]) for resource in MANIFEST["resources"]
    ]


def test_n1_psr_run_aponta_publicacao_divergente(tmp_path: Path):
    resource = MANIFEST["resources"][0]
    mutado = mutated_csv("width", tmp_path / "mutado.csv").read_text(encoding="utf-8")
    captura = captura_reconciliacao(
        tmp_path / "captura", {resource["derived_file"]: mutado.encode(resource["encoding"])}
    )
    with helpers.sem_excecao():
        codigo = reconciliation.run(captura, tmp_path / "relatorio.json")
    relatorio = json.loads((tmp_path / "relatorio.json").read_text(encoding="utf-8"))
    assert codigo == 1
    assert [check["status"] for check in relatorio["checks"]] == ["mismatch", "ok", "ok", "ok"]
