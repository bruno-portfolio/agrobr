"""Golden data tests para garantir não-regressão de parsing."""

from __future__ import annotations

import hashlib
import json
from decimal import Decimal
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from agrobr.exceptions import ParseError

GOLDEN_DIR = Path(__file__).parent / "golden_data"


def _discover_cases(
    source_filter: str | None = None,
    format_filter: str | None = None,
) -> list[tuple[str, Path]]:
    """Descobre golden test cases por fonte ou formato."""
    cases: list[tuple[str, Path]] = []
    if not GOLDEN_DIR.exists():
        return cases

    for source_dir in sorted(GOLDEN_DIR.iterdir()):
        if not source_dir.is_dir():
            continue
        if source_filter and source_dir.name != source_filter:
            continue

        for case_dir in sorted(source_dir.iterdir()):
            if not case_dir.is_dir():
                continue
            meta_path = case_dir / "metadata.json"
            if not meta_path.exists():
                continue

            if format_filter:
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
                if meta.get("format") != format_filter:
                    continue

            cases.append((f"{source_dir.name}/{case_dir.name}", case_dir))

    return cases


def get_golden_test_cases() -> list[tuple[str, Path]]:
    """Descobre todos os casos de teste golden para HTML (CEPEA)."""
    cases: list[tuple[str, Path]] = []
    if not GOLDEN_DIR.exists():
        return cases

    for source_dir in GOLDEN_DIR.iterdir():
        if not source_dir.is_dir():
            continue
        for case_dir in source_dir.iterdir():
            if not case_dir.is_dir():
                continue
            if (case_dir / "response.html").exists():
                meta_path = case_dir / "metadata.json"
                if meta_path.exists():
                    meta = json.loads(meta_path.read_text(encoding="utf-8"))
                    if meta.get("source") == "cepea":
                        cases.append((f"{source_dir.name}/{case_dir.name}", case_dir))
                else:
                    cases.append((f"{source_dir.name}/{case_dir.name}", case_dir))
    return cases


def get_conab_golden_test_cases() -> list[tuple[str, Path]]:
    cases: list[tuple[str, Path]] = []
    conab_dir = GOLDEN_DIR / "conab"
    if not conab_dir.exists():
        return cases

    for case_dir in conab_dir.iterdir():
        if not case_dir.is_dir():
            continue
        if (case_dir / "response.xlsx").exists():
            cases.append((f"conab/{case_dir.name}", case_dir))
    return cases


def _load_metadata(path: Path) -> dict[str, Any]:
    result: dict[str, Any] = json.loads((path / "metadata.json").read_text(encoding="utf-8"))
    return result


def _load_expected(path: Path) -> dict[str, Any]:
    result: dict[str, Any] = json.loads((path / "expected.json").read_text(encoding="utf-8"))
    return result


def _assert_dataframe_golden(df: pd.DataFrame, expected: dict[str, Any]) -> None:
    """Valida DataFrame contra expected.json genérico."""
    if "count" in expected:
        assert len(df) == expected["count"], f"Expected {expected['count']} records, got {len(df)}"
    if "count_min" in expected:
        assert len(df) >= expected["count_min"], (
            f"Expected >= {expected['count_min']} records, got {len(df)}"
        )

    if "columns" in expected:
        for col in expected["columns"]:
            assert col in df.columns, f"Missing column: {col}. Got: {df.columns.tolist()}"

    if "first_row" in expected and len(df) > 0:
        first = df.iloc[0]
        for key, val in expected["first_row"].items():
            actual = first[key]
            if val is None or (isinstance(val, float) and pd.isna(val)):
                assert pd.isna(actual), f"first_row[{key}]: expected NA/None, got {actual!r}"
            elif isinstance(val, float):
                assert actual == pytest.approx(val, rel=1e-4), (
                    f"first_row[{key}]: expected {val}, got {actual}"
                )
            else:
                assert str(actual) == str(val), (
                    f"first_row[{key}]: expected {val!r}, got {actual!r}"
                )

    if "last_row" in expected and len(df) > 0:
        last = df.iloc[-1]
        for key, val in expected["last_row"].items():
            actual = last[key]
            if val is None or (isinstance(val, float) and pd.isna(val)):
                assert pd.isna(actual), f"last_row[{key}]: expected NA/None, got {actual!r}"
            elif isinstance(val, float):
                assert actual == pytest.approx(val, rel=1e-4), (
                    f"last_row[{key}]: expected {val}, got {actual}"
                )
            else:
                assert str(actual) == str(val), f"last_row[{key}]: expected {val!r}, got {actual!r}"

    if "non_null_columns" in expected:
        for col in expected["non_null_columns"]:
            if col in df.columns:
                null_count = df[col].isna().sum()
                assert null_count == 0, f"Column {col} has {null_count} null values"


@pytest.mark.skipif(not get_golden_test_cases(), reason="No golden data available")
@pytest.mark.parametrize("_name,path", get_golden_test_cases())
def test_golden_parsing(_name: str, path: Path):
    """
    Testa parsing contra golden data.

    Garante que:
    1. Parser extrai mesma quantidade de registros
    2. Primeiro e último registro batem
    3. Checksum dos dados bate (se disponível)
    """
    html = (path / "response.html").read_text(encoding="utf-8")
    expected = json.loads((path / "expected.json").read_text(encoding="utf-8"))
    metadata = json.loads((path / "metadata.json").read_text(encoding="utf-8"))

    source = metadata["source"]
    produto = metadata["produto"]

    if source == "cepea":
        import asyncio

        from agrobr.cepea.parsers.detector import get_parser_with_fallback

        parser, results = asyncio.run(get_parser_with_fallback(html, produto, strict=False))
    else:
        pytest.skip(f"Golden tests for {source} not implemented")
        return

    assert len(results) == expected["count"], (
        f"Expected {expected['count']} records, got {len(results)}"
    )

    first = results[0]
    assert str(first.data) == expected["first"]["data"]
    assert first.valor == Decimal(expected["first"]["valor"])
    assert first.unidade == expected["first"]["unidade"]

    last = results[-1]
    assert str(last.data) == expected["last"]["data"]
    assert last.valor == Decimal(expected["last"]["valor"])

    if "checksum" in expected:
        dumps = [r.model_dump(mode="json", exclude={"parsed_at"}) for r in results]
        data_str = json.dumps(dumps, sort_keys=True)
        checksum = f"sha256:{hashlib.sha256(data_str.encode()).hexdigest()[:16]}"
        assert checksum == expected["checksum"], (
            f"Checksum mismatch: {checksum} != {expected['checksum']}"
        )


@pytest.mark.skipif(not get_golden_test_cases(), reason="No golden data available")
@pytest.mark.parametrize("_name,path", get_golden_test_cases())
def test_golden_fingerprint(_name: str, path: Path):
    html = (path / "response.html").read_text(encoding="utf-8")
    metadata = json.loads((path / "metadata.json").read_text(encoding="utf-8"))

    if metadata["source"] == "cepea":
        from agrobr.cepea.parsers.fingerprint import extract_fingerprint
        from agrobr.constants import Fonte

        fp = extract_fingerprint(html, Fonte.CEPEA, "test")

        assert fp.structure_hash, "No structure hash"


@pytest.mark.skipif(not get_golden_test_cases(), reason="No golden data available")
@pytest.mark.parametrize("_name,path", get_golden_test_cases())
def test_golden_parser_can_parse(_name: str, path: Path):
    html = (path / "response.html").read_text(encoding="utf-8")
    metadata = json.loads((path / "metadata.json").read_text(encoding="utf-8"))

    if metadata["source"] == "cepea":
        from agrobr.cepea.parsers.v1 import CepeaParserV1

        parser = CepeaParserV1()
        can_parse, confidence = parser.can_parse(html)

        assert can_parse, "Parser should be able to parse golden data"
        assert confidence >= 0.4, f"Confidence too low: {confidence}"


@pytest.mark.skipif(not get_conab_golden_test_cases(), reason="No CONAB golden data available")
@pytest.mark.parametrize("_name,path", get_conab_golden_test_cases())
def test_conab_golden_parsing_soja(_name: str, path: Path):
    from io import BytesIO

    from agrobr.conab.parsers.v1 import ConabParserV1

    xlsx_path = path / "response.xlsx"
    expected = json.loads((path / "expected.json").read_text(encoding="utf-8"))

    with open(xlsx_path, "rb") as f:
        xlsx = BytesIO(f.read())

    parser = ConabParserV1()
    safras = parser.parse_safra_produto(xlsx, "soja", safra_ref="2025/26")

    assert len(safras) == expected["soja"]["count"], (
        f"Expected {expected['soja']['count']} soja records, got {len(safras)}"
    )

    ufs_found = sorted({s.uf for s in safras if s.uf})
    assert ufs_found == expected["soja"]["ufs_found"], (
        f"UFs mismatch: {ufs_found} != {expected['soja']['ufs_found']}"
    )


@pytest.mark.skipif(not get_conab_golden_test_cases(), reason="No CONAB golden data available")
@pytest.mark.parametrize("_name,path", get_conab_golden_test_cases())
def test_conab_golden_parsing_milho(_name: str, path: Path):
    from io import BytesIO

    from agrobr.conab.parsers.v1 import ConabParserV1

    xlsx_path = path / "response.xlsx"
    expected = json.loads((path / "expected.json").read_text(encoding="utf-8"))

    with open(xlsx_path, "rb") as f:
        xlsx = BytesIO(f.read())

    parser = ConabParserV1()
    safras = parser.parse_safra_produto(xlsx, "milho", safra_ref="2025/26")

    assert len(safras) == expected["milho"]["count"], (
        f"Expected {expected['milho']['count']} milho records, got {len(safras)}"
    )


@pytest.mark.skipif(not get_conab_golden_test_cases(), reason="No CONAB golden data available")
@pytest.mark.parametrize("_name,path", get_conab_golden_test_cases())
def test_conab_golden_parsing_suprimento(_name: str, path: Path):
    from io import BytesIO

    from agrobr.conab.parsers.v1 import ConabParserV1

    xlsx_path = path / "response.xlsx"
    expected = json.loads((path / "expected.json").read_text(encoding="utf-8"))

    with open(xlsx_path, "rb") as f:
        xlsx = BytesIO(f.read())

    parser = ConabParserV1()
    suprimentos = parser.parse_suprimento(xlsx)

    assert len(suprimentos) == expected["suprimento"]["count"], (
        f"Expected {expected['suprimento']['count']} suprimento records, got {len(suprimentos)}"
    )


@pytest.mark.skipif(not get_conab_golden_test_cases(), reason="No CONAB golden data available")
@pytest.mark.parametrize("_name,path", get_conab_golden_test_cases())
def test_conab_golden_parsing_brasil_total(_name: str, path: Path):
    from io import BytesIO

    from agrobr.conab.parsers.v1 import ConabParserV1

    xlsx_path = path / "response.xlsx"
    expected = json.loads((path / "expected.json").read_text(encoding="utf-8"))

    with open(xlsx_path, "rb") as f:
        xlsx = BytesIO(f.read())

    parser = ConabParserV1()
    totais = parser.parse_brasil_total(xlsx)

    assert len(totais) == expected["brasil_total"]["count"], (
        f"Expected {expected['brasil_total']['count']} brasil_total records, got {len(totais)}"
    )


def _get_bcb_cases() -> list[tuple[str, Path]]:
    return _discover_cases(source_filter="bcb")


@pytest.mark.skipif(not _get_bcb_cases(), reason="No BCB golden data")
@pytest.mark.parametrize("_name,path", _get_bcb_cases())
def test_bcb_golden_parsing(_name: str, path: Path):
    from agrobr.bcb.parser import parse_credito_rural

    data = json.loads((path / "response.json").read_text(encoding="utf-8"))
    expected = _load_expected(path)
    metadata = _load_metadata(path)

    kwargs = metadata.get("parser_kwargs", {})
    df = parse_credito_rural(data, **kwargs)

    _assert_dataframe_golden(df, expected)

    if "produto" in df.columns:
        assert df["produto"].str.islower().all(), "produto should be lowercase"
    if "uf" in df.columns:
        assert df["uf"].str.isupper().all(), "uf should be uppercase"


def _get_inmet_cases() -> list[tuple[str, Path]]:
    return _discover_cases(source_filter="inmet")


@pytest.mark.skipif(not _get_inmet_cases(), reason="No INMET golden data")
@pytest.mark.parametrize("_name,path", _get_inmet_cases())
def test_inmet_golden_parsing(_name: str, path: Path):
    from agrobr.inmet.parser import parse_observacoes

    data = json.loads((path / "response.json").read_text(encoding="utf-8"))
    expected = _load_expected(path)

    df = parse_observacoes(data)

    _assert_dataframe_golden(df, expected)

    if expected.get("sentinel_handled"):
        assert "temperatura_max" in df.columns
        sentinel_rows = df[df["temperatura_max"] == -9999.0]
        assert len(sentinel_rows) == 0, "Sentinel -9999 should be replaced with NaN"


def _get_nasa_power_cases() -> list[tuple[str, Path]]:
    return _discover_cases(source_filter="nasa_power")


@pytest.mark.skipif(not _get_nasa_power_cases(), reason="No NASA POWER golden data")
@pytest.mark.parametrize("_name,path", _get_nasa_power_cases())
def test_nasa_power_golden_parsing(_name: str, path: Path):
    from agrobr.nasa_power.parser import parse_daily

    data = json.loads((path / "response.json").read_text(encoding="utf-8"))
    expected = _load_expected(path)
    metadata = _load_metadata(path)

    kwargs = metadata.get("parser_kwargs", {})
    df = parse_daily(data, **kwargs)

    _assert_dataframe_golden(df, expected)

    assert df["data"].is_monotonic_increasing, "data should be sorted ascending"


def _get_comexstat_cases() -> list[tuple[str, Path]]:
    return _discover_cases(source_filter="comexstat")


@pytest.mark.skipif(not _get_comexstat_cases(), reason="No ComexStat golden data")
@pytest.mark.parametrize("_name,path", _get_comexstat_cases())
def test_comexstat_golden_parsing(_name: str, path: Path):
    import csv
    import dataclasses
    import io

    from agrobr.comexstat import models, parser, query

    metadata = _load_metadata(path)
    if path.name == "importacao_oleo_sample":
        body = (path / "response.csv").read_bytes()
        assert hashlib.sha256(body).hexdigest() == metadata["sha256"]
        rows = list(csv.DictReader(io.StringIO(body.decode("utf-8")), delimiter=";"))
        assert sum(int(row["KG_LIQUIDO"]) for row in rows) == 476697
        assert sum(int(row["VL_FOB"]) for row in rows) == 459899
        with pytest.raises(ParseError, match="Projeção divergente"):
            parser.parse_resource(
                io.BytesIO(body),
                query.build_query(fluxo="importacao", produto="oleo_soja", ano=2024),
            )
        return

    aliases = {
        "CO_ANO": "ano",
        "CO_MES": "mes",
        "CO_NCM": "ncm",
        "CO_UNID": "cod_unidade",
        "CO_PAIS": "cod_pais",
        "SG_UF_NCM": "uf",
        "CO_VIA": "cod_via",
        "CO_URF": "cod_urf",
        "QT_ESTAT": "qtd_estatistica",
        "KG_LIQUIDO": "kg_liquido",
        "VL_FOB": "valor_fob_usd",
        "VL_FRETE": "valor_frete_usd",
        "VL_SEGURO": "valor_seguro_usd",
        "NO_UNID": "unidade",
        "SG_UNID": "sigla_unidade",
        "CO_PAIS_ISON3": "cod_pais_iso_numerico",
        "CO_PAIS_ISOA3": "cod_pais_iso_alfa3",
        "NO_PAIS": "pais",
        "NO_PAIS_ING": "pais_ingles",
        "NO_PAIS_ESP": "pais_espanhol",
        "NO_VIA": "via",
        "NO_URF": "urf",
    }
    tables = {
        "NCM_UNIDADE.csv": "unidades",
        "PAIS.csv": "paises",
        "VIA.csv": "vias",
        "URF.csv": "urfs",
    }
    files = sorted(path.glob("*.csv"))
    assert len(files) == (8 if path.name == "integridade20260908" else 1)
    for file in files:
        body = file.read_bytes()
        if "artifacts" in metadata:
            artifact = metadata["artifacts"][file.name]
            assert len(body) == artifact["size_bytes"]
            assert hashlib.sha256(body).hexdigest() == artifact["sha256"]
        try:
            text = body.decode("utf-8-sig")
        except UnicodeDecodeError:
            text = body.decode("cp1252")
        reader = csv.DictReader(io.StringIO(text), delimiter=";")
        raw = list(reader)
        assert reader.fieldnames is not None
        names = [aliases[name] for name in reader.fieldnames]
        if file.name in tables:
            parsed = parser.parse_dictionary(io.BytesIO(body), tabela=tables[file.name])
            expected = pd.DataFrame.from_records(raw).rename(columns=aliases)
            expected = expected.astype("string[python]")
            pd.testing.assert_frame_equal(parsed.frame, expected, check_exact=True)
        else:
            flow = "importacao" if file.name.startswith("IMP_") else "exportacao"
            for ncm in sorted({row["CO_NCM"] for row in raw}):
                selected = [row for row in raw if row["CO_NCM"] == ncm]
                parsed = parser.parse_resource(
                    io.BytesIO(body),
                    dataclasses.replace(
                        query.build_query(
                            fluxo=flow,
                            produto="soja",
                            ano=int(raw[0]["CO_ANO"]),
                            agregacao="detalhado",
                        ),
                        ncm=models.SelecaoNcm((ncm,)),
                    ),
                )
                expected = pd.DataFrame.from_records(selected).rename(columns=aliases)
                for name in names:
                    if name in {"ano", "mes", "qtd_estatistica"}:
                        expected[name] = pd.Series(
                            [int(v) if v else None for v in expected[name]], dtype="Int64"
                        )
                    elif name.startswith("valor_") or name == "kg_liquido":
                        expected[name] = pd.Series(
                            [float(Decimal(v)) if v else None for v in expected[name]],
                            dtype="float64",
                        )
                    else:
                        expected[name] = expected[name].astype("string[python]")
                pd.testing.assert_frame_equal(parsed.frame, expected, check_exact=True)
                assert parsed.details["validated_rows"] == len(raw)
        assert parsed.details["eof_reached"]


def _get_na_cases() -> list[tuple[str, Path]]:
    return _discover_cases(source_filter="na")


@pytest.mark.skipif(not _get_na_cases(), reason="No NA golden data")
@pytest.mark.parametrize("_name,path", _get_na_cases())
def test_na_golden_parsing(_name: str, path: Path):
    from agrobr.noticias_agricolas.parser import parse_indicador

    html = (path / "response.html").read_text(encoding="utf-8")
    expected = _load_expected(path)
    metadata = _load_metadata(path)

    kwargs = metadata.get("parser_kwargs", {})
    indicadores = parse_indicador(html, **kwargs)

    assert len(indicadores) == expected["count"], (
        f"Expected {expected['count']} indicadores, got {len(indicadores)}"
    )

    if "first" in expected:
        first = indicadores[0]
        exp_first = expected["first"]
        assert str(first.data) == exp_first["data"]
        assert first.valor == Decimal(exp_first["valor"])
        assert first.unidade == exp_first["unidade"]
        assert first.praca == exp_first["praca"]

    if "last" in expected:
        last = indicadores[-1]
        exp_last = expected["last"]
        assert str(last.data) == exp_last["data"]
        assert last.valor == Decimal(exp_last["valor"])
        assert last.unidade == exp_last["unidade"]


def _get_ibge_cases() -> list[tuple[str, Path]]:
    return _discover_cases(source_filter="ibge", format_filter="dataframe")


@pytest.mark.skipif(not _get_ibge_cases(), reason="No IBGE golden data")
@pytest.mark.parametrize("_name,path", _get_ibge_cases())
def test_ibge_golden_parsing(_name: str, path: Path):
    from agrobr.ibge.client import parse_sidra_response

    csv_path = path / "response.csv"
    expected = _load_expected(path)

    df_raw = pd.read_csv(csv_path, dtype=str, encoding="utf-8")
    df = parse_sidra_response(df_raw)

    _assert_dataframe_golden(df, expected)

    if "valor" in df.columns:
        assert pd.api.types.is_numeric_dtype(df["valor"]), "valor should be numeric after parsing"


def _get_deral_cases() -> list[tuple[str, Path]]:
    return _discover_cases(source_filter="deral")


@pytest.mark.skipif(not _get_deral_cases(), reason="No DERAL golden data")
@pytest.mark.parametrize("_name,path", _get_deral_cases())
def test_deral_golden_parsing(_name: str, path: Path):
    from agrobr.deral.parser import parse_pc_xls

    xlsx_path = path / "response.xls"
    if not xlsx_path.exists():
        xlsx_path = path / "response.xlsx"
    expected = _load_expected(path)

    data = xlsx_path.read_bytes()
    df = parse_pc_xls(data)

    _assert_dataframe_golden(df, expected)

    if expected.get("has_condicao") and "condicao" in df.columns:
        condicoes = set(df[df["condicao"] != ""]["condicao"].unique())
        for c in expected.get("condicoes_expected", []):
            assert c in condicoes, f"Missing condicao: {c}. Got: {condicoes}"

    if "produto_expected" in expected and "produto" in df.columns:
        assert (df["produto"] == expected["produto_expected"]).all()


def _get_abiove_cases() -> list[tuple[str, Path]]:
    return _discover_cases(source_filter="abiove")


@pytest.mark.skipif(not _get_abiove_cases(), reason="No ABIOVE golden data")
@pytest.mark.parametrize("_name,path", _get_abiove_cases())
def test_abiove_golden_parsing(_name: str, path: Path):
    from agrobr.abiove.parser import parse_exportacao_excel

    xlsx_path = path / "response.xlsx"
    expected = _load_expected(path)
    metadata = _load_metadata(path)

    data = xlsx_path.read_bytes()
    kwargs = metadata.get("parser_kwargs", {})
    df = parse_exportacao_excel(data, **kwargs)

    _assert_dataframe_golden(df, expected)

    if expected.get("has_multiple_products") and "produto" in df.columns:
        produtos = set(df["produto"].unique())
        for p in expected.get("produtos_expected", []):
            assert p in produtos, f"Missing produto: {p}. Got: {produtos}"


def _get_antaq_cases() -> list[tuple[str, Path]]:
    return _discover_cases(source_filter="antaq")


@pytest.mark.skipif(not _get_antaq_cases(), reason="No ANTAQ golden data")
@pytest.mark.parametrize("_name,path", _get_antaq_cases())
def test_antaq_golden_parsing(_name: str, path: Path):
    from agrobr.antaq.parser import (
        join_movimentacao,
        parse_atracacao,
        parse_carga,
        parse_mercadoria,
    )

    atracacao_txt = (path / "atracacao.txt").read_text(encoding="utf-8")
    carga_txt = (path / "carga.txt").read_text(encoding="utf-8")
    mercadoria_txt = (path / "mercadoria.txt").read_text(encoding="utf-8")
    expected = _load_expected(path)

    df_a = parse_atracacao(atracacao_txt)
    df_c = parse_carga(carga_txt)
    df_m = parse_mercadoria(mercadoria_txt)
    df = join_movimentacao(df_a, df_c, df_m)

    _assert_dataframe_golden(df, expected)

    if "ufs_expected" in expected and "uf" in df.columns:
        ufs = sorted(df["uf"].dropna().unique().tolist())
        assert ufs == expected["ufs_expected"], f"UFs: {ufs} != {expected['ufs_expected']}"

    if "ano" in df.columns:
        assert pd.api.types.is_integer_dtype(df["ano"]), "ano should be integer"
    if "mes" in df.columns:
        assert pd.api.types.is_integer_dtype(df["mes"]), "mes should be integer"
    if "peso_bruto_ton" in df.columns:
        assert pd.api.types.is_numeric_dtype(df["peso_bruto_ton"]), (
            "peso_bruto_ton should be numeric"
        )
        non_null_peso = df["peso_bruto_ton"].dropna()
        assert (non_null_peso >= 0).all(), "peso_bruto_ton should be >= 0"


def _get_anp_diesel_cases() -> list[tuple[str, Path]]:
    return _discover_cases(source_filter="anp_diesel")


@pytest.mark.slow
@pytest.mark.skipif(not _get_anp_diesel_cases(), reason="No ANP diesel golden data")
@pytest.mark.parametrize("_name,path", _get_anp_diesel_cases())
def test_anp_diesel_golden_parsing(_name: str, path: Path):
    from agrobr.alt.anp_diesel.parser import parse_precos, parse_vendas

    expected = _load_expected(path)
    metadata = _load_metadata(path)

    response_path = path / f"response.{metadata['format']}"
    assert response_path.exists(), f"Missing golden response: {response_path}"
    data = response_path.read_bytes()
    if "sha256" in metadata:
        assert hashlib.sha256(data).hexdigest() == metadata["sha256"]
    parser_fns = metadata.get("parser_functions", [])

    if "parse_precos" in parser_fns:
        df = parse_precos(data)
    elif "parse_vendas" in parser_fns:
        df = parse_vendas(data)
    else:
        pytest.skip(f"Unknown parser functions: {parser_fns}")
        return

    _assert_dataframe_golden(df, expected)

    if "published_negative_rows" in expected.get("checks", {}):
        negative = df[df["volume_m3"] < 0]
        assert len(negative) == expected["checks"]["published_negative_rows"]
        assert negative["uf"].tolist() == ["SE"]
        assert negative["volume_m3"].tolist() == [-70.0]
        assert negative["data"].dt.strftime("%Y-%m-%d").tolist() == ["2025-12-01"]
    if "parse_vendas" in parser_fns:
        filtered = parse_vendas(data, uf="RO")
        assert len(filtered) == 815
        assert set(filtered["uf"]) == {"RO"}
        assert df["uf"].nunique() == 27

    if expected.get("checks", {}).get("all_products_are_diesel"):
        assert df["produto"].str.upper().str.contains("DIESEL").all(), (
            "All products should contain DIESEL"
        )
    if expected.get("checks", {}).get("all_products_contain_diesel"):
        assert df["produto"].str.upper().str.contains("DIESEL").all(), (
            "All products should contain DIESEL"
        )
    if expected.get("checks", {}).get("data_column_is_datetime"):
        assert pd.api.types.is_datetime64_any_dtype(df["data"]), "data should be datetime"
    if expected.get("checks", {}).get("volume_m3_positive") and "volume_m3" in df.columns:
        assert (df["volume_m3"].dropna() > 0).all(), "volume_m3 should be positive"
    if (
        expected.get("checks", {}).get("margem_equals_venda_minus_compra")
        and "margem" in df.columns
    ):
        diff = (df["preco_venda"] - df["preco_compra"] - df["margem"]).abs()
        assert (diff < 0.01).all(), "margem should equal preco_venda - preco_compra"


def _get_mapa_psr_cases() -> list[tuple[str, Path]]:
    return _discover_cases(source_filter="mapa_psr")


def _get_antt_pedagio_cases() -> list[tuple[str, Path]]:
    return _discover_cases(source_filter="antt_pedagio")


@pytest.mark.skipif(not _get_antt_pedagio_cases(), reason="No ANTT Pedagio golden data")
@pytest.mark.parametrize("_name,path", _get_antt_pedagio_cases())
def test_antt_pedagio_golden_parsing(_name: str, path: Path):
    import csv
    import io
    import re
    from datetime import datetime

    from agrobr.alt.antt_pedagio.parser import parse_trafego_file

    metadata = _load_metadata(path)
    if "samples" in metadata:
        for sample in metadata["samples"].values():
            body = (path / sample["file"]).read_bytes()
            assert hashlib.sha256(body).hexdigest() == sample["sha256"]
            assert len(body) == sample["size_bytes"]
            raw = list(csv.DictReader(io.StringIO(body.decode("cp1252")), delimiter=";"))
            assert raw == [record["cells"] for record in sample["records"]]
            result = parse_trafego_file(
                io.BytesIO(body), ano=sample["year"], frequencia=sample["frequency"]
            )
            rows = []
            for source in raw:
                category = source.get("categoria_eixo", source.get("categoria"))
                match = re.fullmatch(
                    r"(?:Ve[íi]culo (?:Comercial|Passeio) )?([0-9]+) eixos?",
                    category,
                    re.IGNORECASE,
                )
                reference = source["mes_ano"]
                rows.append(
                    {
                        "data": datetime.strptime(
                            reference, "%d/%m/%Y" if reference.count("/") == 2 else "%m/%Y"
                        ),
                        "concessionaria": source["concessionaria"].strip(),
                        "praca": source["praca"].strip(),
                        "sentido": source["sentido"].strip().upper(),
                        "n_eixos": int(match[1]) if match else None,
                        "tipo_veiculo": source["tipo_de_veiculo"].strip(),
                        "volume": int(Decimal(source["volume_total"].replace(",", "."))),
                        "rodovia": None,
                        "uf": None,
                        "municipio": None,
                        "categoria_eixo": category.strip(),
                        "tipo_cobranca": source["tipo_cobranca"].strip(),
                        "frequencia": sample["frequency"],
                    }
                )
            expected_frame = pd.DataFrame.from_records(rows)
            for name in expected_frame:
                dtype = (
                    "datetime64[ns]"
                    if name == "data"
                    else "Int64"
                    if name in {"volume", "n_eixos"}
                    else "string[python]"
                )
                expected_frame[name] = expected_frame[name].astype(dtype)
            pd.testing.assert_frame_equal(result.frame, expected_frame, check_exact=True)
            assert result.diagnostics["eof_reached"]
            assert result.diagnostics["validated_rows"] == len(raw)
        return
    pytest.fail(f"Golden ANTT sem amostras oficiais: {path}")


@pytest.mark.skipif(not _get_mapa_psr_cases(), reason="No MAPA PSR golden data")
@pytest.mark.parametrize("_name,path", _get_mapa_psr_cases())
def test_mapa_psr_golden_parsing(_name: str, path: Path):
    from agrobr.alt.mapa_psr.parser import parse_apolices, parse_sinistros

    expected = _load_expected(path)
    metadata = _load_metadata(path)

    csv_path = path / "response.csv"
    if not csv_path.exists():
        pytest.skip(f"No response.csv in {path}")
        return

    data = csv_path.read_bytes()
    parser_fns = metadata.get("parser_functions", [])

    if "parse_sinistros" in parser_fns:
        df = parse_sinistros(data)
    elif "parse_apolices" in parser_fns:
        df = parse_apolices(data)
    else:
        pytest.skip(f"Unknown parser functions: {parser_fns}")
        return

    _assert_dataframe_golden(df, expected)

    if expected.get("checks", {}).get("pii_removed"):
        assert "NM_SEGURADO" not in df.columns, "PII column NM_SEGURADO should be removed"
        assert "NR_DOCUMENTO_SEGURADO" not in df.columns, "PII column should be removed"
    if expected.get("checks", {}).get("all_indenizacao_positive"):
        assert (df["valor_indenizacao"] > 0).all(), "All valor_indenizacao should be > 0"
    if expected.get("checks", {}).get("all_evento_non_empty"):
        assert (df["evento"].str.strip() != "").all(), "All evento should be non-empty"
    if expected.get("checks", {}).get("evento_is_lowercase"):
        assert all(v == v.lower() for v in df["evento"]), "evento should be lowercase"
    if expected.get("checks", {}).get("sorted_by_ano"):
        anos = df["ano_apolice"].tolist()
        assert anos == sorted(anos), "Should be sorted by ano_apolice"
    if expected.get("checks", {}).get("ano_apolice_is_int"):
        assert df["ano_apolice"].dtype == "Int64", "ano_apolice should be Int64"
    if expected.get("checks", {}).get("area_total_is_float"):
        assert df["area_total"].dtype == "float64", "area_total should be float64"


def _get_b3_cases() -> list[tuple[str, Path]]:
    return _discover_cases(source_filter="b3")


@pytest.mark.skipif(not _get_b3_cases(), reason="No B3 golden data")
@pytest.mark.parametrize("_name,path", _get_b3_cases())
def test_b3_golden_parsing(_name: str, path: Path):
    expected = _load_expected(path)

    if (path / "response.csv").exists():
        from agrobr.b3.parser import parse_posicoes_abertas

        csv_bytes = (path / "response.csv").read_bytes()
        df = parse_posicoes_abertas(csv_bytes)

        for col in expected["columns"]:
            assert col in df.columns, f"Missing column: {col}"
        assert len(df) == expected["total_rows"]

        futures = df[df["tipo"] == "futuro"]
        options = df[df["tipo"] == "opcao"]
        assert len(futures) == expected["futures_count"]
        assert len(options) == expected["options_count"]

        for sample_key in ("sample_bgi", "sample_ccm"):
            if sample_key in expected:
                sample = expected[sample_key]
                row = df[
                    (df["ticker"] == sample["ticker"])
                    & (df["ticker_completo"] == sample["ticker_completo"])
                ].iloc[0]
                assert row["posicoes_abertas"] == sample["posicoes_abertas"]
                assert row["variacao_posicoes"] == sample["variacao_posicoes"]
    else:
        pytest.skip(f"No recognized response file in {path}")


def _get_comtrade_cases() -> list[tuple[str, Path]]:
    cases = _discover_cases(source_filter="comtrade")
    official = GOLDEN_DIR / "comtrade/selecao_20260906"
    if (official / "manifest.json").exists():
        cases.append(("comtrade/selecao_20260906", official))
    return cases


@pytest.mark.skipif(not _get_comtrade_cases(), reason="No Comtrade golden data")
@pytest.mark.parametrize("_name,path", _get_comtrade_cases())
def test_comtrade_golden_parsing(_name: str, path: Path):
    from agrobr.comtrade.parser import parse_mirror, parse_trade_data

    if path.name == "selecao_20260906":
        manifest = json.loads((path / "manifest.json").read_bytes())
        frames = {}
        for artifact in manifest["artifacts"]:
            if artifact["name"] not in {
                "soy_br_cn_2021",
                "soy_br_cn_2023",
                "soy_cn_br_2023_mirror",
            }:
                continue
            body = (path / artifact["body_file"]).read_bytes()
            assert len(body) == artifact["size_bytes"]
            assert hashlib.sha256(body).hexdigest() == artifact["sha256"]
            raw = json.loads(body)["data"]
            assert len(raw) == 1
            frame = parse_trade_data(raw)
            assert len(frame) == 1 and len(frame.columns) == 27
            for source, target in {
                "period": "periodo",
                "reporterCode": "reporter_code",
                "reporterISO": "reporter_iso",
                "reporterDesc": "reporter",
                "partnerCode": "partner_code",
                "partnerISO": "partner_iso",
                "partnerDesc": "partner",
                "flowCode": "fluxo_code",
                "flowDesc": "fluxo",
                "cmdCode": "hs_code",
                "cmdDesc": "produto_desc",
                "netWgt": "peso_liquido_kg",
                "grossWgt": "peso_bruto_kg",
                "fobvalue": "valor_fob_usd",
                "cifvalue": "valor_cif_usd",
                "primaryValue": "valor_primario_usd",
                "qty": "quantidade",
                "qtyUnitAbbr": "unidade_qtd",
                "aggrLevel": "nivel_hs",
                "classificationCode": "classificacao",
                "isOriginalClassification": "classificacao_original",
                "isNetWgtEstimated": "peso_liquido_estimado",
                "isGrossWgtEstimated": "peso_bruto_estimado",
                "isQtyEstimated": "quantidade_estimada",
                "refYear": "ano",
            }.items():
                value = raw[0][source]
                actual = frame.iloc[0][target]
                assert pd.isna(actual) if value is None else actual == value
            assert pd.isna(frame.iloc[0]["mes"])
            assert frame.iloc[0]["volume_ton"] == raw[0]["netWgt"] / 1000
            frames[artifact["name"]] = frame
        assert len(frames) == 3
        mirror = parse_mirror(
            frames["soy_br_cn_2023"], frames["soy_cn_br_2023_mirror"], "BRA", "CHN"
        )
        oracle = json.loads((path / "oracles.json").read_bytes())["mirror"]
        assert len(mirror) == 1
        assert mirror.iloc[0]["diff_peso_kg"] == oracle["diff_peso_kg"]
        assert mirror.iloc[0]["ratio_valor"] == oracle["ratio_fob_cif"]
        assert mirror.iloc[0]["ratio_peso"] == oracle["ratio_peso"]
        return

    expected = _load_expected(path)
    metadata = _load_metadata(path)

    if (path / "response.json").exists():
        raw = json.loads((path / "response.json").read_text(encoding="utf-8"))
        records = raw.get("data", raw) if isinstance(raw, dict) else raw
        assert len(records) == expected["record_count"]
        assert {r["motCode"] for r in records} == {0, 1000, 2000, 2100}
        with pytest.raises(ParseError, match="motCode"):
            parse_trade_data(records)
        aggregate_records = [r for r in records if r["motCode"] == 0]
        df = parse_trade_data(aggregate_records)
        assert len(df) == len(aggregate_records) == 3
        for raw in aggregate_records:
            row = df.loc[df["hs_code"].eq(raw["cmdCode"])].iloc[0]
            assert row["peso_liquido_kg"] == raw["netWgt"]
            assert row["valor_fob_usd"] == raw["fobvalue"]
            assert row["volume_ton"] == raw["netWgt"] / 1000

    elif (path / "response_reporter.json").exists():
        raw_rep = json.loads((path / "response_reporter.json").read_text(encoding="utf-8"))
        raw_par = json.loads((path / "response_partner.json").read_text(encoding="utf-8"))
        recs_rep = raw_rep.get("data", raw_rep) if isinstance(raw_rep, dict) else raw_rep
        recs_par = raw_par.get("data", raw_par) if isinstance(raw_par, dict) else raw_par

        with pytest.raises(ParseError, match="motCode"):
            parse_trade_data(recs_rep)
        df_rep = parse_trade_data([r for r in recs_rep if r["motCode"] == 0])
        df_par = parse_trade_data(recs_par)

        kwargs = metadata.get("parser_kwargs", {})
        df = parse_mirror(df_rep, df_par, **kwargs)

        assert len(df) == 1
        _assert_dataframe_golden(df, expected)
    else:
        pytest.skip(f"No recognized response file in {path}")


def _get_queimadas_cases() -> list[tuple[str, Path]]:
    return _discover_cases(source_filter="queimadas")


@pytest.mark.skipif(not _get_queimadas_cases(), reason="No Queimadas golden data")
@pytest.mark.parametrize("_name,path", _get_queimadas_cases())
def test_queimadas_golden_parsing(_name: str, path: Path):
    from agrobr.queimadas.parser import parse_focos_csv

    expected = _load_expected(path)
    data = (path / "response.csv").read_bytes()
    df = parse_focos_csv(data)

    assert len(df) == expected["record_count"]
    _assert_dataframe_golden(df, expected)


def _get_conab_ceasa_cases() -> list[tuple[str, Path]]:
    return _discover_cases(source_filter="conab_ceasa")


@pytest.mark.skipif(not _get_conab_ceasa_cases(), reason="No CONAB CEASA golden data")
@pytest.mark.parametrize("_name,path", _get_conab_ceasa_cases())
def test_conab_ceasa_golden_parsing(_name: str, path: Path):
    from agrobr.conab.ceasa.parser import parse_precos

    expected = _load_expected(path)
    precos_json = json.loads((path / "precos_response.json").read_text(encoding="utf-8"))
    df = parse_precos(precos_json)

    for col in expected["columns"]:
        assert col in df.columns, f"Missing column: {col}"

    assert df["produto"].nunique() >= expected["total_produtos"]
    assert df["ceasa"].nunique() >= expected["total_ceasas"]
    assert df["preco"].notna().sum() >= expected["non_null_prices_min"]

    if "sample_tomate_ceagesp_sp" in expected:
        s = expected["sample_tomate_ceagesp_sp"]
        row = df[(df["produto"] == s["produto"]) & (df["ceasa"] == s["ceasa"])].iloc[0]
        assert row["ceasa_uf"] == s["ceasa_uf"]
        assert row["preco"] == pytest.approx(s["preco"], rel=1e-2)


def _get_conab_progresso_cases() -> list[tuple[str, Path]]:
    return _discover_cases(source_filter="conab_progresso")


@pytest.mark.skipif(not _get_conab_progresso_cases(), reason="No CONAB Progresso golden data")
@pytest.mark.parametrize("_name,path", _get_conab_progresso_cases())
def test_conab_progresso_golden_parsing(_name: str, path: Path):
    from agrobr.conab.progresso.parser import parse_progresso_xlsx

    expected = _load_expected(path)
    data = (path / "response.xlsx").read_bytes()
    df = parse_progresso_xlsx(data)

    for col in expected["columns"]:
        col = "uf" if col == "estado" else col
        assert col in df.columns, f"Missing column: {col}"

    assert len(df) == expected["total_records"]
    assert sorted(df["cultura"].unique().tolist()) == expected["culturas"]
    assert sorted(df["operacao"].unique().tolist()) == expected["operacoes"]
    assert sorted(df["uf"].unique().tolist()) == expected["estados"]

    if "mt_soja_colheita_pct_atual" in expected:
        row = df[(df["uf"] == "MT") & (df["cultura"] == "Soja") & (df["operacao"] == "Colheita")]
        assert len(row) == 1
        assert row.iloc[0]["pct_semana_atual"] == pytest.approx(
            expected["mt_soja_colheita_pct_atual"], rel=1e-2
        )


def _get_mapbiomas_cases() -> list[tuple[str, Path]]:
    return _discover_cases(source_filter="mapbiomas")


@pytest.mark.skipif(not _get_mapbiomas_cases(), reason="No MapBiomas golden data")
@pytest.mark.parametrize("_name,path", _get_mapbiomas_cases())
def test_mapbiomas_golden_parsing(_name: str, path: Path):
    from agrobr.mapbiomas.parser import parse_cobertura_xlsx, parse_transicao_xlsx

    expected = _load_expected(path)
    data = (path / "response.xlsx").read_bytes()

    exp_cob = expected["cobertura"]
    df_cob = parse_cobertura_xlsx(data, colecao=10)

    for col in exp_cob["columns"]:
        col = "uf" if col == "estado" else col
        assert col in df_cob.columns, f"Cobertura missing column: {col}"
    assert len(df_cob) >= exp_cob["min_records"]
    assert sorted(df_cob["bioma"].unique().tolist()) == exp_cob["biomas_expected"]
    assert sorted(df_cob["uf"].unique().tolist()) == exp_cob["estados_expected"]
    for a in exp_cob["anos_expected"]:
        assert a in df_cob["ano"].values, f"Year {a} not found in cobertura"

    exp_trans = expected["transicao"]
    df_trans = parse_transicao_xlsx(data, colecao=10)

    for col in exp_trans["columns"]:
        col = "uf" if col == "estado" else col
        assert col in df_trans.columns, f"Transicao missing column: {col}"
    assert len(df_trans) >= exp_trans["min_records"]
    assert sorted(df_trans["bioma"].unique().tolist()) == exp_trans["biomas_expected"]
    assert sorted(df_trans["uf"].unique().tolist()) == exp_trans["estados_expected"]
    for p in exp_trans["periodos_expected"]:
        assert p in df_trans["periodo"].values, f"Period {p} not found in transicao"


def _get_funai_cases() -> list[tuple[str, Path]]:
    cases = _discover_cases(source_filter="funai")
    official = GOLDEN_DIR / "funai/official_20260907"
    if (official / "manifest.json").exists():
        cases.append(("funai/official_20260907", official))
    return cases


def _wfs_golden_expected(features: list[dict[str, Any]], source: str) -> pd.DataFrame:
    if source == "funai":
        aliases = {
            "terrai_codigo": "codigo",
            "terrai_nome": "nome",
            "etnia_nome": "etnia",
            "municipio_nome": "municipio",
            "uf_sigla": "uf",
            "superficie_perimetro_ha": "area_ha",
            "fase_ti": "fase",
            "modalidade_ti": "modalidade",
            "data_atualizacao": "data_atualizacao",
            "gid": "gid",
            "reestudo_ti": "reestudo_ti",
            "cr": "cr",
            "faixa_fronteira": "faixa_fronteira",
            "undadm_codigo": "undadm_codigo",
            "undadm_nome": "undadm_nome",
            "undadm_sigla": "undadm_sigla",
            "dominio_uniao": "dominio_uniao",
            "epsg": "epsg",
        }
        columns = list(aliases.values())
        columns.insert(9, "feature_id")
        integers = {"codigo", "gid", "undadm_codigo", "epsg"}
    else:
        aliases = {
            "cd_quilomb": "codigo",
            "no_comunidade": "nome",
            "no_municipio": "municipio",
            "sg_uf": "uf",
            "nu_area_ha": "area_ha",
            "nu_familia": "familias",
            "ds_fase": "fase",
            "st_titulad": "titulado",
            "dt_publica": "data_publicacao",
            "dt_titulo": "data_titulo",
            "co_sr": "regional",
            "nu_processo": "processo",
            "dt_public1": "data_publicacao_2",
            "no_responsavel": "responsavel",
            "no_esfera": "esfera",
            "dt_cadastro": "data_cadastro",
            "cd_sipra": "codigo_sipra",
            "ds_descricao": "descricao",
            "dt_decreto": "data_decreto",
            "tp_levanta": "tipo_levantamento",
            "nr_escalao": "escala",
        }
        columns = list(aliases.values())
        columns.insert(10, "feature_id")
        integers = {"codigo", "familias"}
    records = [
        {
            **{target: feature["properties"][raw] for raw, target in aliases.items()},
            "feature_id": feature["id"],
        }
        for feature in features
    ]
    return pd.DataFrame(
        {
            name: pd.Series(
                [row[name] for row in records],
                dtype="Int64"
                if name in integers
                else "float64"
                if name == "area_ha"
                else pd.Series([""]).dtype,
            )
            for name in columns
        }
    )


@pytest.mark.skipif(not _get_funai_cases(), reason="No FUNAI golden data")
@pytest.mark.parametrize("_name,path", _get_funai_cases())
def test_funai_golden_parsing(_name: str, path: Path):
    from agrobr.funai import parser

    manifest = json.loads((path / "manifest.json").read_bytes())["files"]
    reviewed = 0
    for name, artifact in manifest.items():
        if not name.endswith(".json"):
            continue
        body = (path / name).read_bytes()
        assert len(body) == artifact["size_bytes"]
        assert hashlib.sha256(body).hexdigest() == artifact["sha256"]
        raw = json.loads(body)
        if "features" not in raw:
            continue
        geo = name == "geo.json"
        if name == "geo_default.json":
            with pytest.raises(ParseError, match="CRS"):
                parser.parse_page(body, include_geometry=True)
        page = parser.parse_page(body, include_geometry=geo)
        pd.testing.assert_frame_equal(
            parser.build_frame(page.records),
            _wfs_golden_expected(raw["features"], "funai"),
            check_exact=True,
        )
        if geo:
            assert page.geometries == [row["geometry"] for row in raw["features"]]
        reviewed += 1
    assert reviewed == 8


def _get_icmbio_cases() -> list[tuple[str, Path]]:
    return _discover_cases(source_filter="icmbio")


@pytest.mark.skipif(not _get_icmbio_cases(), reason="No ICMBio golden data")
@pytest.mark.parametrize("_name,path", _get_icmbio_cases())
def test_icmbio_golden_parsing(_name: str, path: Path):
    expected = _load_expected(path)
    metadata = _load_metadata(path)
    fmt = metadata.get("format", "csv")

    if fmt == "csv":
        from agrobr.icmbio.parser import parse_ucs_csv

        data = (path / "response.csv").read_bytes()
        df = parse_ucs_csv(data)
    elif fmt == "geojson":
        pytest.importorskip("geopandas")
        from agrobr.icmbio.parser import parse_ucs_geojson

        data = (path / "response.geojson").read_bytes()
        df = parse_ucs_geojson(data)
        assert df.crs.to_epsg() == expected.get("crs_epsg", 4326)
        assert df.geometry.notna().all()
    else:
        pytest.skip(f"Unknown icmbio format: {fmt}")
        return

    _assert_dataframe_golden(df, expected)


def _get_incra_cases() -> list[tuple[str, Path]]:
    return _discover_cases(source_filter="incra")


def _incra_administrative_expected() -> pd.DataFrame:
    path = GOLDEN_DIR / "incra/andamento_20260608"
    metadata = _load_metadata(path)
    for artifact in metadata["files"]:
        body = (path / artifact["file"]).read_bytes()
        assert len(body) == artifact["bytes"]
        assert hashlib.sha256(body).hexdigest() == artifact["sha256"]
    raw = json.loads((path / "oracle.json").read_bytes())
    columns = [
        "regional",
        "numero_publicado",
        "processo",
        "comunidade",
        "municipio",
        "area_ha_texto",
        "familias_texto",
        "edital_rtid_1",
        "edital_rtid_2",
        "retificacao_edital_1",
        "retificacao_edital_2",
        "portaria",
        "retificacao_portaria",
        "decreto",
        "titulo",
    ]
    records = [
        {
            name: row["numero_publicado"]
            if name == "numero_publicado"
            else row["cells"]["regional_layout"]
            if name == "regional"
            else row["cells"][name if name.endswith("_texto") else name + "_texto"]
            for name in columns
        }
        for row in raw
    ]
    return pd.DataFrame(
        {
            name: pd.Series(
                [row[name] for row in records],
                dtype="Int64" if name == "numero_publicado" else pd.Series([""]).dtype,
            )
            for name in columns
        }
    )


def _incra_national_features() -> list[dict[str, Any]]:
    path = GOLDEN_DIR / "incra/nacional_20260908"
    for artifact in _load_metadata(path)["resources"]:
        body = (path / artifact["file"]).read_bytes()
        assert len(body) == artifact["bytes"]
        assert hashlib.sha256(body).hexdigest() == artifact["sha256"]
    first = json.loads((path / "page_001.json").read_bytes())["features"]
    second = json.loads((path / "page_002.json").read_bytes())["features"]
    assert len(first) == 250 and len(second) == 195
    assert first[-1] == second[0]
    return first + second[1:]


@pytest.mark.skipif(not _get_incra_cases(), reason="No INCRA golden data")
@pytest.mark.parametrize(
    "_name,path",
    [
        pytest.param(name, path, marks=pytest.mark.slow)
        if name == "incra/andamento_20260608"
        else (name, path)
        for name, path in _get_incra_cases()
    ],
)
def test_incra_golden_parsing(_name: str, path: Path):
    from datetime import date

    from agrobr.incra import parser
    from tests.test_incra import replay

    metadata = _load_metadata(path)
    if path.name == "andamento_20260608":
        pytest.importorskip("pdfplumber")
        from agrobr.incra.andamento import parser as administrative

        expected = _incra_administrative_expected()
        parsed = administrative.parse_publication((path / "publication.pdf").read_bytes())
        pd.testing.assert_frame_equal(parsed.frame, expected, check_exact=True)
        assert parsed.declared_total == len(expected) == 613
        assert parsed.edition == date(2026, 6, 8)
        return
    if path.name == "vinculos_20260908":
        from agrobr.incra.vinculos import relation

        expected_body = (path / "expected.json").read_bytes()
        assert hashlib.sha256(expected_body).hexdigest() == metadata["sha256"]
        raw = json.loads(expected_body)
        geographic = _wfs_golden_expected(_incra_national_features(), "incra")
        with pytest.warns(UserWarning, match="viraram NaT"):
            parser.converter_datas(geographic)
        actual = relation.build_relation(
            geographic, _incra_administrative_expected(), max_rows=50_000
        ).frame
        temporais = relation.temporal_columns()
        integers = {
            "perimetro_posicao",
            "perimetro_referencia_posicao",
            "administrativo_referencia_posicao",
            "ocorrencias_perimetro_referencia",
            "ocorrencias_administrativo_referencia",
            "perimetro_codigo",
            "perimetro_familias",
            "administrativo_numero_publicado",
        }
        expected = pd.DataFrame(
            {
                name: pd.Series(
                    [
                        replay.publicado(name.removeprefix("perimetro_"), row[name])
                        if name in temporais
                        else row[name]
                        for row in raw
                    ],
                    dtype="Int64"
                    if name in integers
                    else "float64"
                    if name == "perimetro_area_ha"
                    else "boolean"
                    if name == "referencia_repetida"
                    else temporais.get(name, pd.Series([""]).dtype),
                )
                for name in raw[0]
            }
        )
        assert expected.shape == (781, 46)
        pd.testing.assert_frame_equal(actual, expected, check_exact=True)
        return
    if path.name == "nacional_20260908":
        accepted = _incra_national_features()
        assert len(accepted) == 444
        for artifact in metadata["resources"]:
            body = (path / artifact["file"]).read_bytes()
            raw = json.loads(body)
            page = parser.parse_page(body, include_geometry=False)
            pd.testing.assert_frame_equal(
                parser.build_frame(page.records),
                _wfs_golden_expected(raw["features"], "incra"),
                check_exact=True,
            )
        return
    if path.name == "wfs_v2_20260908":
        reviewed = 0
        for artifact in metadata["files"]:
            body = (path / artifact["file"]).read_bytes()
            assert len(body) == artifact["bytes"]
            assert hashlib.sha256(body).hexdigest() == artifact["sha256"]
            if not artifact["file"].endswith(".json"):
                continue
            raw = json.loads(body)
            geo = artifact["file"] in {"geo.json", "bbox_inside.json", "bbox_empty.json"}
            if artifact["file"] == "geo_default.json":
                with pytest.raises(ParseError, match="CRS"):
                    parser.parse_page(body, include_geometry=True)
            page = parser.parse_page(body, include_geometry=geo)
            pd.testing.assert_frame_equal(
                parser.build_frame(page.records),
                _wfs_golden_expected(raw["features"], "incra"),
                check_exact=True,
            )
            if geo:
                assert page.geometries == [row["geometry"] for row in raw["features"]]
            reviewed += 1
        assert reviewed == 5
        return
    geo = (path / "response.geojson").exists()
    body = (path / ("response.geojson" if geo else "response.csv")).read_bytes()
    if geo:
        raw = json.loads(body)
        assert len(raw["features"]) >= _load_expected(path)["count_min"]
        assert all(len(row["properties"]) == 10 for row in raw["features"])
    with pytest.raises(ParseError):
        parser.parse_page(body, include_geometry=geo)


def _get_mapbiomas_alerta_cases() -> list[tuple[str, Path]]:
    return _discover_cases(source_filter="mapbiomas_alerta")


@pytest.mark.skipif(not _get_mapbiomas_alerta_cases(), reason="No MapBiomas Alerta golden data")
@pytest.mark.parametrize("_name,path", _get_mapbiomas_alerta_cases())
def test_mapbiomas_alerta_golden_parsing(_name: str, path: Path):
    expected = _load_expected(path)
    metadata = _load_metadata(path)
    fmt = metadata.get("format", "graphql")

    records = json.loads((path / "response.json").read_text(encoding="utf-8"))

    if fmt in ("graphql", "graphql_wkt"):
        if "geo" in metadata.get("dataset", ""):
            pytest.importorskip("geopandas")
            from agrobr.mapbiomas_alerta.parser import parse_alertas_geo

            df = parse_alertas_geo(records)
            assert df.crs.to_epsg() == expected.get("crs_epsg", 4326)
        else:
            from agrobr.mapbiomas_alerta.parser import parse_alertas

            df = parse_alertas(records)
    else:
        pytest.skip(f"Unknown mapbiomas_alerta format: {fmt}")
        return

    _assert_dataframe_golden(df, expected)
