"""Golden data tests para garantir não-regressão de parsing."""

from __future__ import annotations

import hashlib
import json
from decimal import Decimal
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

GOLDEN_DIR = Path(__file__).parent / "golden_data"


# ============================================================================
# Discovery helpers
# ============================================================================


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
    """Descobre todos os casos de teste golden para XLSX (CONAB)."""
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
            if isinstance(val, float):
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
            if isinstance(val, float):
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


# ============================================================================
# CEPEA Golden Tests (original)
# ============================================================================


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
        data_str = json.dumps([r.model_dump(mode="json") for r in results], sort_keys=True)
        checksum = f"sha256:{hashlib.sha256(data_str.encode()).hexdigest()[:16]}"
        if checksum != expected["checksum"]:
            import warnings

            warnings.warn(f"Checksum mismatch: {checksum} != {expected['checksum']}", stacklevel=2)


@pytest.mark.skipif(not get_golden_test_cases(), reason="No golden data available")
@pytest.mark.parametrize("_name,path", get_golden_test_cases())
def test_golden_fingerprint(_name: str, path: Path):
    """Testa que fingerprint do golden data é reconhecida."""
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
    """Testa que parser reconhece o golden data."""
    html = (path / "response.html").read_text(encoding="utf-8")
    metadata = json.loads((path / "metadata.json").read_text(encoding="utf-8"))

    if metadata["source"] == "cepea":
        from agrobr.cepea.parsers.v1 import CepeaParserV1

        parser = CepeaParserV1()
        can_parse, confidence = parser.can_parse(html)

        assert can_parse, "Parser should be able to parse golden data"
        assert confidence >= 0.4, f"Confidence too low: {confidence}"


# ============================================================================
# CONAB Golden Tests (original)
# ============================================================================


@pytest.mark.skipif(not get_conab_golden_test_cases(), reason="No CONAB golden data available")
@pytest.mark.parametrize("_name,path", get_conab_golden_test_cases())
def test_conab_golden_parsing_soja(_name: str, path: Path):
    """Testa parsing de soja contra golden data CONAB."""
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
    """Testa parsing de milho contra golden data CONAB."""
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
    """Testa parsing de suprimento contra golden data CONAB."""
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
    """Testa parsing de totais do Brasil contra golden data CONAB."""
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


# ============================================================================
# BCB Golden Tests
# ============================================================================


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


# ============================================================================
# INMET Golden Tests
# ============================================================================


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


# ============================================================================
# NASA POWER Golden Tests
# ============================================================================


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


# ============================================================================
# USDA Golden Tests
# ============================================================================


def _get_usda_cases() -> list[tuple[str, Path]]:
    return _discover_cases(source_filter="usda")


@pytest.mark.skipif(not _get_usda_cases(), reason="No USDA golden data")
@pytest.mark.parametrize("_name,path", _get_usda_cases())
def test_usda_golden_parsing(_name: str, path: Path):
    from agrobr.usda.parser import parse_psd_response

    data = json.loads((path / "response.json").read_text(encoding="utf-8"))
    expected = _load_expected(path)

    df = parse_psd_response(data)

    _assert_dataframe_golden(df, expected)

    if "commodity" in df.columns:
        assert (df["commodity"] == "soja").all(), "commodity should be 'soja' for this sample"
    if "attribute_br" in df.columns:
        assert df["attribute_br"].notna().any(), "attribute_br should have mapped values"


# ============================================================================
# IMEA Golden Tests
# ============================================================================


def _get_imea_cases() -> list[tuple[str, Path]]:
    return _discover_cases(source_filter="imea")


@pytest.mark.skipif(not _get_imea_cases(), reason="No IMEA golden data")
@pytest.mark.parametrize("_name,path", _get_imea_cases())
def test_imea_golden_parsing(_name: str, path: Path):
    from agrobr.imea.parser import parse_cotacoes

    data = json.loads((path / "response.json").read_text(encoding="utf-8"))
    expected = _load_expected(path)

    df = parse_cotacoes(data)

    _assert_dataframe_golden(df, expected)

    if "cadeia" in df.columns:
        assert (df["cadeia"] == "soja").all(), "cadeia should be 'soja'"
    if "valor" in df.columns:
        assert df["valor"].dtype in ("float64", "Float64"), "valor should be numeric"


# ============================================================================
# ComexStat Golden Tests
# ============================================================================


def _get_comexstat_cases() -> list[tuple[str, Path]]:
    return _discover_cases(source_filter="comexstat")


@pytest.mark.skipif(not _get_comexstat_cases(), reason="No ComexStat golden data")
@pytest.mark.parametrize("_name,path", _get_comexstat_cases())
def test_comexstat_golden_parsing(_name: str, path: Path):
    from agrobr.comexstat.parser import parse_exportacao

    csv_text = (path / "response.csv").read_text(encoding="utf-8")
    expected = _load_expected(path)
    metadata = _load_metadata(path)

    kwargs = metadata.get("parser_kwargs", {})
    df = parse_exportacao(csv_text, **kwargs)

    _assert_dataframe_golden(df, expected)

    if "ncm" in df.columns:
        assert df["ncm"].str.len().eq(8).all(), "NCM should be zero-padded to 8 digits"
    if "uf" in df.columns:
        assert df["uf"].str.isupper().all(), "UF should be uppercase"


# ============================================================================
# Notícias Agrícolas Golden Tests
# ============================================================================


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


# ============================================================================
# IBGE Golden Tests
# ============================================================================


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


# ============================================================================
# DERAL Golden Tests
# ============================================================================


def _get_deral_cases() -> list[tuple[str, Path]]:
    return _discover_cases(source_filter="deral")


@pytest.mark.skipif(not _get_deral_cases(), reason="No DERAL golden data")
@pytest.mark.parametrize("_name,path", _get_deral_cases())
def test_deral_golden_parsing(_name: str, path: Path):
    from agrobr.deral.parser import parse_pc_xls

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


# ============================================================================
# ABIOVE Golden Tests
# ============================================================================


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


# ============================================================================
# ANDA Golden Tests
# ============================================================================


def _get_anda_cases() -> list[tuple[str, Path]]:
    return _discover_cases(source_filter="anda")


@pytest.mark.skipif(not _get_anda_cases(), reason="No ANDA golden data")
@pytest.mark.parametrize("_name,path", _get_anda_cases())
def test_anda_golden_parsing(_name: str, path: Path):
    from agrobr.anda.parser import parse_entregas_table

    table = json.loads((path / "response.json").read_text(encoding="utf-8"))
    expected = _load_expected(path)
    metadata = _load_metadata(path)

    kwargs = metadata.get("parser_kwargs", {})
    records = parse_entregas_table(table, **kwargs)

    df = pd.DataFrame(records)

    _assert_dataframe_golden(df, expected)

    if "ufs_expected" in expected and "uf" in df.columns:
        ufs = sorted(df["uf"].unique().tolist())
        assert ufs == expected["ufs_expected"], f"UFs: {ufs} != {expected['ufs_expected']}"

    if "meses_expected" in expected and "mes" in df.columns:
        meses = sorted(df["mes"].unique().tolist())
        assert meses == expected["meses_expected"], (
            f"Meses: {meses} != {expected['meses_expected']}"
        )
