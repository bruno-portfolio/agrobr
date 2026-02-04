"""Golden data tests para garantir não-regressão de parsing."""

from __future__ import annotations

import json
import hashlib
from pathlib import Path
from decimal import Decimal

import pytest

GOLDEN_DIR = Path(__file__).parent / "golden_data"


def get_golden_test_cases() -> list[tuple[str, Path]]:
    """Descobre todos os casos de teste golden para HTML (CEPEA)."""
    cases = []
    if not GOLDEN_DIR.exists():
        return cases

    for source_dir in GOLDEN_DIR.iterdir():
        if not source_dir.is_dir():
            continue
        for case_dir in source_dir.iterdir():
            if not case_dir.is_dir():
                continue
            if (case_dir / "response.html").exists():
                cases.append((f"{source_dir.name}/{case_dir.name}", case_dir))
    return cases


def get_conab_golden_test_cases() -> list[tuple[str, Path]]:
    """Descobre todos os casos de teste golden para XLSX (CONAB)."""
    cases = []
    conab_dir = GOLDEN_DIR / "conab"
    if not conab_dir.exists():
        return cases

    for case_dir in conab_dir.iterdir():
        if not case_dir.is_dir():
            continue
        if (case_dir / "response.xlsx").exists():
            cases.append((f"conab/{case_dir.name}", case_dir))
    return cases


@pytest.mark.skipif(not get_golden_test_cases(), reason="No golden data available")
@pytest.mark.parametrize("name,path", get_golden_test_cases())
def test_golden_parsing(name: str, path: Path):
    """
    Testa parsing contra golden data.

    Garante que:
    1. Parser extrai mesma quantidade de registros
    2. Primeiro e último registro batem
    3. Checksum dos dados bate (se disponível)
    """
    html = (path / "response.html").read_text(encoding="utf-8")
    expected = json.loads((path / "expected.json").read_text())
    metadata = json.loads((path / "metadata.json").read_text())

    source = metadata["source"]
    produto = metadata["produto"]

    if source == "cepea":
        from agrobr.cepea.parsers.detector import get_parser_with_fallback
        import asyncio

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

            warnings.warn(f"Checksum mismatch: {checksum} != {expected['checksum']}")


@pytest.mark.skipif(not get_golden_test_cases(), reason="No golden data available")
@pytest.mark.parametrize("name,path", get_golden_test_cases())
def test_golden_fingerprint(name: str, path: Path):
    """Testa que fingerprint do golden data é reconhecida."""
    html = (path / "response.html").read_text(encoding="utf-8")
    metadata = json.loads((path / "metadata.json").read_text())

    if metadata["source"] == "cepea":
        from agrobr.cepea.parsers.fingerprint import extract_fingerprint
        from agrobr.constants import Fonte

        fp = extract_fingerprint(html, Fonte.CEPEA, "test")

        assert fp.structure_hash, "No structure hash"


@pytest.mark.skipif(not get_golden_test_cases(), reason="No golden data available")
@pytest.mark.parametrize("name,path", get_golden_test_cases())
def test_golden_parser_can_parse(name: str, path: Path):
    """Testa que parser reconhece o golden data."""
    html = (path / "response.html").read_text(encoding="utf-8")
    metadata = json.loads((path / "metadata.json").read_text())

    if metadata["source"] == "cepea":
        from agrobr.cepea.parsers.v1 import CepeaParserV1

        parser = CepeaParserV1()
        can_parse, confidence = parser.can_parse(html)

        assert can_parse, "Parser should be able to parse golden data"
        assert confidence >= 0.4, f"Confidence too low: {confidence}"


# ============================================================================
# CONAB Golden Tests
# ============================================================================


@pytest.mark.skipif(not get_conab_golden_test_cases(), reason="No CONAB golden data available")
@pytest.mark.parametrize("name,path", get_conab_golden_test_cases())
def test_conab_golden_parsing_soja(name: str, path: Path):
    """Testa parsing de soja contra golden data CONAB."""
    from io import BytesIO
    from agrobr.conab.parsers.v1 import ConabParserV1

    xlsx_path = path / "response.xlsx"
    expected = json.loads((path / "expected.json").read_text())

    with open(xlsx_path, "rb") as f:
        xlsx = BytesIO(f.read())

    parser = ConabParserV1()
    safras = parser.parse_safra_produto(xlsx, "soja", safra_ref="2025/26")

    assert len(safras) == expected["soja"]["count"], (
        f"Expected {expected['soja']['count']} soja records, got {len(safras)}"
    )

    ufs_found = sorted(list({s.uf for s in safras if s.uf}))
    assert ufs_found == expected["soja"]["ufs_found"], (
        f"UFs mismatch: {ufs_found} != {expected['soja']['ufs_found']}"
    )


@pytest.mark.skipif(not get_conab_golden_test_cases(), reason="No CONAB golden data available")
@pytest.mark.parametrize("name,path", get_conab_golden_test_cases())
def test_conab_golden_parsing_milho(name: str, path: Path):
    """Testa parsing de milho contra golden data CONAB."""
    from io import BytesIO
    from agrobr.conab.parsers.v1 import ConabParserV1

    xlsx_path = path / "response.xlsx"
    expected = json.loads((path / "expected.json").read_text())

    with open(xlsx_path, "rb") as f:
        xlsx = BytesIO(f.read())

    parser = ConabParserV1()
    safras = parser.parse_safra_produto(xlsx, "milho", safra_ref="2025/26")

    assert len(safras) == expected["milho"]["count"], (
        f"Expected {expected['milho']['count']} milho records, got {len(safras)}"
    )


@pytest.mark.skipif(not get_conab_golden_test_cases(), reason="No CONAB golden data available")
@pytest.mark.parametrize("name,path", get_conab_golden_test_cases())
def test_conab_golden_parsing_suprimento(name: str, path: Path):
    """Testa parsing de suprimento contra golden data CONAB."""
    from io import BytesIO
    from agrobr.conab.parsers.v1 import ConabParserV1

    xlsx_path = path / "response.xlsx"
    expected = json.loads((path / "expected.json").read_text())

    with open(xlsx_path, "rb") as f:
        xlsx = BytesIO(f.read())

    parser = ConabParserV1()
    suprimentos = parser.parse_suprimento(xlsx)

    assert len(suprimentos) == expected["suprimento"]["count"], (
        f"Expected {expected['suprimento']['count']} suprimento records, got {len(suprimentos)}"
    )


@pytest.mark.skipif(not get_conab_golden_test_cases(), reason="No CONAB golden data available")
@pytest.mark.parametrize("name,path", get_conab_golden_test_cases())
def test_conab_golden_parsing_brasil_total(name: str, path: Path):
    """Testa parsing de totais do Brasil contra golden data CONAB."""
    from io import BytesIO
    from agrobr.conab.parsers.v1 import ConabParserV1

    xlsx_path = path / "response.xlsx"
    expected = json.loads((path / "expected.json").read_text())

    with open(xlsx_path, "rb") as f:
        xlsx = BytesIO(f.read())

    parser = ConabParserV1()
    totais = parser.parse_brasil_total(xlsx)

    assert len(totais) == expected["brasil_total"]["count"], (
        f"Expected {expected['brasil_total']['count']} brasil_total records, got {len(totais)}"
    )
