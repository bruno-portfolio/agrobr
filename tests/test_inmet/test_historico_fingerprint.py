from __future__ import annotations

import zipfile
from pathlib import Path

from agrobr.inmet import parser

GOLDEN = Path(__file__).parents[1] / "golden_data" / "inmet" / "selecao_20260906"


def old_csv() -> bytes:
    with zipfile.ZipFile(GOLDEN / "2000.zip") as zipped:
        return zipped.read(next(name for name in zipped.namelist() if "_A001_" in name))


def test_layout_old_e_moderno_tem_assinaturas_distintas():
    old = parser.historico_layout_fingerprint(old_csv())
    modern = parser.historico_layout_fingerprint((GOLDEN / "2026_A001.csv").read_bytes())
    assert old["sha256"] != modern["sha256"]
    assert len(old["sha256"]) == 64
    assert old["algorithm"] == modern["algorithm"] == "sha256"
    assert old["version"] == modern["version"] == 1
    assert old["parser_version"] == modern["parser_version"] == 2
