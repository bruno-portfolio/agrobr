from __future__ import annotations

import shutil
from pathlib import Path

import pytest
from bs4 import BeautifulSoup

from tests import test_golden


def test_middle_value_mutation_fails_golden(tmp_path):
    source = Path(__file__).parent / "golden_data/cepea/soja_sample"
    fixture = tmp_path / "soja_sample"
    shutil.copytree(source, fixture)
    path = fixture / "response.html"
    soup = BeautifulSoup(path.read_text(encoding="utf-8"), "lxml")
    rows = soup.select("tbody tr")
    rows[len(rows) // 2].select("td")[1].string = "147,11"
    path.write_text(str(soup), encoding="utf-8")
    with pytest.raises(AssertionError, match="Checksum mismatch"):
        test_golden.test_golden_parsing("mutated", fixture)
