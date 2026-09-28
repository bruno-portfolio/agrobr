from __future__ import annotations

import csv
import io
import json
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest
from typer.testing import CliRunner

from agrobr.cli import app


@pytest.mark.parametrize(
    "command, target",
    [
        (["cepea", "indicador", "soja"], "agrobr.cepea.indicador"),
        (["conab", "safras", "soja"], "agrobr.conab.safras"),
        (["conab", "balanco"], "agrobr.conab.balanco"),
        (["ibge", "pam", "soja"], "agrobr.ibge.pam"),
        (["ibge", "lspa", "soja"], "agrobr.ibge.lspa"),
    ],
)
@pytest.mark.parametrize("empty", [False, True])
@pytest.mark.parametrize("formato", ["json", "csv"])
def test_machine_output_round_trip(command, target, empty, formato):
    df = pd.DataFrame({"produto": ["feijão"], "valor": [12.5]})
    if empty:
        df = df.iloc[:0]
    with patch(target, new_callable=AsyncMock, return_value=df):
        result = CliRunner().invoke(app, [*command, "--formato", formato])
    assert result.exit_code == 0, result.output
    assert "Consultando" not in result.stdout
    assert "Consultando" in result.stderr
    if formato == "json":
        assert json.loads(result.stdout) == df.to_dict(orient="records")
    else:
        reader = csv.DictReader(io.StringIO(result.stdout))
        assert reader.fieldnames == ["produto", "valor"]
        assert list(reader) == ([] if empty else [{"produto": "feijão", "valor": "12.5"}])


def test_empty_snapshot_list_json():
    with patch("agrobr.snapshots.list_snapshots", return_value=[]):
        result = CliRunner().invoke(app, ["snapshot", "list", "--json"])
    assert result.exit_code == 0
    assert json.loads(result.stdout) == []
