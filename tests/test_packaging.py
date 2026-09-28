from __future__ import annotations

import fnmatch
import tomllib
from pathlib import Path

import pytest


@pytest.mark.parametrize("target", ["wheel", "sdist"])
@pytest.mark.parametrize(
    "path", ["agrobr/.claude/settings.local.json", "agrobr/.env", "agrobr/.env.local"]
)
def test_local_configuration_excluded_from_artifacts(target, path):
    config = tomllib.loads(
        (Path(__file__).parents[1] / "pyproject.toml").read_text(encoding="utf-8")
    )
    patterns = config["tool"]["hatch"]["build"]["targets"][target]["exclude"]
    assert any(fnmatch.fnmatchcase(path, pattern) for pattern in patterns)
