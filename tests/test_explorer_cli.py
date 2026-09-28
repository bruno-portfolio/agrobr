from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from scripts import update_conab_explorer_data as conab_explorer
from scripts import update_explorer_data as explorer


@pytest.mark.parametrize("module", [explorer, conab_explorer])
def test_cli_help_from_an_unrelated_directory(module, tmp_path):
    result = subprocess.run(
        [sys.executable, "-X", "utf8", str(Path(module.__file__).resolve()), "--help"],
        cwd=tmp_path,
        env={**os.environ, "PYTHONPATH": str(Path(explorer.__file__).resolve().parent.parent)},
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert "--input" in result.stdout
    assert "--output" in result.stdout
    assert "--cache-dir" in result.stdout


@pytest.mark.parametrize("module", [explorer, conab_explorer])
async def test_missing_input_fails_before_fetching_or_creating_cache(module, tmp_path, monkeypatch):
    cache = tmp_path / "cache"
    monkeypatch.setattr(
        sys,
        "argv",
        ["update", "--input", str(tmp_path / "missing.html"), "--cache-dir", str(cache)],
    )
    with pytest.raises(FileNotFoundError):
        await module.main()
    assert not cache.exists()


@pytest.mark.parametrize("check", [False, True])
async def test_explicit_html_and_output_paths(check, tmp_path, monkeypatch):
    source, output, cache = (tmp_path / name for name in ("source.html", "output.html", "cache"))
    original = {"version": 1}
    html = '<main>Preservado</main><script type="application/json" id="agroExplorerData">'
    html += json.dumps(original) + "</script>"
    source.write_text(html, encoding="utf-8")
    argv = ["update", "--input", str(source), "--output", str(output), "--cache-dir", str(cache)]
    monkeypatch.setattr(sys, "argv", argv + (["--check"] if check else []))
    fetch = AsyncMock(return_value={})
    monkeypatch.setattr(explorer, "collect_all", fetch)
    monkeypatch.setattr(explorer, "build_data", lambda *_args: {"version": 2, "text": "</script>"})
    monkeypatch.setattr(explorer, "comparison_report", lambda *_args: {"differences": []})
    await explorer.main()
    assert source.read_text(encoding="utf-8") == html
    assert (cache / "verified-data.json").exists()
    fetch.assert_awaited_once_with(original, cache / "calls", False)
    if check:
        assert not output.exists()
    else:
        result, data = explorer.read_explorer_input(output)
        assert result.startswith("<main>Preservado</main>")
        assert data == {"version": 2, "text": "</script>"}


@pytest.mark.parametrize("check", [False, True])
async def test_json_output_from_monthly_conab_job(check, tmp_path, monkeypatch):
    source = tmp_path / "conab.json"
    original = {"version": 4, "states": {}, "geometry": {}}
    source.write_text(json.dumps(original), encoding="utf-8")
    arguments = ["update", "--input", str(source), "--cache-dir", str(tmp_path / "cache")]
    monkeypatch.setattr(sys, "argv", arguments + (["--check"] if check else []))
    history, current = AsyncMock(return_value={}), AsyncMock(return_value={})
    monkeypatch.setattr(conab_explorer, "collect_history", history)
    monkeypatch.setattr(conab_explorer, "collect_current", current)
    year = str(conab_explorer.datetime.now(conab_explorer.UTC).year)
    generated = {
        **original,
        "audit": {"verified_at": "2026-09-09T00:00:00Z"},
        "production": {"history": {}, "current": {year: {"soja": {"MT": [100, 20, 5000]}}}},
    }
    monkeypatch.setattr(conab_explorer, "build_data", lambda *_args, **_kwargs: generated)
    await conab_explorer.main()
    assert json.loads(source.read_text(encoding="utf-8")) == (original if check else generated)
    assert history.await_count == 5
    assert current.await_count == 4
    assert history.call_args.kwargs["year"] == int(year)


def test_json_input_requires_object(tmp_path):
    path = tmp_path / "invalid.json"
    path.write_text("[]", encoding="utf-8")
    with pytest.raises(ValueError, match="objeto JSON"):
        explorer.read_explorer_input(path)
