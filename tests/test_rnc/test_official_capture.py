from __future__ import annotations

import hashlib
import json
from pathlib import Path

GOLDEN_DIR = Path(__file__).resolve().parent.parent / "golden_data" / "rnc" / "snpc_20260906"


def test_official_fixture_hashes_and_token_redaction():
    provenance = json.loads((GOLDEN_DIR / "PROVENANCE.json").read_text(encoding="utf-8"))
    for item in provenance:
        raw = (GOLDEN_DIR / item["file"]).read_bytes()
        assert hashlib.sha256(raw).hexdigest() == item["sha256"]
        assert item["capture"]["url"].startswith("https://sistemas.agricultura.gov.br/snpc/")
        if item["file"].endswith("_form.html"):
            assert b"REDACTED_CSRF_TOKEN" in raw
