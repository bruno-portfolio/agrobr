from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pydantic
import pytest

from agrobr import constants
from agrobr.rnc import acquisition
from tests.helpers import levanta_exatamente, rnc_csv_acquisition


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("method", "GET"),
        ("status_code", 500),
        ("size_bytes", True),
        ("sha256", "not-a-digest"),
        ("received_at", datetime(2026, 9, 7)),
        ("headers", {"set-cookie": "secret"}),
    ],
)
def test_resource_rejects_incompatible_external_metadata(field, value):
    raw = rnc_csv_acquisition(b"csv", "registradas").resource.model_dump()
    raw[field] = value
    with pytest.raises(pydantic.ValidationError):
        acquisition.HTTPResource.model_validate(raw)


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [
        ("url", "https://outside.example/cultivares_registradas.php", "família"),
        ("url", constants.RNC_PUBLIC_URLS["protegidas"], "família"),
        ("url", constants.RNC_PUBLIC_URLS["registradas"] + "#fragment", "família"),
        ("size_bytes", 2, "Tamanho"),
        ("sha256", "0" * 64, "Hash"),
        ("url", constants.RNC_PUBLIC_URLS["registradas"].replace("https:", "http:"), "família"),
        (
            "url",
            constants.RNC_PUBLIC_URLS["registradas"].replace(".gov.br/", ".gov.br:8443/"),
            "família",
        ),
        ("url", constants.RNC_PUBLIC_URLS["registradas"].replace("://", "://user@"), "família"),
        ("requested_url", "https://[invalid]/cultivares_registradas.php", "inválida"),
        (
            "url",
            constants.RNC_PUBLIC_URLS["registradas"].replace(
                "sistemas.agricultura.gov.br", "outside.example"
            ),
            "família",
        ),
    ],
)
def test_acquisition_rejects_wrong_family_or_content(field, value, reason):
    raw = rnc_csv_acquisition(b"csv", "registradas").model_dump()
    raw["content"] = b"csv"
    raw["resource"][field] = value
    with levanta_exatamente(pydantic.ValidationError, match=reason):
        acquisition.CSVAcquisition.model_validate(raw)


def test_acquisition_rejects_search_after_export():
    raw = rnc_csv_acquisition(b"csv", "protegidas").model_dump()
    raw["content"] = b"csv"
    raw["search"]["received_at"] = raw["resource"]["received_at"] + timedelta(seconds=1)
    with pytest.raises(pydantic.ValidationError, match="posterior"):
        acquisition.CSVAcquisition.model_validate(raw)


def test_provenance_excludes_body_and_returns_independent_dictionaries():
    captured = rnc_csv_acquisition(b"unexported-body", "protegidas", received_at=datetime.now(UTC))
    provenance = captured.provenance()
    assert "content" not in provenance and "unexported-body" not in str(provenance)
    provenance["resource"]["headers"]["modified"] = "outside"
    assert "modified" not in captured.resource.headers
