from __future__ import annotations

import pytest
from pydantic import ValidationError

from agrobr.incra import acquisition, client, query
from tests.helpers import incra_features, install_incra_wfs


@pytest.mark.parametrize(
    "total,received,validated", [(0, 1, 1), (1, 0, 0), (2, 2, 2), (1, 1, 0), (True, 1, 1)]
)
def test_count_check_rejects_inconsistent_population(total, received, validated):
    with pytest.raises(ValidationError):
        acquisition.IncraCountCheck(
            role="count_before",
            resource_index=0,
            reported_total=total,
            received_rows=received,
            validated_rows=validated,
            source_timestamp=None,
            layout_fingerprint={},
        )


@pytest.mark.parametrize(
    "defect",
    [
        "missing",
        "role",
        "index",
        "total",
        "response_role",
        "incomplete",
        "error",
        "status",
        "page_index",
    ],
)
async def test_acquisition_rejects_contradictory_count_provenance(defect, monkeypatch):
    install_incra_wfs(monkeypatch, incra_features()[:1])
    result = await client.fetch_acquisition(
        query.build_query(include_geometry=False, max_registros=None)
    )
    payload = {name: getattr(result, name) for name in type(result).model_fields}
    payload["resources"] = [resource.model_dump() for resource in result.resources]
    payload["pages"] = [page.model_dump() for page in result.pages]
    payload["count_checks"] = [check.model_dump() for check in result.count_checks]
    if defect == "missing":
        payload["count_checks"].pop()
    elif defect == "role":
        payload["count_checks"][0]["role"] = "count_after"
    elif defect == "index":
        payload["count_checks"][0]["resource_index"] = 99
    elif defect == "total":
        payload["count_checks"][0]["reported_total"] = 2
    elif defect == "response_role":
        payload["resources"][0]["role"] = "page"
    elif defect == "incomplete":
        payload["resources"][0]["complete_body"] = False
    elif defect == "error":
        payload["resources"][0]["error_type"] = "ReadError"
    elif defect == "status":
        payload["resources"][0]["status"] = 403
    else:
        payload["pages"][0]["resource_index"] = 99
    with pytest.raises(ValidationError):
        acquisition.IncraAcquisition.model_validate(payload)
