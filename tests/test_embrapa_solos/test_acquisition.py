from __future__ import annotations

from datetime import datetime

import pytest
from pydantic import ValidationError

from agrobr.embrapa_solos import acquisition
from agrobr.exceptions import ParseError


@pytest.mark.parametrize(
    "body",
    [
        b'{"type":"FeatureCollection","features":[{}],"numberMatched":1,"numberReturned":1}',
        b'{"type":"FeatureCollection","features":[],"numberMatched":1,"numberReturned":0,"totalFeatures":2}',
        b'{"type":"FeatureCollection","features":[],"numberMatched":1,"numberMatched":1,"numberReturned":0}',
        b"<html>maintenance</html>",
        b"<ExceptionReport/>",
        b'<FeatureCollection numberMatched="0" numberReturned="0"/>',
        b'<wfs:FeatureCollection xmlns:wfs="http://www.opengis.net/wfs/2.0" numberMatched="unknown" numberReturned="0"/>',
        b'<wfs:FeatureCollection xmlns:wfs="http://www.opengis.net/wfs/2.0" numberMatched="1" numberReturned="0"><wfs:member/></wfs:FeatureCollection>',
    ],
)
def test_hits_invalid_envelope(body):
    with pytest.raises(ParseError):
        acquisition.parse_hits(body)


@pytest.mark.parametrize("encoding", ["utf-8", "utf-16"])
def test_hits_dtd_rejected(encoding):
    body = (
        '<!DOCTYPE root [<!ENTITY n "1">]><wfs:FeatureCollection '
        'xmlns:wfs="http://www.opengis.net/wfs/2.0" numberMatched="&n;" numberReturned="0"/>'
    ).encode(encoding)
    with pytest.raises(ParseError):
        acquisition.parse_hits(body)


def test_resource_requires_aware_timestamp():
    with pytest.raises(ValidationError):
        acquisition.SolosResource(
            role="page",
            logical_index=0,
            attempt_index=0,
            requested_url="https://example.test",
            url="https://example.test",
            parameters={},
            status=200,
            fetched_at=datetime(2026, 9, 7),
        )
