from __future__ import annotations

import pytest
from pydantic import ValidationError

from agrobr.incra import models
from tests.helpers import incra_features


@pytest.mark.parametrize("value", [None, "", "2026-09-07", "2026-09-07T00:00:00Z "])
def test_registration_requires_nonnull_valid_datetime(value):
    raw = incra_features()[0]["properties"]
    raw["dt_cadastro"] = value
    with pytest.raises(ValidationError, match="dt_cadastro"):
        models.Properties.model_validate(raw)


@pytest.mark.parametrize("field", ["dt_publica", "dt_public1", "dt_titulo", "dt_decreto"])
def test_invalid_civil_date_is_not_coerced_to_null(field):
    raw = incra_features()[0]["properties"]
    raw[field] = "2023-02-29"
    with pytest.raises(ValidationError, match=field):
        models.Properties.model_validate(raw)


@pytest.mark.parametrize("field", ["cd_quilomb", "nu_familia"])
@pytest.mark.parametrize("value", [True, "1", 1.5, 2**31, -(2**31) - 1])
def test_integer_properties_reject_type_fraction_or_out_of_range(field, value):
    raw = incra_features()[0]["properties"]
    raw[field] = value
    with pytest.raises(ValidationError, match=field):
        models.Properties.model_validate(raw)


@pytest.mark.parametrize("identifier", [None, "", " ", 113])
def test_feature_id_must_be_nonblank_literal_string(identifier):
    feature = incra_features()[0]
    feature["id"] = identifier
    with pytest.raises(ValidationError, match="id"):
        models.Feature.model_validate(feature)


def test_feature_geometry_name_must_be_layer_column():
    feature = incra_features(include_geometry=True)[0]
    feature["geometry_name"] = "the_geom"
    with pytest.raises(ValidationError, match="geometry_name"):
        models.Feature.model_validate(feature)
