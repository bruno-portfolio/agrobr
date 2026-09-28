from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from typing import Any

import pytest

from agrobr import datasets
from tests import helpers
from tests.integration import test_datasets_live

pytestmark = [pytest.mark.integration, pytest.mark.timeout(300)]

COLLECTION_DATE = datetime.now(UTC).date()
ISO_WEEK = COLLECTION_DATE.isocalendar().week
PRODUCT_CASES = [
    ("cadastro_rural", product) for product in datasets.list_products("cadastro_rural")
]
RecordProperty = Callable[[str, Any], None]
LivePolicy = Callable[[str, dict[str, Any]], None]


def _previous_month_start(reference: date) -> str:
    return (reference.replace(day=1) - timedelta(days=1)).replace(day=1).isoformat()


CREATED_AFTER = _previous_month_start(COLLECTION_DATE)


@pytest.mark.parametrize(
    "dataset_name,produto",
    PRODUCT_CASES,
    ids=[f"{name}-{product}" for name, product in PRODUCT_CASES],
)
async def test_produto_live(
    dataset_name: str,
    produto: str,
    record_property: RecordProperty,
    apply_live_policy: LivePolicy,
):
    record_property("dataset", dataset_name)
    record_property("produto", produto)
    record_property("iso_week", ISO_WEEK)
    record_property("collection_date", COLLECTION_DATE.isoformat())
    record_property("criado_apos", CREATED_AFTER)
    record_property("sampled_products", ",".join(product for _, product in PRODUCT_CASES))
    original_args, original_kwargs = test_datasets_live.LIVE_CASES[dataset_name]
    args = (produto, *original_args[1:])
    kwargs = dict(original_kwargs)
    kwargs["criado_apos"] = CREATED_AFTER
    apply_live_policy(dataset_name, kwargs)
    await helpers.assert_live_product_result(
        dataset_name, produto, args, kwargs, 0.5, record_property
    )
