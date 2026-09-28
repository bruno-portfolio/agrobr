from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import duckdb
import pytest
from bs4 import BeautifulSoup

from agrobr.cache import migrations
from tests.helpers import insert_cache_indicator, seed_cache_schema

ORIGINALS = "* EXCLUDE (quarantine_migration, quarantined_at, quarantine_reason)"
PAGES = Path(__file__).resolve().parents[1] / "golden_data" / "cepea" / "pages_20260905"


@pytest.mark.parametrize(
    "produto,legacy_unit,unit,header,value",
    [
        ("trigo", "BRL/sc60kg", "BRL/ton", "Valor R$/t*", "1.457,55"),
        ("algodao", "BRL/@", "cBRL/lb", "Centavos R$/lp", "439,98"),
    ],
)
def test_migration_9_unit_labels_match_official_cells_without_rescaling(
    produto, legacy_unit, unit, header, value
):
    html = (PAGES / f"{produto}.html").read_text(encoding="utf-8")
    table = BeautifulSoup(html, "lxml").select_one("#imagenet-indicador1")
    assert table is not None
    rows = table.select("tr")
    assert rows[0].select("th")[1].get_text(strip=True) == header
    assert [cell.get_text(strip=True) for cell in rows[1].select("td")][:2] == [
        "04/09/2026",
        value,
    ]
    original_value = Decimal(value.replace(".", "").replace(",", "."))
    with duckdb.connect(":memory:") as conn:
        seed_cache_schema(conn, 8)
        insert_cache_indicator(
            conn,
            produto=produto,
            fonte="cepea",
            data=date(2026, 9, 4),
            valor=original_value,
            unidade=legacy_unit,
            parser_version=1,
        )
        original = conn.execute("SELECT * FROM indicadores").fetchall()
        migrations.migrate(conn)
        assert conn.execute(
            "SELECT valor, unidade, parser_version FROM indicadores"
        ).fetchall() == [(original_value, unit, 1)]
        assert (
            conn.execute(f"SELECT {ORIGINALS} FROM indicadores_quarentena").fetchall() == original
        )
