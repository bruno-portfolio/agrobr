from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from agrobr.comtrade.models import COLUNAS_MIRROR, COLUNAS_SAIDA
from agrobr.comtrade.parser import PARSER_VERSION, parse_mirror, parse_trade_data

GOLDEN_DIR = Path(__file__).parent.parent / "golden_data" / "comtrade"


class TestParseTradeData:
    def _load_golden_records(self) -> list[dict]:
        raw = json.loads((GOLDEN_DIR / "comercio_sample" / "response.json").read_text())
        return raw["data"]

    def test_golden_data_columns(self):
        records = self._load_golden_records()
        df = parse_trade_data(records)
        assert list(df.columns) == COLUNAS_SAIDA

    def test_golden_data_records_count(self):
        records = self._load_golden_records()
        df = parse_trade_data(records)
        assert len(df) == 8

    def test_golden_data_types(self):
        records = self._load_golden_records()
        df = parse_trade_data(records)
        assert pd.api.types.is_string_dtype(df["periodo"])
        assert df["ano"].dtype == int
        assert pd.api.types.is_float_dtype(df["peso_liquido_kg"])
        assert pd.api.types.is_float_dtype(df["valor_fob_usd"])
        assert pd.api.types.is_float_dtype(df["volume_ton"])

    def test_volume_ton_calculated(self):
        records = self._load_golden_records()
        df = parse_trade_data(records)
        row = df[df["hs_code"] == "1201"].iloc[0]
        assert row["volume_ton"] == pytest.approx(row["peso_liquido_kg"] / 1000.0)

    def test_ano_extracted(self):
        records = self._load_golden_records()
        df = parse_trade_data(records)
        assert (df["ano"] == 2024).all()

    def test_mes_null_for_annual(self):
        records = self._load_golden_records()
        df = parse_trade_data(records)
        assert df["mes"].isna().all()

    def test_sorted_by_hs_code(self):
        records = self._load_golden_records()
        df = parse_trade_data(records)
        assert list(df["hs_code"]) == sorted(df["hs_code"].tolist())

    def test_empty_records(self):
        df = parse_trade_data([])
        assert len(df) == 0
        assert list(df.columns) == COLUNAS_SAIDA

    def test_monthly_period_extracts_mes(self):
        records = [
            {
                "period": "202403",
                "reporterCode": 76,
                "reporterISO": "BRA",
                "reporterDesc": "Brazil",
                "partnerCode": 156,
                "partnerISO": "CHN",
                "partnerDesc": "China",
                "flowCode": "X",
                "flowDesc": "Export",
                "cmdCode": "1201",
                "cmdDesc": "Soybeans",
                "aggrLevel": 4,
                "netWgt": 5000000000.0,
                "grossWgt": 5100000000.0,
                "fobvalue": 2200000000.0,
                "cifvalue": None,
                "primaryValue": 2200000000.0,
                "qty": 5000000.0,
                "qtyUnitAbbr": "kg",
            }
        ]
        df = parse_trade_data(records)
        assert df.iloc[0]["ano"] == 2024
        assert df.iloc[0]["mes"] == 3


class TestParseMirror:
    def _load_golden_mirror(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        raw_r = json.loads((GOLDEN_DIR / "mirror_sample" / "response_reporter.json").read_text())
        raw_p = json.loads((GOLDEN_DIR / "mirror_sample" / "response_partner.json").read_text())
        df_r = parse_trade_data(raw_r["data"])
        df_p = parse_trade_data(raw_p["data"])
        return df_r, df_p

    def test_golden_mirror_columns(self):
        df_r, df_p = self._load_golden_mirror()
        df = parse_mirror(df_r, df_p, "BRA", "CHN")
        assert list(df.columns) == COLUNAS_MIRROR

    def test_golden_mirror_records(self):
        df_r, df_p = self._load_golden_mirror()
        df = parse_mirror(df_r, df_p, "BRA", "CHN")
        assert len(df) == 4

    def test_golden_mirror_discrepancies(self):
        df_r, df_p = self._load_golden_mirror()
        df = parse_mirror(df_r, df_p, "BRA", "CHN")
        row = df.iloc[0]
        assert row["diff_peso_kg"] == pytest.approx(-2089669638.0, rel=1e-3)
        assert pd.isna(row["diff_valor_fob_usd"])

    def test_golden_mirror_ratio_valor_range(self):
        df_r, df_p = self._load_golden_mirror()
        df = parse_mirror(df_r, df_p, "BRA", "CHN")
        ratio = df.iloc[0]["ratio_valor"]
        assert 0.80 < ratio < 1.0 or pd.isna(ratio)

    def test_golden_mirror_ratio_peso_approx_1(self):
        df_r, df_p = self._load_golden_mirror()
        df = parse_mirror(df_r, df_p, "BRA", "CHN")
        ratio = df.iloc[0]["ratio_peso"]
        assert 0.90 < ratio < 1.10

    def test_golden_mirror_iso_codes(self):
        df_r, df_p = self._load_golden_mirror()
        df = parse_mirror(df_r, df_p, "BRA", "CHN")
        assert df.iloc[0]["reporter_iso"] == "BRA"
        assert df.iloc[0]["partner_iso"] == "CHN"

    def test_empty_mirror(self):
        empty = pd.DataFrame(columns=COLUNAS_SAIDA)
        df = parse_mirror(empty, empty, "BRA", "CHN")
        assert len(df) == 0
        assert list(df.columns) == COLUNAS_MIRROR


class TestParserVersion:
    def test_version_is_1(self):
        assert PARSER_VERSION == 1
