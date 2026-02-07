"""Testes para os modelos ComexStat."""

import pytest

from agrobr.comexstat.models import NCM_PRODUTOS, ExportRecord, resolve_ncm


class TestExportRecord:
    def test_basic_creation(self):
        rec = ExportRecord(
            ano=2024,
            mes=3,
            ncm="12019000",
            uf="MT",
            kg_liquido=9876543.0,
            valor_fob_usd=1234567.0,
        )

        assert rec.ano == 2024
        assert rec.mes == 3
        assert rec.ncm == "12019000"
        assert rec.uf == "MT"

    def test_uf_normalization(self):
        rec = ExportRecord(
            ano=2024,
            mes=1,
            ncm="12019000",
            uf="mt",
            kg_liquido=100.0,
            valor_fob_usd=50.0,
        )

        assert rec.uf == "MT"

    def test_validation(self):
        with pytest.raises(ValueError):
            ExportRecord(
                ano=1990,
                mes=13,
                ncm="12019000",
                uf="MT",
                kg_liquido=-100.0,
                valor_fob_usd=50.0,
            )


class TestResolveNcm:
    def test_known_products(self):
        assert resolve_ncm("soja") == "12019000"
        assert resolve_ncm("milho") == "10059010"
        assert resolve_ncm("cafe") == "09011110"
        assert resolve_ncm("algodao") == "52010000"

    def test_case_insensitive(self):
        assert resolve_ncm("SOJA") == "12019000"
        assert resolve_ncm("Soja") == "12019000"

    def test_unknown_raises(self):
        with pytest.raises(ValueError, match="sem mapeamento NCM"):
            resolve_ncm("quinoa")

    def test_ncm_map_completeness(self):
        assert len(NCM_PRODUTOS) >= 10
