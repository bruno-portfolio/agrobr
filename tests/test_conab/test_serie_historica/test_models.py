from agrobr.conab.serie_historica.models import SafraHistorica


class TestSafraHistorica:
    def test_uf_none_stays_none(self):
        rec = SafraHistorica(
            produto="soja",
            safra="2023/24",
            uf=None,
        )
        assert rec.uf is None

    def test_regiao_none_stays_none(self):
        rec = SafraHistorica(
            produto="soja",
            safra="2023/24",
            regiao=None,
        )
        assert rec.regiao is None
