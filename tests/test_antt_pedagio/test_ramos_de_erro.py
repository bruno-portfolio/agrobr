from __future__ import annotations

from agrobr.alt import antt_pedagio
from agrobr.exceptions import ParseError
from tests import helpers


async def test_pracas_csv_com_aspas_abertas_preserva_erro_de_layout(monkeypatch):
    chamadas, _ = helpers.install_anttpedagio_source(
        monkeypatch,
        {},
        plazas=b'concessionaria;praca_de_pedagio\n"Concessionaria;Praca\n',
    )

    with helpers.levanta_exatamente(ParseError, match="CSV cadastro inválido: Error"):
        await antt_pedagio.pracas_pedagio()

    assert len(chamadas) == 2
    assert chamadas[-1].url.path == "/pracas.csv"
