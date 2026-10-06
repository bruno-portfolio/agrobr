from __future__ import annotations

from agrobr import desmatamento
from agrobr.exceptions import ParseError
from tests import helpers


async def test_prodes_recusa_ocorrencia_de_outro_ano(monkeypatch):
    features = helpers.desmatamento_features("prodes_amazonia")[:1]
    assert features[0]["properties"]["year"] == 2022
    calls = helpers.install_desmatamento_wfs(monkeypatch, features)

    with helpers.levanta_exatamente(ParseError, "Ocorrência 0 contradiz filtro ano") as erro:
        await desmatamento.prodes(ano=2021, bioma="Amazônia", max_registros=None)

    assert erro.value.source == "desmatamento"
    assert len(calls) == 2
    assert all(request.url.params["CQL_FILTER"] == "year=2021" for request, _ in calls)
