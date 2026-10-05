from __future__ import annotations

from datetime import date
from decimal import Decimal

import pandas as pd
import pytest

from agrobr.cache import duckdb_store
from agrobr.cepea import api as cepea_api
from agrobr.cepea import client as cepea_client
from agrobr.exceptions import ParseError, StaleDataWarning
from agrobr.models import Indicador
from agrobr.noticias_agricolas import parser
from tests.helpers import levanta_exatamente


def _pagina(*linhas: tuple[str, str]) -> str:
    corpo = "".join(
        f"<tr><td>{data}</td><td>{valor}</td><td>0,00%</td></tr>" for data, valor in linhas
    )
    return (
        '<div class="cotacao"><div class="fechamento">Fechamento: 25/09/2026</div>'
        '<table class="cot-fisicas"><thead><tr><th>Data</th><th>Valor R$/ Saca de 60 kg</th>'
        f"<th>Variação (%)</th></tr></thead><tbody>{corpo}</tbody></table></div>"
    )


NAO_POSITIVOS = _pagina(("25/09/2026", "0,00"), ("24/09/2026", "-1,00"))


def test_parser_descarta_valor_nao_positivo():
    pagina = _pagina(("25/09/2026", "0,00"), ("24/09/2026", "-1,00"), ("23/09/2026", "155,07"))

    indicadores = parser.parse_indicador(pagina, "soja_parana")

    assert [(ind.data, ind.valor) for ind in indicadores] == [
        (date(2026, 9, 23), Decimal("155.07"))
    ]


def _cepea_fora_do_ar(monkeypatch: pytest.MonkeyPatch) -> None:
    async def pagina_da_na(*_args: object, **_kwargs: object) -> cepea_client.FetchResult:
        return cepea_client.FetchResult(html=NAO_POSITIVOS, source="noticias_agricolas")

    monkeypatch.setattr(cepea_api.client, "fetch_indicador_page", pagina_da_na)
    monkeypatch.setattr(cepea_api, "_today", lambda: date(2026, 9, 25))


async def test_na_so_com_valor_nao_positivo_usa_o_cache(monkeypatch):
    _cepea_fora_do_ar(monkeypatch)
    cache = Indicador(
        fonte="cepea",
        produto="soja_parana",
        praca="Paraná",
        data=date(2026, 9, 22),
        valor=Decimal("155.50"),
        unidade="BRL/sc60kg",
    )
    duckdb_store.get_store().indicadores_upsert(cepea_api._indicadores_to_dicts([cache]))

    with pytest.warns(StaleDataWarning):
        df = await cepea_api.indicador(
            "soja_parana", inicio="2026-09-20", fim="2026-09-25", force_refresh=True
        )

    assert df[["data", "valor"]].to_dict("records") == [
        {"data": pd.Timestamp("2026-09-22"), "valor": 155.5}
    ]


async def test_na_so_com_valor_nao_positivo_sem_cache_levanta_parse_error(monkeypatch):
    _cepea_fora_do_ar(monkeypatch)

    with levanta_exatamente(ParseError, "No indicators found for 'soja_parana'"):
        await cepea_api.indicador("soja_parana", inicio="2026-09-20", fim="2026-09-25")
