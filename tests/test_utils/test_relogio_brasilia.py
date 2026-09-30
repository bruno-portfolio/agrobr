from __future__ import annotations

import importlib
import json
import os
import subprocess
import sys
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import httpx
import pytest

import agrobr
from agrobr import datasets
from agrobr.anda import client as anda_client
from agrobr.exceptions import SourceUnavailableError
from agrobr.utils import time as time_utils
from tests.helpers import levanta_exatamente

REVEILLON_22H30_BRT = datetime(2027, 1, 1, 1, 30, tzinfo=UTC)
REVEILLON_20H30_BRT = datetime(2026, 12, 31, 23, 30, tzinfo=UTC)
PADROES_AS_23H_DE_NOVA_YORK = """
import importlib
import json
import time
from datetime import UTC, date, datetime

relogio = importlib.import_module("agrobr.utils.time")
modulos = {
    nome: importlib.import_module(caminho)
    for nome, caminho in (
        ("cepea", "agrobr.cepea.api"),
        ("conab", "agrobr.conab.api"),
        ("conab_serie_historica", "agrobr.conab._serie_historica.api"),
        ("producao_anual", "agrobr.datasets.producao_anual"),
    )
}
instante = datetime(2026, 1, 15, 4, 0, tzinfo=UTC)
time.time = instante.timestamp
relogio.utcnow_aware = lambda: instante
padroes = {"maquina": date.today()}
padroes["cepea"] = modulos["cepea"]._today()
for nome in ("conab", "conab_serie_historica", "producao_anual"):
    padroes[nome] = modulos[nome]._hoje()
print(json.dumps({nome: data.isoformat() for nome, data in padroes.items()}))
"""
PADROES_DE_ANO_E_SAFRA_EM_NOVA_YORK = """
import asyncio
import json
import sys
import time
from datetime import date, datetime

from agrobr.utils import time as relogio
from tests.test_utils import test_relogio_brasilia as teste

instante = datetime.fromisoformat(sys.argv[1])
time.time = instante.timestamp
relogio.utcnow_aware = lambda: instante
padroes = asyncio.run(teste.padroes_de_ano_e_safra(setattr))
print(json.dumps({"maquina": date.today().isoformat(), **padroes}))
"""


class _Parar(Exception):
    pass


async def padroes_de_ano_e_safra(trocar: Any) -> dict[str, Any]:
    ibge = importlib.import_module("agrobr.ibge")
    ibge_api = importlib.import_module("agrobr.ibge.api")
    estimativa = importlib.import_module("agrobr.datasets.estimativa_safra")
    antt = importlib.import_module("agrobr.alt.antt_pedagio.models")
    periodos: list[str] = []

    async def capturar(**kwargs: Any) -> None:
        periodos.append(kwargs["period"])
        raise _Parar

    lspa = AsyncMock(side_effect=_Parar)
    trocar(ibge_api.client, "fetch_sidra", capturar)
    with levanta_exatamente(_Parar):
        await ibge_api.lspa("soja")
    trocar(ibge, "lspa", lspa)
    with levanta_exatamente(_Parar):
        await estimativa._fetch_ibge_lspa("soja")
    return {
        "ibge_lspa": int(periodos[0][:4]),
        "estimativa_safra": lspa.await_args.kwargs["ano"],
        "safra_atual": importlib.import_module("agrobr.normalize.dates").safra_atual(),
        "antt_sem_ano": antt._resolve_anos(),
        "antt_desde_2025": antt._resolve_anos(ano_inicio=2025),
    }


def em_nova_york(codigo: str, *argumentos: str) -> dict[str, Any]:
    raiz = Path(agrobr.__file__).parents[1]
    saida = subprocess.run(
        [sys.executable, "-c", codigo, *argumentos],
        cwd=raiz,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=120,
        env={
            **os.environ,
            "PYTHONPATH": str(raiz),
            "PYTHONIOENCODING": "utf-8",
            "TZ": "EST5EDT",
        },
        check=False,
    )
    assert saida.returncode == 0, saida.stderr[-2000:]
    return json.loads(saida.stdout.strip().splitlines()[-1])


@pytest.fixture
def rede_recusada(monkeypatch: pytest.MonkeyPatch) -> None:
    factory = httpx.AsyncClient

    def recusar(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("rede recusada", request=request)

    monkeypatch.setattr(
        httpx,
        "AsyncClient",
        lambda **kwargs: factory(transport=httpx.MockTransport(recusar), **kwargs),
    )
    monkeypatch.setenv("AGROBR_HTTP_MAX_RETRIES", "1")


@pytest.mark.parametrize(
    "agora", [REVEILLON_22H30_BRT, REVEILLON_20H30_BRT], ids=["22h30", "20h30"]
)
def test_hoje_e_a_data_de_brasilia(monkeypatch: pytest.MonkeyPatch, agora: datetime):
    monkeypatch.setattr(time_utils, "utcnow_aware", lambda: agora)
    assert time_utils.hoje() == date(2026, 12, 31)


@pytest.mark.usefixtures("rede_recusada")
@pytest.mark.parametrize(
    "agora", [REVEILLON_22H30_BRT, REVEILLON_20H30_BRT], ids=["22h30", "20h30"]
)
async def test_padrao_do_fertilizante_passa_na_validacao_da_anda(
    monkeypatch: pytest.MonkeyPatch, agora: datetime
):
    monkeypatch.setattr(time_utils, "utcnow_aware", lambda: agora)
    fonte = AsyncMock(side_effect=SourceUnavailableError("anda", last_error="rede recusada"))
    monkeypatch.setattr(anda_client, "fetch_entregas_pdf", fonte)

    with levanta_exatamente(SourceUnavailableError):
        await datasets.fertilizante("total")

    fonte.assert_awaited_once_with(2026)


@pytest.mark.usefixtures("rede_recusada")
@pytest.mark.parametrize(
    "agora", [REVEILLON_22H30_BRT, REVEILLON_20H30_BRT], ids=["22h30", "20h30"]
)
async def test_padrao_do_clima_passa_na_validacao_do_inmet(
    monkeypatch: pytest.MonkeyPatch, agora: datetime
):
    monkeypatch.setattr(time_utils, "utcnow_aware", lambda: agora)
    monkeypatch.delenv("AGROBR_INMET_TOKEN", raising=False)

    with levanta_exatamente(SourceUnavailableError) as erro:
        await datasets.clima("MT")

    assert "ano deve" not in str(erro.value)


def test_padrao_do_cepea_conab_e_pam_segue_o_relogio_do_agrobr(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(time_utils, "utcnow_aware", lambda: datetime(2001, 1, 15, 4, 0, tzinfo=UTC))
    modulos = {
        nome: importlib.import_module(caminho)
        for nome, caminho in (
            ("cepea", "agrobr.cepea.api"),
            ("conab", "agrobr.conab.api"),
            ("conab_serie_historica", "agrobr.conab._serie_historica.api"),
            ("producao_anual", "agrobr.datasets.producao_anual"),
        )
    }

    padroes = {"cepea": modulos["cepea"]._today()}
    for nome in ("conab", "conab_serie_historica", "producao_anual"):
        padroes[nome] = modulos[nome]._hoje()

    assert padroes == dict.fromkeys(modulos, date(2001, 1, 15))


def test_padrao_do_cepea_conab_e_pam_e_a_data_de_brasilia_as_23h_de_nova_york():
    padroes = em_nova_york(PADROES_AS_23H_DE_NOVA_YORK)
    assert padroes.pop("maquina") == "2026-01-14"
    assert padroes == dict.fromkeys(
        ("cepea", "conab", "conab_serie_historica", "producao_anual"), "2026-01-15"
    )


async def test_padroes_de_ano_e_safra_seguem_o_relogio_do_agrobr(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(time_utils, "utcnow_aware", lambda: datetime(2031, 1, 1, 3, 30, tzinfo=UTC))
    padroes = await padroes_de_ano_e_safra(monkeypatch.setattr)
    monkeypatch.setattr(time_utils, "utcnow_aware", lambda: datetime(2031, 7, 1, 3, 30, tzinfo=UTC))
    padroes["safra_em_julho"] = importlib.import_module("agrobr.normalize.dates").safra_atual()

    assert padroes == {
        "ibge_lspa": 2031,
        "estimativa_safra": 2031,
        "safra_atual": "2030/31",
        "antt_sem_ano": [2030, 2031],
        "antt_desde_2025": list(range(2025, 2032)),
        "safra_em_julho": "2031/32",
    }


@pytest.mark.parametrize(
    ("instante", "maquina", "safra"),
    [
        ("2026-01-01T03:30:00+00:00", "2025-12-31", "2025/26"),
        ("2026-07-01T03:30:00+00:00", "2026-06-30", "2026/27"),
    ],
    ids=["22h30_de_31_12", "23h30_de_30_06"],
)
def test_padroes_de_ano_e_safra_na_virada_em_nova_york(instante: str, maquina: str, safra: str):
    padroes = em_nova_york(PADROES_DE_ANO_E_SAFRA_EM_NOVA_YORK, instante)

    assert padroes.pop("maquina") == maquina
    assert padroes == {
        "ibge_lspa": 2026,
        "estimativa_safra": 2026,
        "safra_atual": safra,
        "antt_sem_ano": [2025, 2026],
        "antt_desde_2025": [2025, 2026],
    }
