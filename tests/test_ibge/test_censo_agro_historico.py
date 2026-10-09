from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from agrobr import datasets
from agrobr.ibge import client
from agrobr.ibge.censo_api import (
    censo_agro_historico,
)
from tests import helpers

SINTETICO = json.loads(
    (
        Path(__file__).resolve().parents[1]
        / "golden_data/reconciliacao_censos_producao_ibge_conab_20260918/lot3_synthetic_regressions.json"
    ).read_text(encoding="utf-8")
)


class TestCensoHistoricoValidation:
    async def test_validacao_censo_agro_historico(self):
        cases = [
            (
                "test_tema_invalido",
                censo_agro_historico,
                ("tema_inexistente",),
                {},
                ValueError,
                "Tema inválido",
            ),
            (
                "test_ano_invalido",
                censo_agro_historico,
                ("estabelecimentos_area",),
                {"ano": 1990},
                ValueError,
                "Ano 1990 não disponível",
            ),
            (
                "test_ano_list_invalido",
                censo_agro_historico,
                ("estabelecimentos_area",),
                {"ano": [1985, 1999]},
                ValueError,
                "Ano 1999 não disponível",
            ),
            (
                "test_nivel_invalido",
                censo_agro_historico,
                ("estabelecimentos_area",),
                {"nivel": "meso"},
                ValueError,
                "Nível inválido: 'meso'",
            ),
        ]
        with helpers.collect_failures() as check:
            for case, function, args, kwargs, exception, message in cases:
                with check(case), helpers.isolated_dataset_case((case, kwargs)) as monkeypatch:
                    fetch = AsyncMock()
                    monkeypatch.setattr(client, "fetch_sidra", fetch)
                    with pytest.raises(exception, match=message):
                        await function(*args, **kwargs)
                    fetch.assert_not_awaited()


class TestCensoHistoricoParsing:
    @pytest.mark.asyncio
    @patch("agrobr.ibge.client.fetch_sidra", new_callable=AsyncMock)
    async def test_fetch_sidra_called_with_correct_params(self, mock_fetch):
        mock_fetch.return_value = pd.DataFrame()
        await censo_agro_historico("estabelecimentos_area", ano=1985, nivel="brasil")
        mock_fetch.assert_called_once()
        call_kwargs = mock_fetch.call_args
        assert call_kwargs.kwargs["table_code"] == "263"
        assert call_kwargs.kwargs["territorial_level"] == "1"
        assert call_kwargs.kwargs["period"] == "1985"
        assert "183" in call_kwargs.kwargs["variable"]


@pytest.mark.parametrize(
    "ano,periodo,return_meta",
    [
        (None, "1970,1975,1980,1985,1995,2006", False),
        (None, "1970,1975,1980,1985,1995,2006", True),
        (1970, "1970", False),
    ],
)
async def test_dataset_historico_consulta_todos_os_censos_e_rotula_total(
    monkeypatch, ano, periodo, return_meta
):
    rows = [
        {**row, "D3C": censo, "D3N": censo, "V": valor}
        for censo, valores in (("1970", ("120", "5")), ("2006", ("80", "9")))
        if censo in periodo.split(",")
        for row, valor in zip(SINTETICO["historico_1970"], valores, strict=True)
    ]
    fetch = AsyncMock(return_value=pd.DataFrame(rows))
    monkeypatch.setattr(client, "fetch_sidra", fetch)
    result = await datasets.censo_agropecuario_historico(
        "pessoal_tratores", ano=ano, return_meta=return_meta
    )
    frame, meta = result if return_meta else (result, None)
    assert str(frame["valor"].dtype) == "float64"
    assert "cod_municipio" in frame and frame["cod_municipio"].isna().all()
    assert fetch.await_args.kwargs == {
        "table_code": "265",
        "territorial_level": "3",
        "ibge_territorial_code": "all",
        "variable": "185,1862",
        "period": periodo,
        "classifications": None,
    }
    observed = frame.set_index(["ano", "variavel"])[
        ["localidade_cod", "categoria", "valor", "unidade"]
    ].to_dict("index")
    assert observed == {
        (censo, variavel): {
            "localidade_cod": 35,
            "categoria": "total",
            "valor": valor,
            "unidade": unidade,
        }
        for censo, variavel, valor, unidade in [
            (1970, "pessoal_ocupado", 120, "Pessoas"),
            (1970, "tratores", 5, "Unidades"),
            (2006, "pessoal_ocupado", 80, "Pessoas"),
            (2006, "tratores", 9, "Unidades"),
        ]
        if str(censo) in periodo.split(",")
    }
    if return_meta:
        assert meta.records_count == 4
        assert meta.selected_source == "ibge_censo_agro_historico"
