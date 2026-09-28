from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock

import pandas as pd
import pytest

from agrobr import contracts, datasets
from agrobr.ibge import censo_agro, client
from tests import helpers

SINTETICO = json.loads(
    (
        Path(__file__).resolve().parents[1]
        / "golden_data/reconciliacao_r5_20260918/lot3_synthetic_regressions.json"
    ).read_text(encoding="utf-8")
)
COLUNAS = [
    "ano",
    "localidade",
    "localidade_cod",
    "tema",
    "categoria",
    "variavel",
    "valor",
    "unidade",
    "fonte",
]
CENSO_2017 = Path(__file__).resolve().parents[1] / "golden_data/ibge/censo_2017_oficial_20260922"
CENSO_OFICIAL = {
    2006: Path(__file__).resolve().parents[1] / "golden_data/ibge/censo_2006_oficial_20260922",
    2017: CENSO_2017,
}


class TestCensoAgroValidation:
    async def test_validacao_censo_agro(self):
        cases = [
            (
                "test_tema_invalido",
                censo_agro,
                ("tema_inexistente",),
                {},
                ValueError,
                "Tema não suportado",
            ),
            (
                "test_ano_invalido",
                censo_agro,
                ("preparo_solo",),
                {"ano": 2020},
                ValueError,
                "Ano .* não disponível",
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


@pytest.mark.parametrize(
    "layout,ano",
    [("ano_do_registro", 2006), ("sem_dimensao_de_ano", 2017), ("classificacao_na_d3", 2017)],
)
async def test_preparo_solo_2017_variaveis_viram_categorias(monkeypatch, layout, ano):
    rows = [dict(row) for row in SINTETICO["preparo_2017"]]
    rows[0]["V"] = "-"
    rows.append({**rows[1], "D2C": "9999", "D2N": "Variável fora do mapa", "V": "99"})
    for row in rows:
        if layout == "ano_do_registro":
            row["D3C"] = row["D3N"] = "2006"
        elif layout == "classificacao_na_d3":
            row["D3C"], row["D3N"] = row.pop("D4C"), row.pop("D4N")
        else:
            del row["D3C"], row["D3N"]
    monkeypatch.setattr(client, "fetch_sidra", AsyncMock(return_value=pd.DataFrame(rows)))
    frame = await censo_agro("preparo_solo", ano=2017)
    expected = pd.DataFrame(
        [
            ("Não utiliza preparo", "estabelecimentos", 0.0, "unidades"),
            ("Utiliza preparo", "estabelecimentos", 20.0, "unidades"),
            ("Cultivo convencional", "estabelecimentos", 30.0, "unidades"),
            ("Cultivo mínimo", "estabelecimentos", 40.0, "unidades"),
            ("Plantio direto na palha", "estabelecimentos", 50.0, "unidades"),
            ("Plantio direto na palha", "area", 60.0, "hectares"),
        ],
        columns=["categoria", "variavel", "valor", "unidade"],
    ).assign(ano=ano, localidade="São Paulo", localidade_cod=35, tema="preparo_solo")
    expected["fonte"] = "ibge_censo_agro"
    assert list(frame.columns) == COLUNAS
    assert str(frame["localidade_cod"].dtype) == "Int64"
    assert str(frame["valor"].dtype) == "float64"
    pd.testing.assert_frame_equal(
        frame.sort_values(["categoria", "variavel"]).reset_index(drop=True),
        expected[COLUNAS].sort_values(["categoria", "variavel"]).reset_index(drop=True),
        check_dtype=False,
    )


@pytest.mark.parametrize(
    "layout", ["classificacao_antes_da_variavel", "ano_antes_da_classificacao"]
)
async def test_preparo_solo_2006_categoria_vem_da_classificacao(monkeypatch, layout):
    categorias = [
        ("113223", "Aração e/ou gradagem (cultivo convencional)", "8035"),
        ("113224", "Cultivo mínimo", "8036"),
        ("114631", "Plantio direto na palha", "8037"),
    ]
    variavel = ("183", "Número de estabelecimentos agropecuários")
    rows = []
    for codigo, nome, valor in categorias:
        dims = (
            [(codigo, nome), variavel]
            if layout == "classificacao_antes_da_variavel"
            else [variavel, ("2006", "2006"), (codigo, nome), ("46302", "Total")]
        )
        row = {"NC": "3", "NN": "Unidade da Federação", "MC": "1020", "MN": "Unidades"}
        row |= {"V": valor, "D1C": "35", "D1N": "São Paulo"}
        for index, (dim_code, dim_name) in enumerate(dims, start=2):
            row |= {f"D{index}C": dim_code, f"D{index}N": dim_name}
        rows.append(row)
    monkeypatch.setattr(client, "fetch_sidra", AsyncMock(return_value=pd.DataFrame(rows)))
    frame = await censo_agro("preparo_solo", ano=2006)
    observed = frame.set_index("categoria")[["ano", "localidade_cod", "variavel", "valor"]]
    assert observed.to_dict("index") == {
        nome: {"ano": 2006, "localidade_cod": 35, "variavel": "estabelecimentos", "valor": int(v)}
        for _codigo, nome, v in categorias
    }


@pytest.mark.parametrize(("ano", "total"), [(2006, 6), (2017, 11)])
async def test_censo_pede_so_variaveis_e_categorias_das_tabelas_oficiais(monkeypatch, ano, total):
    pedidos = []

    async def fetch_sidra(**pedido):
        pedidos.append(pedido)
        return pd.DataFrame()

    monkeypatch.setattr(client, "fetch_sidra", fetch_sidra)
    temas = [tema for tema, anos in client.TABELAS_CENSO_AGRO.items() if str(ano) in anos]
    for tema in temas:
        await censo_agro(tema, ano=ano, nivel="brasil")
    assert len(pedidos) == len(temas) == total
    for tema, pedido in zip(temas, pedidos, strict=True):
        oficial = json.loads(
            (CENSO_OFICIAL[ano] / f"metadados_{pedido['table_code']}.json").read_text(
                encoding="utf-8"
            )
        )
        periodo = oficial["periodicidade"]
        assert periodo["inicio"] <= ano <= periodo["fim"], (tema, pedido["table_code"])
        variaveis = {str(variavel["id"]) for variavel in oficial["variaveis"]}
        categorias = {
            str(classificacao["id"]): {"all"} | {str(c["id"]) for c in classificacao["categorias"]}
            for classificacao in oficial["classificacoes"]
        }
        assert set(pedido["variable"].split(",")) <= variaveis, (tema, pedido["variable"])
        for codigo, categoria in pedido["classifications"].items():
            assert categoria in categorias.get(codigo, set()), (tema, codigo, categoria)


@pytest.mark.parametrize("ano", [2006, 2017])
async def test_irrigacao_pede_as_medidas_absolutas_da_tabela_oficial(monkeypatch, ano):
    pedidos = []

    async def fetch_sidra(**pedido):
        pedidos.append(pedido)
        return pd.DataFrame()

    monkeypatch.setattr(client, "fetch_sidra", fetch_sidra)
    await censo_agro("irrigacao", ano=ano, nivel="brasil")
    oficial = json.loads(
        (CENSO_OFICIAL[ano] / f"metadados_{pedidos[0]['table_code']}.json").read_text(
            encoding="utf-8"
        )
    )
    absolutas = {str(v["id"]) for v in oficial["variaveis"] if v["unidade"] != "%"}
    assert set(pedidos[0]["variable"].split(",")) == absolutas


async def test_irrigacao_2017_confere_celulas_oficiais_de_brasilia(monkeypatch):
    corpo = CENSO_2017 / "sidra_6857_df_municipio.json"
    oficial = json.loads(corpo.read_text(encoding="utf-8"))
    path, params, skip = helpers.replay_signature(
        "https://apisidra.ibge.gov.br/values/t/6857/n6/in%20N3%2053/h/n/p/all/v/2372,2373/c12604/all"
    )
    pedido = {"match": {"path": path, "params": dict(params), "skip": skip}}
    pedido |= {"file": corpo.name, "content_type": "application/json"}
    seen = helpers.install_replay_http(monkeypatch, {"requests": [pedido]}, CENSO_2017)
    frame = await censo_agro("irrigacao", ano=2017, uf="DF", nivel="municipio")
    helpers.assert_replay_served(seen)
    total = {row["D3C"]: row["V"] for row in oficial if row["D4N"] == "Total"}
    assert total == {"2372": "2726", "2373": "25626"}
    esperado = {
        (row["D4N"], "area" if row["D3N"].startswith("Área") else "estabelecimentos"): (
            float(row["V"]),
            row["MN"].lower(),
        )
        for row in oficial
    }
    assert len(esperado) == len(frame) == 24
    observado = {(r.categoria, r.variavel): (r.valor, r.unidade) for r in frame.itertuples()}
    assert observado == esperado
    assert set(zip(frame["ano"], frame["localidade"], frame["localidade_cod"], strict=True)) == {
        (2017, "Brasília - DF", 5300108)
    }


async def test_censo_irrigacao_df_entrega_valor_float64_como_o_contrato(monkeypatch):
    corpo = CENSO_2017 / "sidra_6857_df_municipio.json"
    oficial = json.loads(corpo.read_text(encoding="utf-8"))
    path, params, skip = helpers.replay_signature(
        "https://apisidra.ibge.gov.br/values/t/6857/n6/in%20N3%2053/h/n/p/all/v/2372,2373/c12604/all"
    )
    pedido = {"match": {"path": path, "params": dict(params), "skip": skip}}
    pedido |= {"file": corpo.name, "content_type": "application/json"}
    seen = helpers.install_replay_http(monkeypatch, {"requests": [pedido]}, CENSO_2017)
    frame = await datasets.censo_agropecuario("irrigacao", ano=2017, uf="DF", nivel="municipio")
    helpers.assert_replay_served(seen)
    flutuantes = [
        coluna.name
        for coluna in contracts.get_contract("censo_agropecuario").columns
        if coluna.type is contracts.ColumnType.FLOAT
    ]
    assert flutuantes == ["valor"]
    assert str(frame["valor"].dtype) == "float64"
    gotejamento = [row for row in oficial if row["D4C"] == "45916"]
    esperado = {
        ("area" if row["D3N"].startswith("Área") else "estabelecimentos"): float(row["V"])
        for row in gotejamento
    }
    assert esperado == {"estabelecimentos": 1336.0, "area": 7731.0}
    rotulo = frame["categoria"] == gotejamento[0]["D4N"]
    assert frame[rotulo].set_index("variavel")["valor"].to_dict() == esperado
