import csv
import hashlib
import io
import json
import math
import re
from pathlib import Path
from unittest.mock import AsyncMock, Mock

import pandas as pd
import pytest

from agrobr import contracts, datasets
from agrobr.bcb import api, client, models
from agrobr.exceptions import InvalidParameterError
from tests.helpers import collect_failures, levanta_exatamente

ORACULO = Path(__file__).parents[1] / "golden_data/bcb/oraculo_20260923"
ORACULO_MANIFEST = json.loads((ORACULO / "manifest.json").read_text(encoding="utf-8"))
R9_SICOR = (
    Path(__file__).parents[1]
    / "golden_data/reconciliacao_r9_20260918/sicor/sicor_custeio_soja_2024_2025_MT.json"
)
COLUNAS = [
    "safra",
    "produto",
    "uf",
    "finalidade",
    "agregacao",
    "programa",
    "cd_programa",
    "qtd_contratos",
    "valor",
    "area_financiada",
    "fonte",
]


def valores(frame: pd.DataFrame, colunas: list[str]) -> list[tuple]:
    return [
        tuple(None if pd.isna(valor) else valor for valor in linha)
        for linha in frame[colunas].itertuples(index=False, name=None)
    ]


def soma(registros: list[dict], campo: str = "VlCusteio") -> float:
    return math.fsum(registro[campo] for registro in registros)


async def test_consulta_publica_agrega_o_corpo_oficial_com_meta(monkeypatch: pytest.MonkeyPatch):
    registros = json.loads(R9_SICOR.read_bytes())["value"]
    fetch = AsyncMock(side_effect=[(registros, "odata"), (registros, "bigquery")])
    monkeypatch.setattr(api.client, "fetch_credito_rural_with_fallback", fetch)

    frame, meta = await api.credito_rural("soja", safra="2024/25", uf=" mt ", return_meta=True)

    assert fetch.await_args.kwargs == {
        "finalidade": "custeio",
        "produto_sicor": '"SOJA"',
        "safra_sicor": "2024/2025",
        "cd_uf": "51",
        "sem_fallback": None,
    }
    assert frame.columns.tolist() == COLUNAS
    assert valores(frame, COLUNAS[:7] + ["qtd_contratos", "area_financiada", "fonte"]) == [
        (
            "2024/25",
            "soja",
            "MT",
            "custeio",
            "uf",
            None,
            None,
            sum(r["QtdCusteio"] for r in registros),
            None,
            "bcb_odata",
        )
    ]
    assert frame["valor"].tolist() == [pytest.approx(soma(registros))]
    assert (
        meta.source,
        meta.source_url,
        meta.source_method,
        meta.schema_version,
        meta.attempted_sources,
        meta.selected_source,
        meta.records_count,
    ) == (
        "bcb_credito",
        f"{client.BASE_URL}/CusteioRegiaoUFProduto",
        "httpx",
        "2.0",
        ["bcb_odata"],
        "bcb_odata",
        1,
    )
    assert meta.fetch_timestamp is not None
    frame, meta = await api.credito_rural("soja", safra="2024/25", return_meta=True)
    assert (meta.source_method, meta.attempted_sources, meta.selected_source) == (
        "bigquery",
        ["bcb_odata", "bcb_bigquery"],
        "bcb_bigquery",
    )
    assert frame["fonte"].tolist() == ["bcb_bigquery"]


async def test_filtros_locais_de_uf_programa_e_seguro(monkeypatch: pytest.MonkeyPatch):
    registros = json.loads(R9_SICOR.read_bytes())["value"]
    pronamp_mt = [r for r in registros if r["cdPrograma"] == "0050"]
    pronamp_go = [dict(r, nomeUF="GO") for r in pronamp_mt[:3]]
    monkeypatch.setattr(
        api.client,
        "fetch_credito_rural_with_fallback",
        AsyncMock(return_value=(registros + pronamp_go, "odata")),
    )

    por_uf = await api.credito_rural("soja", safra="2024/25", uf="MT")
    pronamp = await api.credito_rural("soja", programa="pronamp", agregacao="programa")
    funcafe = await api.credito_rural("soja", programa="Funcafe", agregacao="programa")
    proagro = await api.credito_rural("soja", uf="MT", tipo_seguro="proagro TRADICIONAL")

    assert valores(por_uf, ["uf"]) == [("MT",)]
    assert por_uf["valor"].tolist() == [pytest.approx(soma(registros))]
    assert valores(pronamp, ["uf", "programa", "cd_programa"]) == [
        ("GO", "PRONAMP", "0050"),
        ("MT", "PRONAMP", "0050"),
    ]
    assert pronamp["valor"].tolist() == pytest.approx([soma(pronamp_go), soma(pronamp_mt)])
    assert funcafe.empty and funcafe.columns.tolist() == COLUNAS
    assert proagro["valor"].tolist() == [
        pytest.approx(soma([r for r in registros if r["cdTipoSeguro"] == "1"]))
    ]


async def test_recusas_antes_da_fonte(monkeypatch: pytest.MonkeyPatch):
    fetch = AsyncMock()
    monkeypatch.setattr(api.client, "fetch_credito_rural_with_fallback", fetch)
    with collect_failures() as check:
        for argumentos, erro, motivo in [
            ({"uf": "XX"}, InvalidParameterError, "UF inválida: 'XX'"),
            (
                {"agregacao": "municipio"},
                InvalidParameterError,
                "agregacao inválida: 'municipio'. Use agregacao='uf', 'programa' ou 'registro'. O SICOR "
                "publica município por produto (CusteioMunicipioProduto e InvestMunicipioProduto), "
                "que o agrobr ainda não lê; o extra agrobr[bigquery] traz dados municipais.",
            ),
            (
                {"finalidade": "industrializacao"},
                InvalidParameterError,
                "O SICOR não publica a industrialização por produto; use "
                "bcb.credito_rural_total(finalidade='industrializacao'), com o total por UF",
            ),
            (
                {"finalidade": "invalida"},
                InvalidParameterError,
                "Finalidade inválida: 'invalida'. Opções: ['custeio', 'investimento', "
                "'comercializacao']",
            ),
            ({"produto": ""}, InvalidParameterError, "produto deve ser uma string não vazia"),
            (
                {"produto": "cafe_arabica"},
                InvalidParameterError,
                "O SICOR não distingue café arábica/conilon; use 'cafe'",
            ),
        ]:
            with check(argumentos), levanta_exatamente(erro, match=re.escape(motivo)):
                await api.credito_rural(**{"produto": "soja", "safra": "2023/24", **argumentos})
    fetch.assert_not_awaited()


@pytest.mark.asyncio
async def test_as_polars():
    pl = pytest.importorskip("polars")
    registros = json.loads(R9_SICOR.read_bytes())["value"]
    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(
            api.client,
            "fetch_credito_rural_with_fallback",
            AsyncMock(return_value=(registros, "odata")),
        )
        result = await api.credito_rural("soja", safra="2024/25", as_polars=True)
    assert isinstance(result, pl.DataFrame)
    assert result.columns == COLUNAS


@pytest.mark.integration
@pytest.mark.asyncio
async def test_credito_rural_live_filtra_uf_e_safra():
    df = await api.credito_rural("soja", safra="2024/25", uf="MT")

    assert not df.empty
    assert set(df["uf"]) == {"MT"}
    assert set(df["safra"]) == {"2024/25"}


@pytest.mark.parametrize("nulo", [None, float("nan")], ids=["none", "nan"])
async def test_codigo_sicor_nulo_vira_nome_nulo(
    nulo: float | None, monkeypatch: pytest.MonkeyPatch
):
    registros = json.loads(R9_SICOR.read_bytes())["value"]
    alvo = next(registro for registro in registros if registro["cdPrograma"] == "0999")
    alvo.update(cdPrograma=nulo, cdModalidade=nulo)
    restantes = [r for r in registros if r["cdPrograma"] == "0999"]
    avisos = Mock()
    monkeypatch.setattr(models, "logger", avisos)
    monkeypatch.setattr(
        api.client,
        "fetch_credito_rural_with_fallback",
        AsyncMock(return_value=(registros, "odata")),
    )

    fonte = await api.credito_rural("soja", safra="2024/25", uf="MT", agregacao="programa")
    dataset = await datasets.credito_rural("soja", safra="2024/25", uf="MT", agregacao="programa")

    for frame in (fonte, dataset):
        nulos = frame[frame["cd_programa"].isna()]
        sem_programa = frame[frame["cd_programa"] == "0999"]
        assert nulos["programa"].isna().all()
        assert (len(nulos), nulos["valor"].sum(), nulos["qtd_contratos"].sum()) == (
            1,
            alvo["VlCusteio"],
            alvo["QtdCusteio"],
        )
        assert sem_programa["programa"].tolist() == [
            "FINANCIAMENTO SEM VÍNCULO A PROGRAMA ESPECÍFICO"
        ]
        assert sem_programa["valor"].sum() == pytest.approx(
            math.fsum(r["VlCusteio"] for r in restantes)
        )
        assert not frame["programa"].astype("string").str.startswith("Desconhecido").any()
        contracts.validate_dataset(frame, "credito_rural")
    avisados = {str(chamada.kwargs.get("codigo")) for chamada in avisos.warning.call_args_list}
    assert not avisados & {"nan", "None", "<NA>"}


def corpo_oraculo(nome: str) -> list[dict]:
    recurso = next(item for item in ORACULO_MANIFEST["resources"] if item["file"] == nome)
    corpo = (ORACULO / nome).read_bytes()
    assert hashlib.sha256(corpo).hexdigest() == recurso["sha256"]
    return json.loads(corpo)["value"]


def programas_oficiais() -> dict[str, str]:
    corpo = (ORACULO / "dominio_Programa.csv").read_bytes().decode("cp1252")
    return {
        codigo: descricao.split(" - ", 1)[0].replace('"', "").strip()
        for codigo, descricao, *_vigencia in list(csv.reader(io.StringIO(corpo), delimiter=";"))[1:]
    }


async def test_programa_e_seguro_publicados_pela_tabela_oficial(monkeypatch: pytest.MonkeyPatch):
    investimento = corpo_oraculo("sicor_investimento_bovinos_mt_000.json")
    custeio = json.loads(R9_SICOR.read_bytes())["value"]
    monkeypatch.setattr(
        api.client,
        "fetch_credito_rural_with_fallback",
        AsyncMock(side_effect=[(investimento, "odata")] + [(custeio, "odata")] * 5),
    )
    oficiais = programas_oficiais()

    investimento_fonte = await api.credito_rural(
        "bovinos", safra="2024/25", finalidade="investimento", uf="MT", agregacao="programa"
    )
    custeio_dataset = await datasets.credito_rural(
        "soja", safra="2024/25", uf="MT", agregacao="programa"
    )

    for frame, registros, campo in (
        (investimento_fonte, investimento, "VlInvest"),
        (custeio_dataset, custeio, "VlCusteio"),
    ):
        esperado: dict[str, float] = {}
        for registro in registros:
            nome = oficiais[registro["cdPrograma"]]
            esperado[nome] = esperado.get(nome, 0.0) + registro[campo]
        assert dict(zip(frame["programa"], frame["valor"], strict=True)) == pytest.approx(esperado)
    assert {"MODERAGRO", "INOVAGRO", "RenovAgro"} <= set(investimento_fonte["programa"])
    for consultar in (api.credito_rural, datasets.credito_rural):
        sem_adesao = await consultar(
            "soja", safra="2024/25", uf="MT", tipo_seguro="sem adesão a seguro"
        )
        proagro_mais = await consultar("soja", safra="2024/25", uf="MT", tipo_seguro="PROAGRO MAIS")
        assert sem_adesao["valor"].sum() == pytest.approx(
            math.fsum(r["VlCusteio"] for r in custeio if r["cdTipoSeguro"] == "9")
        )
        assert proagro_mais["valor"].sum() == pytest.approx(
            math.fsum(r["VlCusteio"] for r in custeio if r["cdTipoSeguro"] == "2")
        )


async def test_finalidade_publicada_igual_nas_duas_fontes(monkeypatch: pytest.MonkeyPatch):
    odata = json.loads(R9_SICOR.read_bytes())["value"]
    bigquery = [
        {
            "ano_emissao": 2024,
            "mes_emissao": 9,
            "uf": "MT",
            "cd_municipio": "5107248",
            "produto": "SOJA",
            "finalidade": "CUSTEIO",
            "valor": 285431200.0,
            "area_financiada": 98500.0,
            "qtd_contratos": 1240,
        }
    ]
    fetch = AsyncMock(
        side_effect=[
            (odata, "odata"),
            (bigquery, "bigquery"),
            (odata, "odata"),
            (bigquery, "bigquery"),
        ]
    )
    monkeypatch.setattr(api.client, "fetch_credito_rural_with_fallback", fetch)

    publicadas = [
        (await api.credito_rural("soja", safra="2024/25", uf="MT", finalidade=finalidade))[
            ["finalidade", "fonte"]
        ].values.tolist()
        for finalidade in ["custeio", "custeio", "Custeio", "CUSTEIO"]
    ]

    assert publicadas == [
        [["custeio", "bcb_odata"]],
        [["custeio", "bcb_bigquery"]],
        [["custeio", "bcb_odata"]],
        [["custeio", "bcb_bigquery"]],
    ]
