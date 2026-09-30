from __future__ import annotations

import csv
import hashlib
import io
import json
import math
import re
from pathlib import Path
from unittest.mock import AsyncMock

import pandas as pd
import pytest

from agrobr import bcb, contracts, datasets
from agrobr.bcb import api, bigquery_client, client, parser
from agrobr.exceptions import SourceUnavailableError
from tests.helpers import (
    assert_replay_served,
    collect_failures,
    install_replay_http,
    levanta_exatamente,
)

ORACULO = Path(__file__).parents[1] / "golden_data/bcb/oraculo_20260923"
ORACULO_MANIFEST = json.loads((ORACULO / "manifest.json").read_bytes())
DOMINIOS = Path(__file__).parents[1] / "golden_data/bcb/sicor_dominios_20260927"
DOMINIOS_MANIFEST = json.loads((DOMINIOS / "manifest.json").read_bytes())
REGISTRO = contracts.get_contract("bcb_credito_rural_registro")
CASOS = {
    "sicor_custeio_milho_mt": ("milho", "custeio", ("QtdCusteio", "VlCusteio", "AreaCusteio")),
    "sicor_investimento_bovinos_mt": ("bovinos", "investimento", ("QtdInvest", "VlInvest", None)),
    "sicor_comercializacao_soja_mt": ("soja", "comercializacao", ("QtdComerc", "VlComerc", None)),
}
COLUNAS_DO_REGISTRO = [
    "ano_emissao",
    "mes_emissao",
    "regiao",
    "uf",
    "cd_programa",
    "programa",
    "cd_sub_programa",
    "cd_fonte_recurso",
    "fonte_recurso",
    "cd_tipo_seguro",
    "tipo_seguro",
    "cd_modalidade",
    "modalidade",
    "cd_atividade",
    "atividade",
    "qtd_contratos",
    "valor",
    "area_financiada",
]


def recurso(pasta: Path, manifesto: dict, nome: str) -> bytes:
    recibo = next(item for item in manifesto["resources"] if item["file"] == nome)
    corpo = (pasta / nome).read_bytes()
    assert hashlib.sha256(corpo).hexdigest() == recibo["sha256"]
    return corpo


def tabela(pasta: Path, manifesto: dict, nome: str, delimitador: str) -> list[list[str]]:
    texto = recurso(pasta, manifesto, nome).decode("cp1252")
    return list(csv.reader(io.StringIO(texto), delimiter=delimitador))[1:]


def nomes_oficiais() -> dict[str, dict[str, str]]:
    modalidades: dict[str, set[str]] = {}
    for *_finalidade_atividade, codigo, nome in tabela(
        DOMINIOS, DOMINIOS_MANIFEST, "dominio_Modalidade.csv", ";"
    ):
        modalidades.setdefault(codigo, set()).add(nome.strip())
    assert all(len(nomes) == 1 for nomes in modalidades.values())
    return {
        "programa": {
            codigo: descricao.split(" - ", 1)[0].replace('"', "").strip()
            for codigo, descricao, *_vigencia in tabela(
                ORACULO, ORACULO_MANIFEST, "dominio_Programa.csv", ";"
            )
        },
        "tipo_seguro": dict(
            tabela(ORACULO, ORACULO_MANIFEST, "dominio_TipoGarantiaEmpreendimento.csv", ",")
        ),
        "fonte_recurso": {
            codigo: descricao.strip()
            for codigo, descricao, *_vigencia in tabela(
                DOMINIOS, DOMINIOS_MANIFEST, "dominio_FonteRecursos.csv", ";"
            )
        },
        "modalidade": {codigo: nomes.pop() for codigo, nomes in modalidades.items()},
        "atividade": dict(tabela(DOMINIOS, DOMINIOS_MANIFEST, "dominio_Atividade.csv", ",")),
    }


def corpo(caso_id: str) -> list[dict]:
    caso = next(c for c in ORACULO_MANIFEST["cases"] if c["id"] == caso_id)
    return json.loads(recurso(ORACULO, ORACULO_MANIFEST, caso["requests"][0]["file"]))["value"]


def servir(monkeypatch: pytest.MonkeyPatch, caso_id: str) -> dict[str, list[str]]:
    caso = next(c for c in ORACULO_MANIFEST["cases"] if c["id"] == caso_id)
    return install_replay_http(monkeypatch, caso, ORACULO)


def linhas(frame: pd.DataFrame, colunas: list[str]) -> list[tuple]:
    return [
        tuple(None if pd.isna(valor) else valor for valor in linha)
        for linha in frame[colunas].itertuples(index=False, name=None)
    ]


@pytest.mark.parametrize("caso_id", list(CASOS))
async def test_registro_publica_cada_registro_do_sicor(
    caso_id: str, monkeypatch: pytest.MonkeyPatch
):
    produto, finalidade, (qtd, valor, area) = CASOS[caso_id]
    registros = corpo(caso_id)
    nomes = nomes_oficiais()
    visto = servir(monkeypatch, caso_id)

    frame, meta = await bcb.credito_rural(
        produto,
        safra="2024/25",
        finalidade=finalidade,
        uf="MT",
        agregacao="registro",
        return_meta=True,
    )

    assert_replay_served(visto)
    assert (frame.columns.tolist(), len(frame)) == (REGISTRO.list_columns(), len(registros))
    esperado = sorted(
        (
            int(r["AnoEmissao"]),
            int(r["MesEmissao"]),
            r["nomeRegiao"],
            r["nomeUF"],
            r["cdPrograma"],
            nomes["programa"][r["cdPrograma"]],
            r["cdSubPrograma"],
            r["cdFonteRecurso"],
            nomes["fonte_recurso"][r["cdFonteRecurso"]],
            r["cdTipoSeguro"],
            nomes["tipo_seguro"][r["cdTipoSeguro"]],
            r["cdModalidade"],
            nomes["modalidade"][str(int(r["cdModalidade"]))],
            r["Atividade"],
            nomes["atividade"][r["Atividade"]],
            r[qtd],
            r[valor],
            r[area] if area else None,
        )
        for r in registros
    )
    assert sorted(linhas(frame, COLUNAS_DO_REGISTRO)) == esperado
    assert set(linhas(frame, ["safra", "produto", "finalidade", "agregacao", "fonte"])) == {
        ("2024/25", produto, finalidade, "registro", "bcb_odata")
    }
    assert (
        meta.schema_version,
        meta.contract_version,
        meta.source_details["contract"],
        meta.records_count,
    ) == ("1.0", "1.0", "bcb.credito_rural_registro", len(registros))
    contracts.validate_dataset(frame, REGISTRO)


async def test_dataset_registro_igual_a_fonte_com_o_contrato_do_registro(
    monkeypatch: pytest.MonkeyPatch,
):
    servir(monkeypatch, "sicor_custeio_milho_mt")

    fonte = await bcb.credito_rural("milho", safra="2024/25", uf="MT", agregacao="registro")
    dataset, meta = await datasets.credito_rural(
        "milho", safra="2024/25", uf="MT", agregacao="registro", return_meta=True
    )

    pd.testing.assert_frame_equal(dataset, fonte)
    assert (
        meta.dataset,
        meta.schema_version,
        meta.contract_version,
        meta.source_details["contract"],
    ) == ("credito_rural", "1.0", "1.0", "bcb.credito_rural_registro")


async def test_registro_tem_os_tipos_do_contrato(monkeypatch: pytest.MonkeyPatch):
    servir(monkeypatch, "sicor_investimento_bovinos_mt")
    cheio = await bcb.credito_rural(
        "bovinos", safra="2024/25", finalidade="investimento", uf="MT", agregacao="registro"
    )
    monkeypatch.setattr(
        api.client, "fetch_credito_rural_with_fallback", AsyncMock(return_value=([], "odata"))
    )
    with pytest.warns(UserWarning, match="Nenhum registro"):
        vazio = await bcb.credito_rural("soja", safra="2024/25", agregacao="registro")
    vazio_dataset = await datasets.credito_rural("soja", safra="2024/25", agregacao="registro")

    tipos = {
        contracts.ColumnType.STRING: str(pd.Series(["texto"]).dtype),
        contracts.ColumnType.INTEGER: "Int64",
        contracts.ColumnType.FLOAT: "float64",
    }
    esperado = {coluna.name: tipos[coluna.type] for coluna in REGISTRO.columns}
    assert (len(cheio), len(vazio), len(vazio_dataset)) == (135, 0, 0)
    for frame in (cheio, vazio, vazio_dataset):
        assert {nome: str(tipo) for nome, tipo in frame.dtypes.items()} == esperado
    assert cheio["valor"].notna().all() and cheio["area_financiada"].isna().all()


async def test_registro_as_polars_tem_os_tipos_do_contrato(monkeypatch: pytest.MonkeyPatch):
    pl = pytest.importorskip("polars")
    servir(monkeypatch, "sicor_comercializacao_soja_mt")
    tipos = {
        contracts.ColumnType.STRING: pl.Utf8,
        contracts.ColumnType.INTEGER: pl.Int64,
        contracts.ColumnType.FLOAT: pl.Float64,
    }
    esperado = {coluna.name: tipos[coluna.type] for coluna in REGISTRO.columns}

    for consultar in (bcb.credito_rural, datasets.credito_rural):
        frame = await consultar(
            "soja",
            safra="2024/25",
            finalidade="comercializacao",
            uf="MT",
            agregacao="registro",
            as_polars=True,
        )
        assert isinstance(frame, pl.DataFrame)
        assert (frame.height, dict(frame.schema)) == (67, esperado)
    sem_codigo = [
        dict(
            corpo("sicor_comercializacao_soja_mt")[0],
            cdFonteRecurso=None,
            cdModalidade=None,
            Atividade=None,
        )
    ]
    monkeypatch.setattr(
        api.client,
        "fetch_credito_rural_with_fallback",
        AsyncMock(side_effect=[([], "odata"), ([], "odata"), (sem_codigo, "odata")]),
    )
    for consultar in (bcb.credito_rural, datasets.credito_rural):
        vazio = await consultar("soja", safra="2024/25", agregacao="registro", as_polars=True)
        assert (vazio.height, dict(vazio.schema)) == (0, esperado)
    nulos = await bcb.credito_rural(
        "soja", safra="2024/25", finalidade="comercializacao", agregacao="registro", as_polars=True
    )
    assert nulos.select(["fonte_recurso", "modalidade", "atividade"]).null_count().row(0) == (
        1,
        1,
        1,
    )
    assert dict(nulos.schema) == esperado


@pytest.mark.parametrize("caso_id", ["sicor_custeio_milho_mt", "sicor_investimento_bovinos_mt"])
async def test_registro_somado_e_o_agregado_por_uf(caso_id: str, monkeypatch: pytest.MonkeyPatch):
    produto, finalidade, (qtd, valor, _area) = CASOS[caso_id]
    registros = corpo(caso_id)
    servir(monkeypatch, caso_id)
    argumentos = {"safra": "2024/25", "finalidade": finalidade, "uf": "MT"}

    registro = await bcb.credito_rural(produto, agregacao="registro", **argumentos)
    por_uf = await bcb.credito_rural(produto, agregacao="uf", **argumentos)

    chave = ["safra", "uf", "produto", "finalidade"]
    medidas = ["qtd_contratos", "valor", "area_financiada"]
    somado = registro.groupby(chave, as_index=False)[medidas].sum(min_count=1)
    assert linhas(somado, chave + ["qtd_contratos", "area_financiada"]) == linhas(
        por_uf, chave + ["qtd_contratos", "area_financiada"]
    )
    assert somado["valor"].tolist() == pytest.approx(por_uf["valor"].tolist())
    assert (por_uf["qtd_contratos"].tolist(), por_uf["valor"].tolist()) == (
        [sum(r[qtd] for r in registros)],
        [pytest.approx(math.fsum(r[valor] for r in registros))],
    )


async def test_registro_aplica_os_filtros_de_uf_programa_e_seguro(
    monkeypatch: pytest.MonkeyPatch,
):
    registros = corpo("sicor_custeio_milho_mt")
    goias = [dict(registro, nomeUF="GO") for registro in registros[:5]]
    monkeypatch.setattr(
        api.client,
        "fetch_credito_rural_with_fallback",
        AsyncMock(return_value=(registros + goias, "odata")),
    )

    mato_grosso = await bcb.credito_rural("milho", safra="2024/25", uf="MT", agregacao="registro")
    pronamp = await bcb.credito_rural(
        "milho", safra="2024/25", agregacao="registro", programa="pronamp"
    )
    proagro_mais = await bcb.credito_rural(
        "milho", safra="2024/25", uf="MT", agregacao="registro", tipo_seguro="proagro MAIS"
    )

    assert (len(mato_grosso), set(mato_grosso["uf"])) == (len(registros), {"MT"})
    assert (len(pronamp), set(pronamp["cd_programa"])) == (
        sum(1 for r in registros + goias if r["cdPrograma"] == "0050"),
        {"0050"},
    )
    assert (len(proagro_mais), set(proagro_mais["cd_tipo_seguro"])) == (
        sum(1 for r in registros if r["cdTipoSeguro"] == "2"),
        {"2"},
    )


async def test_codigo_fora_da_tabela_oficial_fica_com_nome_nulo(monkeypatch: pytest.MonkeyPatch):
    registros = corpo("sicor_custeio_milho_mt")
    alterado = dict(registros[0], cdFonteRecurso="0999", cdModalidade="88", Atividade="7")
    monkeypatch.setattr(
        api.client,
        "fetch_credito_rural_with_fallback",
        AsyncMock(return_value=([alterado, *registros[1:]], "odata")),
    )

    frame = await bcb.credito_rural("milho", safra="2024/25", uf="MT", agregacao="registro")

    nomes = ["fonte_recurso", "modalidade", "atividade"]
    fora = frame["cd_fonte_recurso"] == "0999"
    assert linhas(
        frame[fora],
        [
            "cd_fonte_recurso",
            "fonte_recurso",
            "cd_modalidade",
            "modalidade",
            "cd_atividade",
            "atividade",
        ],
    ) == [("0999", None, "88", None, "7", None)]
    assert frame.loc[~fora, nomes].notna().all().all()
    assert set(frame.loc[~fora, "modalidade"]) == {"LAVOURA"}


def test_chave_do_registro_e_o_grao_das_entidades_do_sicor():
    metadata = recurso(DOMINIOS, DOMINIOS_MANIFEST, "odata_metadata.xml").decode("utf-8")
    dimensoes: set[str] = set()
    with collect_failures() as check:
        for finalidade, entidade in client.ENDPOINT_MAP.items():
            with check(entidade):
                tipo = re.search(
                    rf'<EntitySet Name="{entidade}" EntityType="[\w.]*\.(\w+)"', metadata
                )
                assert tipo is not None
                corpo_tipo = re.search(
                    rf'<EntityType Name="{tipo[1]}">(.*?)</EntityType>', metadata, re.S
                )
                assert corpo_tipo is not None
                propriedades = re.findall(r'<Property Name="(\w+)"', corpo_tipo[1])
                assert sorted(client.SELECT_MAP[finalidade]) == sorted(propriedades)
                dimensoes |= {p for p in propriedades if not p.startswith(("Qtd", "Vl", "Area"))}
    assert set(REGISTRO.primary_key) == {
        parser.COLUNAS_MAP[campo] for campo in dimensoes - {"nomeRegiao"}
    } | {"finalidade"}


async def test_registro_e_filtros_sem_bigquery(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(
        client,
        "fetch_credito_rural",
        AsyncMock(side_effect=SourceUnavailableError(source="bcb", last_error="HTTP 503")),
    )
    bigquery = AsyncMock(
        return_value=[
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
    )
    monkeypatch.setattr(bigquery_client, "fetch_credito_rural_bigquery", bigquery)

    with collect_failures() as check:
        for argumentos in [
            {"agregacao": "registro"},
            {"agregacao": "programa"},
            {"programa": "pronaf"},
            {"tipo_seguro": "proagro mais"},
        ]:
            with (
                check(argumentos),
                levanta_exatamente(
                    SourceUnavailableError,
                    match=re.escape(
                        "OData: HTTP 503; sem fallback BigQuery: a tabela da Base dos Dados agrega "
                        "por município e não traz programa"
                    ),
                ),
            ):
                await bcb.credito_rural("soja", safra="2024/25", uf="MT", **argumentos)
    bigquery.assert_not_awaited()
    por_uf = await bcb.credito_rural("soja", safra="2024/25", uf="MT")
    assert linhas(por_uf, ["uf", "valor", "fonte"]) == [("MT", 285431200.0, "bcb_bigquery")]
