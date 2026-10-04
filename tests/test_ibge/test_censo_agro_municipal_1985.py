from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import Mock

import duckdb
import pytest
from typer.testing import CliRunner

from agrobr import datasets, ibge
from agrobr.cli import app
from agrobr.contracts import get_contract
from agrobr.exceptions import InvalidParameterError, ParseError
from agrobr.ibge import censo_municipal_1985 as censo

GOLDEN = Path(__file__).resolve().parents[1] / "golden_data" / "censo_1985_municipal"
REGISTRO_CENSO_AGRO_1985 = (
    "https://biblioteca.ibge.gov.br/index.php/biblioteca-catalogo?view=detalhes&id=747"
)


async def test_parametros_invalidos_recusam_antes_de_ler_o_pacote(monkeypatch):
    consultar = Mock(side_effect=AssertionError("o pacote não pode ser lido"))
    monkeypatch.setattr(censo, "_consultar", consultar)
    casos = [
        ({"tema": "nao_existe"}, "Tema inválido: 'nao_existe'"),
        ({"tema": "efetivo_bovinos", "uf": "XX"}, "UF inválida: 'XX'"),
        ({"tema": "efetivo_bovinos", "nivel": "total"}, "Nível inválido: 'total'"),
    ]
    for kwargs, mensagem in casos:
        with pytest.raises(InvalidParameterError, match=mensagem):
            await ibge.censo_agro_municipal_1985(**kwargs)
    consultar.assert_not_called()


async def test_uf_e_nivel_so_com_espacos_recusam():
    for kwargs, mensagem in (
        ({"uf": "  "}, "UF inválida: '  '"),
        ({"nivel": "  "}, "Nível inválido: '  '"),
    ):
        with pytest.raises(InvalidParameterError, match=mensagem):
            await ibge.censo_agro_municipal_1985("efetivo_bovinos", **kwargs)


def test_tema_de_cada_tabela_segue_o_titulo_impresso():
    tabelas = censo.TABELAS_CENSO_MUNICIPAL_1985
    assert sorted(tabelas) == list(range(67, 120))
    assert set(censo.TITULOS_CENSO_MUNICIPAL_1985) == set(tabelas)
    assert len(set(tabelas.values())) == 53
    assert (tabelas[89], tabelas[90], tabelas[91], tabelas[93]) == (
        "empregados_temporarios",
        "silos_forragens",
        "depositos_producao",
        "meios_transporte",
    )
    assert (tabelas[97], tabelas[98], tabelas[102], tabelas[106]) == (
        "efetivo_bovinos",
        "efetivo_bubalinos",
        "efetivo_suinos",
        "efetivo_aves",
    )


async def test_consulta_de_1_volume_segue_o_contrato_e_aponta_o_pdf():
    df, meta = await ibge.censo_agro_municipal_1985("efetivo_bovinos", uf="ES", return_meta=True)
    valido, erros = get_contract("censo_agropecuario_municipal_1985").validate(df)
    assert valido, erros
    assert set(df["tabela"]) == {97}
    assert set(df["uf"]) == {"ES"}
    assert set(df["tema"]) == {"efetivo_bovinos"}
    confirmadas = df["status"].isin(censo.STATUS_CONFIRMADOS)
    assert confirmadas.any()
    assert df.loc[~confirmadas, "valor"].isna().all()
    assert (df.loc[confirmadas, "valor"] == df.loc[confirmadas, "valor_lido"]).all()
    manifesto = json.loads(censo._MANIFESTO.read_text(encoding="utf-8"))
    es = next(v for v in manifesto["volumes"] if v["volume"] == "n19_es")
    assert meta.source_url == es["url"]
    assert meta.raw_content_hash == es["sha256"]
    assert meta.source_method == "pacote"
    assert meta.from_cache is False
    assert meta.schema_version == "2.0"
    assert meta.fetch_timestamp is not None
    assert meta.source_details["tabela"] == 97
    assert sum(meta.source_details["cobertura_paginas"].values()) > 0


async def test_consulta_de_varios_volumes_usa_o_hash_da_lista_de_pdfs():
    df, meta = await ibge.censo_agro_municipal_1985(
        "propriedade_terras", nivel="uf", return_meta=True
    )
    assert set(df["nivel"]) == {"uf"}
    assert df["volume"].nunique() > 1
    volumes = meta.source_details["volumes"]
    assert {v["volume"] for v in volumes} == set(df["volume"])
    assert meta.source_url == REGISTRO_CENSO_AGRO_1985
    assert meta.raw_content_hash not in {v["sha256"] for v in volumes}


async def test_uf_cujo_volume_nao_tem_a_tabela_recusa_com_o_motivo():
    cobertura = await ibge.cobertura_censo_agro_municipal_1985()
    ausente = sorted({"CE", "MA", "RN"} - set(cobertura["efetivo_coelhos"]))[0]
    with pytest.raises(InvalidParameterError, match="omite a tabela que não se aplica"):
        await ibge.censo_agro_municipal_1985("efetivo_coelhos", uf=ausente)


async def test_tabela_no_volume_sem_casa_lida_levanta_parse_error():
    tema = censo.TABELAS_CENSO_MUNICIPAL_1985[80]
    for uf in ("AM", "AP", "RR"):
        with pytest.raises(
            ParseError, match=f"está no volume de {uf}, mas a extração não leu nenhuma casa"
        ):
            await ibge.censo_agro_municipal_1985(tema, uf=uf)


async def test_nivel_sem_linha_na_uf_devolve_vazio():
    tema = censo.TABELAS_CENSO_MUNICIPAL_1985[80]
    assert not (await ibge.censo_agro_municipal_1985(tema, uf="AC", nivel="uf")).empty
    df = await ibge.censo_agro_municipal_1985(tema, uf="AC", nivel="municipio")
    assert df.empty
    assert "valor_lido" in df.columns


async def test_temas_e_cobertura_vem_do_pacote():
    assert await ibge.temas_censo_agro_municipal_1985() == sorted(censo.TEMAS_CENSO_MUNICIPAL_1985)
    cobertura = await ibge.cobertura_censo_agro_municipal_1985()
    assert set(cobertura) <= set(censo.TEMAS_CENSO_MUNICIPAL_1985)
    assert "SP" in cobertura["efetivo_coelhos"]


def test_chave_do_pacote_e_unica():
    repetidas = duckdb.sql(
        f"SELECT count(*) FROM (SELECT volume, tabela, pagina_pdf, linha, coluna FROM read_parquet('{censo._PACOTE.as_posix()}') "
        "GROUP BY ALL HAVING count(*) > 1)"
    ).fetchone()
    assert repetidas == (0,)


async def test_regressao_contra_os_oraculos_cegos():
    amostra = json.loads((GOLDEN / "oraculo_amostra.json").read_text(encoding="utf-8"))["celulas"]
    assert len(amostra) >= 100
    por_consulta: dict[tuple[str, int], list[dict]] = {}
    for celula in amostra:
        por_consulta.setdefault((celula["uf"], celula["tabela"]), []).append(celula)
    for (uf, tabela), celulas in por_consulta.items():
        df = await ibge.censo_agro_municipal_1985(censo.TABELAS_CENSO_MUNICIPAL_1985[tabela], uf=uf)
        indice = df.set_index(["volume", "pagina_pdf", "linha", "coluna"])
        for celula in celulas:
            casa = indice.loc[
                (celula["volume"], celula["pagina_pdf"], celula["linha"], celula["coluna"])
            ]
            assert casa["status"] in censo.STATUS_CONFIRMADOS, celula
            assert casa["valor"] == celula["valor"], celula


def test_cli_lista_temas_e_exporta_csv():
    temas = CliRunner().invoke(app, ["ibge", "temas-municipal-1985"])
    assert temas.exit_code == 0
    assert "efetivo_bovinos" in temas.output
    csv = CliRunner().invoke(
        app, ["ibge", "censo-municipal-1985", "efetivo_bovinos", "--uf", "ES", "--formato", "csv"]
    )
    assert csv.exit_code == 0
    assert "ano,uf,volume,tabela,tema" in csv.output


async def test_dataset_entrega_o_contrato_2():
    df, meta = await datasets.get_dataset("censo_agropecuario_municipal_1985").fetch(
        "efetivo_bovinos", uf="ES", return_meta=True
    )
    valido, erros = get_contract("censo_agropecuario_municipal_1985").validate(df)
    assert valido, erros
    assert meta.from_cache is False
    assert meta.schema_version == "2.0"


async def test_as_polars_entrega_as_mesmas_casas():
    pl = pytest.importorskip("polars")
    pandas_df = await ibge.censo_agro_municipal_1985("efetivo_bovinos", uf="ES")
    polars_df = await ibge.censo_agro_municipal_1985("efetivo_bovinos", uf="ES", as_polars=True)
    assert isinstance(polars_df, pl.DataFrame)
    assert polars_df.columns == list(pandas_df.columns)
    assert polars_df.height == len(pandas_df)
    assert (
        polars_df["valor"].drop_nulls().to_list()
        == pandas_df["valor"].dropna().astype(int).tolist()
    )


def test_dataset_aponta_o_registro_do_censo_agropecuario_1985_na_biblioteca():
    info = datasets.info("censo_agropecuario_municipal_1985")
    assert info["source_url"] == REGISTRO_CENSO_AGRO_1985
