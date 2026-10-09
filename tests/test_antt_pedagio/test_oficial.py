from __future__ import annotations

import asyncio
import csv
import io
import threading
import warnings

import pandas as pd
import pytest

from agrobr import contracts
from agrobr.alt.antt_pedagio import api, parser
from agrobr.exceptions import ParseError
from tests.helpers import conferir_corpo, install_anttpedagio_source
from tests.test_antt_pedagio import oficial


async def fetch(monkeypatch, files, plazas=None, **query):
    install_anttpedagio_source(monkeypatch, files, plazas=plazas)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        try:
            frame, meta = await api.fluxo_pedagio(return_meta=True, **query)
        except Exception as exc:
            frame, meta = exc, None
    assert not isinstance(frame, Exception), frame
    return frame, meta, [str(item.message) for item in caught]


def warning(messages, fragment):
    matching = [text for text in messages if fragment in text]
    assert len(matching) == 1, messages
    return matching[0]


@pytest.mark.parametrize("rodovia", ["BR 40", "br-040", "BR-40", "br-40"])
async def test_fluxo_rodovia_normalizada_preserva_linhas_publicadas(monkeypatch, rodovia):
    frame, _, _ = await fetch(
        monkeypatch,
        {"volume-2023.csv": oficial.load("mensal_2023.csv")},
        plazas=oficial.load("pracas.csv"),
        ano=2023,
        rodovia=rodovia,
        uf="rj",
        concessionaria="CONCER",
        tipo_veiculo="Passeio",
        inicio="2023-11-01",
        fim="2023-11-01",
    )
    assert frame["rodovia"].tolist() == ["BR-40"] * 3
    assert frame["praca"].tolist() == ["P1"] * 3
    assert dict(zip(frame["categoria_eixo"], frame["volume"], strict=True)) == {
        "Categoria 1": 269173,
        "Categoria 3": 694,
        "Categoria 5": 104,
    }


@pytest.mark.parametrize(
    "name,labels,year",
    [
        (
            "mensal_2023.csv",
            (
                "CONCER dia 2",
                "CONCER mês anterior",
                "CRO fim de mês",
                "ECOVIAS DO ARAGUAIA diário",
            ),
            2023,
        ),
        ("mensal_2021.csv", ("ECOPONTE série de preenchimento", "ECOSUL nov Cat. 1"), 2021),
    ],
)
async def test_referencia_mensal_fora_do_dia_1_vira_o_mes_com_aviso(
    monkeypatch, name, labels, year
):
    body = oficial.subset(name, *labels)
    frame, meta, messages = await fetch(
        monkeypatch, {f"volume-{year}.csv": body}, ano=year, enriquecer=False
    )
    assert oficial.published(frame) == oficial.oracle(body)
    assert frame["data"].dt.day.eq(1).all()
    anomalous = [
        {
            "record": line - 1,
            "line": line,
            "value": row["mes_ano"],
            "concessionaria": row["concessionaria"],
        }
        for line, row in enumerate(oficial.rows(body), 2)
        if not row["mes_ano"].startswith("01/")
    ]
    parsing = meta.source_details["parsing"][0]
    months = sorted(key[0] for key in oficial.oracle(body))
    assert (parsing["source_date_min"], parsing["source_date_max"]) == (
        months[0].isoformat(),
        months[-1].isoformat(),
    )
    assert parsing["source_zero_volume_rows"] == sum(
        row["volume_total"] in ("0", "0,00") for row in oficial.rows(body)
    )
    assert parsing["normalized_references"] == len(anomalous) > 0
    assert parsing["normalized_references_examples"] == anomalous[:10]
    first = anomalous[0]
    message = warning(messages, "fora do dia 1")
    assert f"{len(anomalous)} registros" in message
    assert f"linha {first['line']} '{first['value']}' {first['concessionaria']}" in message
    assert message in meta.validation_warnings


@pytest.mark.parametrize(
    "name,labels,year",
    [
        (
            "mensal_2013.csv",
            ("AUTOPISTA FLUMINENSE fracionário", "AUTOPISTA FLUMINENSE nov"),
            2013,
        ),
        ("mensal_2015.csv", ("CONCEBRA negativo", "CONCEBRA P6 dez"), 2015),
        ("mensal_2023.csv", ("RIOSP fracionário", "RIOSP inteiro"), 2023),
    ],
)
async def test_volume_que_nao_e_contagem_sai_com_aviso_e_o_ano_segue(
    monkeypatch, name, labels, year
):
    body = oficial.subset(name, *labels)
    frame, meta, messages = await fetch(
        monkeypatch, {f"volume-{year}.csv": body}, ano=year, enriquecer=False
    )
    assert not frame.empty
    assert oficial.published(frame) == oficial.oracle(body)
    excluded = [
        {
            "record": line - 1,
            "line": line,
            "value": row["volume_total"],
            "concessionaria": row["concessionaria"],
        }
        for line, row in enumerate(oficial.rows(body), 2)
        if not oficial.countable(row["volume_total"])
    ]
    parsing = meta.source_details["parsing"][0]
    assert parsing["excluded_volumes"] == len(excluded) > 0
    assert parsing["excluded_volumes_examples"] == excluded[:10]
    assert parsing["validated_rows"] == len(oficial.rows(body)) - len(excluded)
    first = excluded[0]
    message = warning(messages, "não é contagem")
    assert f"{len(excluded)} registros" in message
    assert f"linha {first['line']} '{first['value']}' {first['concessionaria']}" in message
    assert message in meta.validation_warnings


async def test_bloco_publicado_duas_vezes_mantem_uma_copia(monkeypatch):
    body = oficial.subset(
        "mensal_2021.csv",
        "ECOSUL dez bloco duplicado",
        "ECOSUL nov Cat. 1",
        "CONCEBRA dez linhas diárias no dia 1",
    )
    frame, meta, messages = await fetch(
        monkeypatch, {"volume-2021.csv": body}, ano=2021, enriquecer=False
    )
    assert oficial.published(frame) == oficial.oracle(body)
    ecosul = frame[frame["concessionaria"].eq("ECOSUL") & frame["categoria_eixo"].eq("Categoria 1")]
    december = ecosul[ecosul["data"].dt.month.eq(12)]
    november = ecosul[ecosul["data"].dt.month.eq(11)]
    plaza = december["praca"].eq("Praça Capão Seco") & december["sentido"].eq("CRESCENTE")
    assert december.loc[plaza, "volume"].tolist() == [115974]
    assert 1.0 < december["volume"].sum() / november["volume"].sum() < 1.3
    source = oficial.rows(body)
    concebra = [
        int(row["volume_total"].split(",")[0])
        for row in source
        if row["concessionaria"] == "CONCEBRA"
    ]
    assert len(concebra) == 4
    assert frame.loc[frame["concessionaria"].eq("CONCEBRA"), "volume"].tolist() == [sum(concebra)]
    lines = [
        line
        for line, row in enumerate(source, 2)
        if row["concessionaria"] == "ECOSUL" and row["mes_ano"].endswith("/12/2021")
    ]
    parsing = meta.source_details["parsing"][0]
    assert parsing["duplicated_blocks"] == [
        {
            "concessionaria": "ECOSUL",
            "mes": "2021-12",
            "rows": 200,
            "first_line": lines[0],
            "last_line": lines[-1],
        }
    ]
    assert parsing["duplicate_rows_removed"] == 100
    assert "parsing.duplicated_blocks" in meta.source_details["coverage"]["aggregation"]
    assert (
        parsing["duplicate_volume_removed"]
        == sum(
            int(row["volume_total"].split(",")[0])
            for row in source
            if row["concessionaria"] == "ECOSUL" and row["mes_ano"].endswith("/12/2021")
        )
        // 2
    )
    message = warning(messages, "publicado em duplicata")
    assert f"ECOSUL 2021-12, linhas {lines[0]}–{lines[-1]}" in message
    assert message in meta.validation_warnings


@pytest.mark.parametrize(
    "query,plazas",
    [
        ({"uf": "RS", "rodovia": "BR-290"}, {"P2-Santo Antônio da Patrulha", "P3-Gravatai"}),
        (
            {"uf": "rs"},
            {"P1-Três Cachoeiras", "P2-Santo Antônio da Patrulha", "P3-Gravatai", "P4-Montenegro"},
        ),
    ],
)
async def test_filtro_geografico_exclui_registro_sem_vinculo_com_aviso(monkeypatch, query, plazas):
    body = oficial.load("mensal_2026.csv")
    registry = {
        (row["concessionaria"], row["praca_de_pedagio"]): row
        for row in csv.DictReader(
            io.StringIO(oficial.load("pracas.csv").decode("cp1252"), newline=""), delimiter=";"
        )
    }
    frame, meta, messages = await fetch(
        monkeypatch,
        {"volume-2026_mensal.csv": body},
        plazas=oficial.load("pracas.csv"),
        ano=2026,
        **query,
    )
    source = oficial.rows(body)
    selected = [
        row
        for row in source
        if (place := registry.get((row["concessionaria"], row["praca"]))) is not None
        and place["uf"] == query["uf"].upper()
        and place["rodovia"] == query.get("rodovia", place["rodovia"])
    ]
    unlinked = [row for row in source if (row["concessionaria"], row["praca"]) not in registry]
    assert set(frame["praca"]) == plazas == {row["praca"] for row in selected}
    assert frame["volume"].sum() == sum(int(row["volume_total"].split(",")[0]) for row in selected)
    assert frame["uf"].eq("RS").all()
    geographic = meta.source_details["geographic_filter"]
    assert geographic["unlinked_rows_excluded"] == len(unlinked) == 4
    assert geographic["unlinked_volume_excluded"] == sum(
        int(row["volume_total"].split(",")[0]) for row in unlinked
    )
    assert geographic["unlinked_pairs"] == [
        list(pair) for pair in sorted({(row["concessionaria"], row["praca"]) for row in unlinked})
    ]
    message = warning(messages, "sem vínculo")
    assert "4 registros" in message and "2 pares" in message
    assert message in meta.validation_warnings


@pytest.mark.parametrize(
    "query",
    [{}, {"uf": " rs "}, {"rodovia": "br-290"}, {"situacao": "ATIV"}, {"situacao": "inativ"}],
)
async def test_cadastro_real_todas_as_celulas_e_filtros(monkeypatch, query):
    body = oficial.load("pracas.csv")
    install_anttpedagio_source(monkeypatch, {}, plazas=body)
    frame, meta = await api.pracas_pedagio(return_meta=True, **query)
    expected = [
        row
        for row in oficial.plazas(body)
        if ("uf" not in query or row["uf"] == query["uf"].strip().upper())
        and ("rodovia" not in query or str(row["rodovia"]).upper() == query["rodovia"].upper())
        and (
            "situacao" not in query
            or query["situacao"].casefold() in str(row["situacao"]).casefold()
        )
    ]
    assert oficial.records(frame) == expected
    assert len(expected) == {(): 277, ("uf",): 12, ("rodovia",): 2}.get(tuple(query), len(expected))
    assert meta.records_count == len(expected)
    conferir_corpo(meta, body)
    assert meta.source_url == "https://dados.antt.gov.br/pracas.csv"


async def test_data_de_inativacao_inexistente_vira_nat_e_texto_sem_data_e_layout(monkeypatch):
    body = oficial.load("pracas.csv")
    install_anttpedagio_source(
        monkeypatch, {}, plazas=body.replace(b";Ativo;;", b";Ativo;31/02/2024;", 1)
    )
    with pytest.warns(UserWarning, match="data_da_inativacao viraram NaT"):
        frame, meta = await api.pracas_pedagio(return_meta=True)
    assert len(frame) == 277 and frame["data_da_inativacao"].isna().all()
    assert any("data_da_inativacao" in aviso for aviso in meta.validation_warnings)
    install_anttpedagio_source(
        monkeypatch, {}, plazas=body.replace(b";Ativo;;", b";Ativo;texto-sem-data;", 1)
    )
    with pytest.raises(ParseError, match="fora de DD/MM/AAAA") as erro:
        await api.pracas_pedagio()
    assert erro.value.errors == [("data_da_inativacao", "DD/MM/AAAA", "1")]


async def test_cadastro_real_cheio_e_vazio_com_os_mesmos_dtypes(monkeypatch):
    install_anttpedagio_source(monkeypatch, {}, plazas=oficial.load("pracas.csv"))
    cheio = await api.pracas_pedagio()
    with pytest.warns(UserWarning, match="nenhuma praça"):
        vazio = await api.pracas_pedagio(uf="AC")
    assert len(cheio) == 277 and vazio.empty
    assert vazio.dtypes.to_dict() == cheio.dtypes.to_dict()
    assert cheio.dtypes["concessionaria"] == pd.Series([""]).dtype
    assert str(cheio.dtypes["km_m"]) == "float64"
    assert str(cheio.dtypes["ano_do_pnv_snv"]) == "Int64"
    assert str(cheio.dtypes["data_da_inativacao"]) == "datetime64[ns]"
    assert cheio["municipal"].tolist() == cheio["municipio"].tolist()
    assert contracts.get_contract("antt_pedagio_pracas").validate(cheio) == (True, [])


async def test_fluxo_real_cheio_e_vazio_mantem_string_python(monkeypatch):
    body = oficial.load("mensal_2026.csv")
    files = {"volume-2026_mensal.csv": body}
    cheio, _, _ = await fetch(monkeypatch, files, ano=2026, enriquecer=False)
    vazio, _, _ = await fetch(monkeypatch, files, ano=2026, enriquecer=False, concessionaria="xx")
    assert len(cheio) and vazio.empty
    assert vazio.dtypes.to_dict() == cheio.dtypes.to_dict()
    assert cheio.dtypes["praca"] == pd.StringDtype(storage="python")


async def test_enriquecimento_real_vincula_so_pares_unicos(monkeypatch):
    body, plazas = oficial.load("mensal_2026.csv"), oficial.load("pracas.csv")
    registry = {
        (row["concessionaria"], row["praca_de_pedagio"]): (
            row["rodovia"],
            row["uf"],
            row["municipio"],
        )
        for row in oficial.plazas(plazas)
    }
    frame, meta, _ = await fetch(
        monkeypatch, {"volume-2026_mensal.csv": body}, plazas=plazas, ano=2026
    )
    linked = 0
    for row in oficial.records(frame):
        expected = registry.get((row["concessionaria"], row["praca"]), (None, None, None))
        assert (row["rodovia"], row["uf"], row["municipio"]) == expected, row
        linked += expected != (None, None, None)
    enrichment = meta.source_details["enrichment"]
    assert (enrichment["linked_rows"], enrichment["unlinked_rows"]) == (linked, len(frame) - linked)
    assert 0 < linked < len(frame)


async def test_parse_do_arquivo_roda_fora_do_loop(monkeypatch):
    threads: list[int] = []
    original = parser.parse_trafego_file
    monkeypatch.setattr(
        parser,
        "parse_trafego_file",
        lambda *a, **k: threads.append(threading.get_ident()) or original(*a, **k),
    )

    frame, _, _ = await fetch(
        monkeypatch,
        {"volume-2023.csv": oficial.load("mensal_2023.csv")},
        plazas=oficial.load("pracas.csv"),
        ano=2023,
    )

    assert len(frame) > 0
    assert len(threads) == 1
    assert threading.get_ident() not in threads


async def test_cancelar_o_parse_nao_fecha_o_arquivo_com_a_thread_lendo(monkeypatch):
    loop = asyncio.get_running_loop()
    comecou = asyncio.Event()
    liberar = threading.Event()
    terminou = threading.Event()
    fechado_durante: list[bool] = []
    original = parser.parse_trafego_file

    def parse(arquivo, *args, **kwargs):
        loop.call_soon_threadsafe(comecou.set)
        liberar.wait(5)
        fechado_durante.append(arquivo.closed)
        try:
            return original(arquivo, *args, **kwargs)
        finally:
            terminou.set()

    monkeypatch.setattr(parser, "parse_trafego_file", parse)
    tarefa = asyncio.create_task(
        fetch(
            monkeypatch,
            {"volume-2023.csv": oficial.load("mensal_2023.csv")},
            plazas=oficial.load("pracas.csv"),
            ano=2023,
        )
    )
    await asyncio.wait_for(comecou.wait(), 3)
    tarefa.cancel()
    await asyncio.sleep(0.1)
    liberar.set()

    with pytest.raises(asyncio.CancelledError):
        await tarefa
    assert await asyncio.to_thread(terminou.wait, 5)
    assert fechado_durante == [False]
