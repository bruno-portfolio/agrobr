from __future__ import annotations

import csv
import hashlib
import io
import json
from datetime import date

import pandas as pd
import pytest

from agrobr import b3, contracts
from agrobr.exceptions import InvalidParameterError, ParseError
from tests.helpers import (
    assert_replay_served,
    collect_failures,
    install_replay_http,
    levanta_exatamente,
    sem_excecao,
)
from tests.test_b3.test_historico_oraculo import DIAS, GOLDEN, PREGOES, _caso

CADASTRO = GOLDEN / "b3/cadastro_20260922"
MESES = dict(zip("FGHJKMNQUVXZ", range(1, 13), strict=True))
CONTRATOS_OI = {
    "boi": "BGI",
    "milho": "CCM",
    "cafe_arabica": "ICF",
    "etanol": "ETH",
    "soja_cross": "SJC",
}


def _instrumentos() -> dict[str, dict[str, str]]:
    manifest = json.loads((CADASTRO / "manifest.json").read_bytes())
    arquivo = manifest["arquivos"][0]
    corpo = (CADASTRO / arquivo["file"]).read_bytes()
    assert hashlib.sha256(corpo).hexdigest() == arquivo["sha256"]
    return {linha["TckrSymb"]: linha for linha in json.loads(corpo)}


def _mes_do_contrato(instrumento: dict[str, str]) -> tuple[int, int]:
    expira = date.fromisoformat(instrumento["XprtnDt"])
    mes = MESES[instrumento["XprtnCd"][0]]
    return (expira.year if mes >= expira.month else expira.year + 1, mes)


def _posicoes_oficiais(dia: date) -> list[dict[str, str]]:
    texto = (PREGOES / f"b3_oi_{dia:%Y%m%d}_agribusiness.csv").read_text(encoding="utf-8")
    return list(csv.DictReader(io.StringIO(texto), delimiter=";"))


def _inteiro(valor: object) -> int | None:
    return None if pd.isna(valor) else int(valor)


def _publicado(frame: pd.DataFrame) -> list[tuple]:
    return sorted(
        (
            linha.data.date(),
            linha.ticker_completo,
            linha.vencimento_codigo,
            _inteiro(linha.vencimento_ano),
            _inteiro(linha.vencimento_mes),
        )
        for linha in frame.itertuples()
    )


async def test_oi_publica_o_mes_do_contrato_em_futuros_e_opcoes(monkeypatch):
    visto = install_replay_http(monkeypatch, _caso(ajustes=[]), GOLDEN)
    instrumentos = _instrumentos()
    expira_antes = 0

    with collect_failures() as check:
        for contrato, ativo in CONTRATOS_OI.items():
            esperado = []
            for dia in DIAS:
                for linha in _posicoes_oficiais(dia):
                    if linha["Asst"] != ativo:
                        continue
                    instrumento = instrumentos[linha["TckrSymb"]]
                    ano, mes = _mes_do_contrato(instrumento)
                    expira_antes += instrumento["XprtnDt"][:7] != f"{ano}-{mes:02d}"
                    esperado.append((dia, linha["TckrSymb"], linha["XprtnCd"], ano, mes))
            with check(contrato):
                with sem_excecao():
                    frame, meta = await b3.posicoes_abertas_historico(
                        contrato=contrato, inicio=DIAS[0], fim=DIAS[1], return_meta=True
                    )
                assert esperado
                assert _publicado(frame) == sorted(esperado)
                assert meta.parser_version == 2

    assert_replay_served(visto)
    assert expira_antes == 2 * (27 - 4) + 2 * 64


async def test_cadastro_inteiro_decodifica_o_mes_do_contrato(monkeypatch, tmp_path):
    instrumentos = [i for i in _instrumentos().values() if i["Asst"] != "SOY"]
    cabecalho = "RptDt;TckrSymb;ISIN;Asst;XprtnCd;SgmtNm;OpnIntrst;VartnOpnIntrst\n"
    corpo = cabecalho + "".join(
        f"2026-09-22;{i['TckrSymb']};;{i['Asst']};{i['XprtnCd']};AGRIBUSINESS;1;0\n"
        for i in instrumentos
    )
    (tmp_path / "oi.csv").write_text(corpo, encoding="utf-8")
    caso = _caso(ajustes=[], posicoes=[DIAS[1]])
    caso["requests"][-1]["file"] = str(tmp_path / "oi.csv")
    install_replay_http(monkeypatch, caso, GOLDEN)

    with sem_excecao():
        frame = await b3.posicoes_abertas(data=DIAS[1])

    assert len(instrumentos) == 1906
    assert sorted(
        (linha.ticker_completo, _inteiro(linha.vencimento_ano), _inteiro(linha.vencimento_mes))
        for linha in frame.itertuples()
    ) == sorted((i["TckrSymb"], *_mes_do_contrato(i)) for i in instrumentos)


async def test_filtro_de_vencimento_casa_futuro_e_opcao_do_mes(monkeypatch):
    install_replay_http(monkeypatch, _caso(ajustes=[]), GOLDEN)
    instrumentos = _instrumentos()

    def esperado(ativo: str, ano: int, mes: int) -> list[tuple]:
        return sorted(
            (dia, linha["TckrSymb"], linha["XprtnCd"], ano, mes)
            for dia in DIAS
            for linha in _posicoes_oficiais(dia)
            if linha["Asst"] == ativo
            and _mes_do_contrato(instrumentos[linha["TckrSymb"]]) == (ano, mes)
        )

    with collect_failures() as check:
        for contrato, filtro, ativo, ano, mes in (
            ("boi", "V26", "BGI", 2026, 10),
            ("boi", " v26 ", "BGI", 2026, 10),
            ("cafe_arabica", "H27", "ICF", 2027, 3),
            ("soja_cross", "F27", "SJC", 2027, 1),
        ):
            with check(f"{contrato} {filtro!r}"):
                with sem_excecao():
                    frame = await b3.posicoes_abertas_historico(
                        contrato=contrato, inicio=DIAS[0], fim=DIAS[1], vencimento=filtro
                    )
                assert set(frame["tipo"]) == {"futuro", "opcao"}
                assert _publicado(frame) == esperado(ativo, ano, mes)
        with check("boi V26 opções"):
            with sem_excecao():
                opcoes = await b3.posicoes_abertas_historico(
                    contrato="boi", inicio=DIAS[0], fim=DIAS[1], tipo="opcao", vencimento="V26"
                )
            assert opcoes.groupby(opcoes["data"].dt.date)["posicoes_abertas"].agg(
                ["size", "sum"]
            ).to_dict("index") == {
                DIAS[0]: {"size": 77, "sum": 23502},
                DIAS[1]: {"size": 77, "sum": 24123},
            }
        with check("código próprio da opção"):
            with sem_excecao():
                serie = await b3.posicoes_abertas_historico(
                    contrato="boi", inicio=DIAS[0], fim=DIAS[1], vencimento="vvjk"
                )
            assert sorted(serie["ticker_completo"]) == ["BGIV26C030000"] * 2


async def test_filtro_de_vencimento_invalido_recusado_antes_da_rede(monkeypatch):
    visto = install_replay_http(monkeypatch, _caso(ajustes=[]), GOLDEN)

    with collect_failures() as check:
        for filtro in ("V2026", "Z9", "A26", "V2X", "VVJ", "", "outubro"):
            with check(repr(filtro)), levanta_exatamente(InvalidParameterError, match="vencimento"):
                await b3.posicoes_abertas_historico(
                    contrato="boi", inicio=DIAS[0], fim=DIAS[1], vencimento=filtro
                )

    assert visto == {"served": [], "unmatched": []}


async def test_vencimento_fora_do_padrao_vira_erro(tmp_path):
    original = (PREGOES / "b3_oi_20260922_agribusiness.csv").read_text(encoding="utf-8")

    with collect_failures() as check:
        for nome, antes, depois in (
            ("mês da opção diverge do ticker", ";BGI;VVJK;", ";BGI;XVJK;"),
            ("ticker fora do padrão", ";BGIV26C030000;", ";BGIV26X030000;"),
            ("futuro com código de outro mês", ";BGI;V26;", ";BGI;X26;"),
            ("código vazio", ";BGI;F27;", ";BGI;;"),
        ):
            with check(nome), pytest.MonkeyPatch.context() as mp:
                assert original.count(antes) == 1
                (tmp_path / "oi.csv").write_text(original.replace(antes, depois), encoding="utf-8")
                caso = _caso(ajustes=[], posicoes=[DIAS[1]])
                caso["requests"][-1]["file"] = str(tmp_path / "oi.csv")
                install_replay_http(mp, caso, GOLDEN)
                with levanta_exatamente(ParseError, match="vencimento"):
                    await b3.posicoes_abertas(data=DIAS[1], contrato="boi")


def test_contrato_de_posicoes_exige_mes_e_ano():
    contrato = contracts.get_contract("posicoes_abertas")
    colunas = {coluna.name: coluna for coluna in contrato.columns}

    assert contrato.version == "1.1"
    assert not colunas["vencimento_mes"].nullable
    assert not colunas["vencimento_ano"].nullable
