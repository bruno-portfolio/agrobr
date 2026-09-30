from __future__ import annotations

import json
import warnings
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from agrobr.acervo_fundiario import parser as acervo_parser
from agrobr.cftc import parser as cftc_parser
from agrobr.exceptions import ParseError
from agrobr.inmet import parser as inmet_parser
from agrobr.mapbiomas_alerta import parser as alerta_parser
from agrobr.normalize.dates import (
    MESES_PT,
    converter_coluna,
    converter_datas,
    lista_safras,
    month_to_number,
    normalizar_safra,
    periodo_safra,
    safra_anterior,
    safra_atual,
    safra_para_anos,
    safra_posterior,
    validar_safra,
)
from agrobr.queimadas import parser as queimadas_parser
from agrobr.utils.result import build_source_meta
from tests.helpers import capturar_logs, collect_failures, levanta_exatamente

GOLDEN = Path(__file__).parents[1] / "golden_data"
FORA = "data ilegível ou com ano fora de 1900–2099"


class TestSafraAtual:
    def test_julho_inicio_safra(self):
        with collect_failures() as check:
            for case, value, expected in [
                ("test_segundo_semestre", date(2024, 10, 15), "2024/25"),
                ("test_primeiro_semestre", date(2025, 3, 15), "2024/25"),
                ("test_julho_inicio_safra", date(2024, 7, 1), "2024/25"),
                ("test_junho_safra_anterior", date(2024, 6, 30), "2023/24"),
            ]:
                with check(case):
                    assert safra_atual(value) == expected
            with check("test_none_uses_today"):
                result = safra_atual()
                assert len(result) == 7
                assert "/" in result


class TestValidarSafra:
    def test_apenas_ano(self):
        with collect_failures() as check:
            for case, value, expected in [
                ("test_formato_padrao", "2024/25", True),
                ("test_formato_curto", "24/25", True),
                ("test_formato_completo", "2024/2025", True),
                ("test_invalido", "abc", False),
                ("test_vazio", "", False),
                ("test_apenas_ano", "2024", False),
            ]:
                with check(case):
                    assert validar_safra(value) is expected


class TestNormalizarSafra:
    def test_completa(self):
        with collect_failures() as check:
            for case, value, expected in [
                ("test_padrao_retorna_igual", "2024/25", "2024/25"),
                ("test_curta", "24/25", "2024/25"),
                ("test_completa", "2024/2025", "2024/25"),
                ("test_curta_seculo_anterior", "99/00", "1999/00"),
                ("test_espacos_normalizados", " 2024 / 25 ", "2024/25"),
            ]:
                with check(case):
                    assert normalizar_safra(value) == expected

    def test_invalido_raises(self):
        with collect_failures() as check:
            with check("test_invalido_raises"), pytest.raises(ValueError, match="inválido"):
                normalizar_safra("abc")
            with check("test_vazio_raises"), pytest.raises(ValueError):
                normalizar_safra("")


class TestSafraParaAnos:
    def test_curta(self):
        with collect_failures() as check:
            for case, value, expected in [
                ("test_padrao", "2024/25", (2024, 2025)),
                ("test_curta", "24/25", (2024, 2025)),
                ("test_virada_seculo", "1999/00", (1999, 2000)),
            ]:
                with check(case):
                    assert safra_para_anos(value) == expected


class TestSafraAnterior:
    def test_tres_safras(self):
        with collect_failures() as check:
            with check("test_uma_safra"):
                assert safra_anterior("2024/25") == "2023/24"
            with check("test_tres_safras"):
                assert safra_anterior("2024/25", 3) == "2021/22"


class TestSafraPosterior:
    def test_uma_safra(self):
        assert safra_posterior("2024/25") == "2025/26"


class TestListaSafras:
    def test_mesma_safra(self):
        with collect_failures() as check:
            with check("test_range_5_safras"):
                result = lista_safras("2020/21", "2024/25")

                assert len(result) == 5
                assert result[0] == "2020/21"
                assert result[-1] == "2024/25"
            with check("test_mesma_safra"):
                assert lista_safras("2024/25", "2024/25") == ["2024/25"]


class TestPeriodoSafra:
    def test_periodo(self):
        inicio, fim = periodo_safra("2024/25")

        assert inicio == date(2024, 7, 1)
        assert fim == date(2025, 6, 30)


class TestMesesPt:
    def test_all_12_months_full(self):
        full_names = [
            "janeiro",
            "fevereiro",
            "março",
            "abril",
            "maio",
            "junho",
            "julho",
            "agosto",
            "setembro",
            "outubro",
            "novembro",
            "dezembro",
        ]
        for i, name in enumerate(full_names, 1):
            assert MESES_PT[name] == i

    def test_all_12_months_abbrev(self):
        abbrevs = [
            "jan",
            "fev",
            "mar",
            "abr",
            "mai",
            "jun",
            "jul",
            "ago",
            "set",
            "out",
            "nov",
            "dez",
        ]
        for i, name in enumerate(abbrevs, 1):
            assert MESES_PT[name] == i

    def test_marco_sem_acento(self):
        assert MESES_PT["marco"] == 3


class TestMonthToNumber:
    def test_abbreviation(self):
        with collect_failures() as check:
            for case, value, expected in [
                ("test_full_name", "janeiro", 1),
                ("test_abbreviation", "jan", 1),
                ("test_whitespace", "  março  ", 3),
                ("test_accented", "março", 3),
                ("test_unaccented_variant", "marco", 3),
            ]:
                with check(case):
                    assert month_to_number(value) == expected
            with check("test_case_insensitive"):
                assert month_to_number("JANEIRO") == 1
                assert month_to_number("JAN") == 1
            with check("test_unknown_returns_none"):
                assert month_to_number("xyz") is None


def _avisos(logs: list[dict[str, Any]]) -> list[tuple[str, str, int]]:
    return [
        (e["fonte"], e["coluna"], e["descartadas"])
        for e in logs
        if e["event"] == "datas_descartadas"
    ]


def _sem_nulo(serie: pd.Series) -> list[object]:
    return [None if pd.isna(valor) else valor for valor in serie]


def _no_meta(df: pd.DataFrame) -> list[str]:
    return build_source_meta("teste", "url", "metodo", 0, 0, df, 1).validation_warnings


class TestConverterDatas:
    def test_intervalo_unidade_e_aviso(self):
        valores = pd.Series(
            [
                "1899-12-31 23:59:59",
                "1900-01-01 00:00:00",
                "2099-12-31 23:59:59",
                "2100-01-01 00:00:00",
                "1667-05-31 11:10:00",
                "2925-09-25 15:10:00",
                "31/12/2020 00:00:00",
                "",
                "  ",
                None,
            ],
            name="data_teste",
        )
        with collect_failures() as check:
            with check("intervalo"):
                with capturar_logs() as logs:
                    datas, descartadas = converter_datas(
                        valores, fonte="teste", formato="%Y-%m-%d %H:%M:%S"
                    )
                assert descartadas == 5
                assert str(datas.dtype) == "datetime64[ns]"
                assert _sem_nulo(datas) == [
                    None,
                    pd.Timestamp("1900-01-01"),
                    pd.Timestamp("2099-12-31 23:59:59"),
                    *[None] * 7,
                ]
                assert [e["intervalo"] for e in logs] == ["1900-2099"]
                assert _avisos(logs) == [("teste", "data_teste", 5)]
            with check("sem_descarte_sem_aviso"):
                with capturar_logs() as logs:
                    datas, descartadas = converter_datas(
                        pd.Series(["2024-01-02", "", None]), fonte="teste"
                    )
                assert descartadas == 0
                assert _sem_nulo(datas) == [pd.Timestamp("2024-01-02"), None, None]
                assert logs == []
            for caso, valores_caso, opcoes, esperado, tipo in [
                ("formato", ["01/02/2024"], {"formato": "%d/%m/%Y"}, "2024-02-01", "ns"),
                ("dayfirst", ["01/02/2024"], {"dayfirst": True}, "2024-02-01", "ns"),
                ("fuso", ["2024-01-02T10:00:00Z", "1667-01-02T10:00:00Z"], {}, None, "ns, UTC"),
                ("vazia", [None, None], {}, None, "ns"),
            ]:
                with check(caso):
                    datas, _ = converter_datas(pd.Series(valores_caso), fonte="teste", **opcoes)
                    assert str(datas.dtype) == f"datetime64[{tipo}]"
                    if esperado is not None:
                        assert datas.tolist() == [pd.Timestamp(esperado)]
            with check("fuso_fora_do_intervalo"):
                datas, _ = converter_datas(
                    pd.Series(["2024-01-02T10:00:00Z", "1667-01-02T10:00:00Z"]), fonte="teste"
                )
                assert _sem_nulo(datas) == [pd.Timestamp("2024-01-02T10:00:00Z"), None]

    def test_fontes_descartam_data_fora_do_intervalo(self, monkeypatch):
        with collect_failures() as check:
            with check("inmet"), capturar_logs() as logs:
                dados = json.loads((GOLDEN / "inmet/observacoes_sample/response.json").read_bytes())
                dados[0]["DT_MEDICAO"] = "1667-01-01"
                df = inmet_parser.parse_observacoes(dados)
                assert len(df) == len(dados) - 1
                assert str(df["data"].dtype) == "datetime64[ns]"
                assert _avisos(logs) == [("inmet", "data", 1)]
                assert _no_meta(df) == [
                    f"inmet: 1 valor(es) de data viraram NaT ({FORA}). "
                    "As observações sem data saíram do resultado."
                ]
            with check("mapbiomas_alerta"), capturar_logs() as logs:
                bruto = (GOLDEN / "mapbiomas_alerta/alertas_sample/response.json").read_bytes()
                registros = json.loads(bruto)
                registros[0]["detectedAt"] = "1667-06-15"
                registros[1]["publishedAt"] = "2925-06-21"
                df = alerta_parser.parse_alertas(registros)
                assert _sem_nulo(df["data_deteccao"])[:2] == [None, pd.Timestamp("2024-06-16")]
                assert _sem_nulo(df["data_publicacao"])[:2] == [pd.Timestamp("2024-06-20"), None]
                assert [str(df[c].dtype) for c in ("data_deteccao", "data_publicacao")] == [
                    "datetime64[ns]",
                    "datetime64[ns]",
                ]
                assert _avisos(logs) == [
                    ("mapbiomas_alerta", "data_deteccao", 1),
                    ("mapbiomas_alerta", "data_publicacao", 1),
                ]
                assert _no_meta(df) == [
                    f"mapbiomas_alerta: 1 valor(es) de data_deteccao viraram NaT ({FORA}).",
                    f"mapbiomas_alerta: 1 valor(es) de data_publicacao viraram NaT ({FORA}).",
                ]
            with check("queimadas"), capturar_logs() as logs:
                corpo = (GOLDEN / "queimadas/focos_sample/response.csv").read_bytes()
                assert corpo.count(b",2025-01-01 00:00:00,") > 1
                df = queimadas_parser.parse_focos_csv(
                    corpo.replace(b",2025-01-01 00:00:00,", b",1667-01-01 00:00:00,", 1)
                )
                assert int(df["data"].isna().sum()) == 1
                assert _avisos(logs) == [("queimadas", "data_hora_gmt", 1)]
                assert _no_meta(df) == [
                    f"queimadas: 1 valor(es) de data_hora_gmt viraram NaT ({FORA})."
                ]
            with check("cftc"), capturar_logs() as logs:
                registros = json.loads(
                    (GOLDEN / "cftc/spreads_20260925/soja_recent.json").read_bytes()
                )
                assert str(cftc_parser.parse_cot(registros)["data"].dtype) == "datetime64[ns]"
                registros[0]["report_date_as_yyyy_mm_dd"] = "1667-09-01T00:00:00.000"
                with levanta_exatamente(ParseError, match="Datas inválidas"):
                    cftc_parser.parse_cot(registros)
                assert _avisos(logs) == [("cftc", "data", 1)]
            with check("acervo_fundiario"), capturar_logs() as logs:
                zip_path = GOLDEN / "acervo_fundiario/assentamentos_20260922/response.zip"
                tabela = acervo_parser._read_tabular(zip_path)
                tabela.loc[0, "data_de_cr"] = "31/05/1667"
                monkeypatch.setattr(acervo_parser, "_read_tabular", lambda *_a, **_k: tabela)
                df = acervo_parser.parse_assentamentos(zip_path)
                assert pd.isna(df["data_criacao"].iloc[0])
                assert df["data_criacao"].iloc[1:].notna().all()
                assert [str(df[c].dtype) for c in ("data_criacao", "data_obtencao")] == [
                    "datetime64[ns]",
                    "datetime64[ns]",
                ]
                assert _avisos(logs) == [("acervo_fundiario", "data_criacao", 1)]
                assert _no_meta(df) == [
                    f"acervo_fundiario: 1 valor(es) de data_criacao viraram NaT ({FORA})."
                ]

    def test_coluna_avisa_e_leva_o_descarte_ao_meta(self):
        df = pd.DataFrame(
            {
                "data": [
                    "2024-01-02 00:00:00",
                    "1667-01-02 00:00:00",
                    "2024-09-23 10:00:00",
                    "2024-09-24 00:00:00",
                    "",
                ]
            }
        )
        aviso = (
            "teste: 2 valor(es) de data viraram NaT (data ilegível ou com ano fora de "
            "1900–2099 ou de dia posterior a 2024-09-23). Sai do resultado."
        )
        with warnings.catch_warnings(record=True) as emitidos:
            warnings.simplefilter("always")
            converter_coluna(
                df,
                "data",
                fonte="teste",
                formato="%Y-%m-%d %H:%M:%S",
                ate=pd.Timestamp("2024-09-23 00:54:47"),
                efeito=" Sai do resultado.",
            )
        limpo = pd.DataFrame({"data": ["2024-01-02"]})
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            converter_coluna(limpo, "data", fonte="teste")

        assert _sem_nulo(df["data"]) == [
            pd.Timestamp("2024-01-02"),
            None,
            pd.Timestamp("2024-09-23 10:00:00"),
            None,
            None,
        ]
        assert [str(w.message) for w in emitidos] == [aviso]
        assert [w.category for w in emitidos] == [UserWarning]
        assert _no_meta(df) == [aviso]
        assert _no_meta(limpo) == []
