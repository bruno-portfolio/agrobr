from __future__ import annotations

import json
import re
from datetime import date

import pandas as pd

from agrobr.bcb import sgs_parser
from agrobr.contracts.bcb_sgs import BCB_SGS_V2
from agrobr.exceptions import ParseError
from tests.helpers import collect_failures, levanta_exatamente, sem_excecao


def encode(records):
    return json.dumps(records, ensure_ascii=False).encode()


VALOR = "Observação SGS inválida na linha 1; campos: ['valor']"
DATA = "Observação SGS inválida na linha 1; campos: ['data']"
FIM = "Observação SGS inválida na linha 1; campos: ['dataFim']"
RECUSAS = [
    *((corpo, "Resposta SGS não é JSON válido") for corpo in [b"not-json", b"<html>error</html>"]),
    *(
        (corpo, "Resposta SGS deve ser uma lista de observações")
        for corpo in [b"{}", b"null", b"1"]
    ),
    *((corpo, "Cada observação SGS deve ser um objeto") for corpo in [b"[null]", b"[1]"]),
    (b'[{"data":"01/01/2024"}]', VALOR),
    (b'[{"valor":"1"}]', DATA),
    (b'[{"data":"01/01/2024","valor":"1","valor":"2"}]', "Objeto JSON SGS contém campo repetido"),
    *(
        (encode([{"data": "01/01/2024", "valor": valor}]), VALOR)
        for valor in [
            "",
            " ",
            "nan",
            "NaN",
            "inf",
            "-Infinity",
            "bad",
            "1,23",
            "1_0",
            " 1",
            True,
            1,
            1.5,
            "1e999",
            "1e-400",
            "1e-999999999999999999999999999999",
        ]
    ),
    *(
        (encode([{"data": data, "valor": "1.2"}]), DATA)
        for data in ["31/02/2024", "29/02/2023", "2024-01-01", "1/01/2024", "", None, 20240101]
    ),
    *(
        (encode([{"data": "19/09/2026", "dataFim": fim, "valor": "0.1056"}]), FIM)
        for fim in ["31/02/2026", "2026-10-19", "19/10/26", "", 20261019]
    ),
    *(
        (
            encode(
                [{"data": "01/01/2024", "valor": "1.2"}, {"data": "01/01/2024", "valor": outro}]
            ),
            "Data duplicada no mesmo corpo SGS, linha 2",
        )
        for outro in ["1.2", "2.4", None]
    ),
]


def test_corpo_invalido_vira_parse_error_com_o_motivo():
    with collect_failures() as check:
        for corpo, motivo in RECUSAS:
            with check(corpo), levanta_exatamente(ParseError, match=re.escape(motivo)):
                sgs_parser.parse_observations(corpo)
        for data, fim in [("01/01/0001", None), ("31/12/9999", None), ("01/01/2024", "31/12/9999")]:
            with check(data):
                with sem_excecao():
                    bloco = sgs_parser.parse_observations(
                        encode([{"data": data, "dataFim": fim, "valor": "1"}])
                    )
                with levanta_exatamente(
                    ParseError, match=re.escape("Data SGS fora do domínio datetime64[ns]")
                ):
                    sgs_parser.build_frame(bloco.records, 1, "dolar_ptax_venda")


def test_data_fim_do_corpo_vira_coluna_opcional_do_contrato():
    with sem_excecao():
        bloco = sgs_parser.parse_observations(
            encode(
                [
                    {"data": "20/09/2026", "dataFim": "20/10/2026", "valor": "0.1371"},
                    {"data": "19/09/2026", "dataFim": None, "valor": "0.1056"},
                ]
            )
        )
    frame = sgs_parser.build_frame(bloco.records, 226, "tr")
    assert bloco.warnings == []
    assert frame.columns.tolist() == ["data", "valor", "codigo", "nome_serie", "data_fim"]
    assert frame["data_fim"].isna().tolist() == [True, False]
    assert frame["data_fim"].iloc[1] == pd.Timestamp("2026-10-20")
    assert BCB_SGS_V2.validate(frame) == (True, [])
    assert BCB_SGS_V2.empty_frame().dtypes.astype(str).to_dict() == {
        "data": "datetime64[ns]",
        "valor": "float64",
        "codigo": "int64",
        "nome_serie": "object",
        "data_fim": "datetime64[ns]",
    }
    frame["data_fim"] = frame["data_fim"].astype("datetime64[us]")
    assert BCB_SGS_V2.validate(frame) == (
        False,
        ["Column 'data_fim' must use datetime64[ns] dtype"],
    )


def test_corpo_valido_preserva_valor_ordem_tipos_e_layout():
    parsed = sgs_parser.parse_observations(
        encode(
            [
                {"data": "01/09/2024", "valor": "0"},
                {"data": "01/08/2024", "valor": "-0.02"},
                {"data": "01/10/2024", "valor": None},
            ]
        )
    )
    assert parsed.records[0].data == date(2024, 9, 1)
    assert (parsed.source_rows, parsed.parser_version, parsed.warnings) == (3, 2, [])
    frame = sgs_parser.build_frame(parsed.records, 433, "ipca")
    assert frame["data"].tolist() == [
        pd.Timestamp("2024-08-01"),
        pd.Timestamp("2024-09-01"),
        pd.Timestamp("2024-10-01"),
    ]
    assert frame["valor"].iloc[:2].tolist() == [-0.02, 0.0]
    assert pd.isna(frame["valor"].iloc[2])
    assert frame["codigo"].eq(433).all() and frame["nome_serie"].eq("ipca").all()
    assert [str(frame[col].dtype) for col in ["data", "valor", "codigo"]] == [
        "datetime64[ns]",
        "float64",
        "int64",
    ]
    anonima = sgs_parser.build_frame(parsed.records, 999999999, None)
    assert anonima["nome_serie"].isna().all()
    vazio = sgs_parser.build_frame(sgs_parser.parse_observations(b"[]").records, 1, "x")
    assert vazio.empty and vazio.columns.tolist() == ["data", "valor", "codigo", "nome_serie"]
    assert [str(vazio[col].dtype) for col in ["data", "valor", "codigo"]] == [
        "datetime64[ns]",
        "float64",
        "int64",
    ]
    plain = sgs_parser.parse_observations(encode([{"data": "01/01/2024", "valor": "1"}]))
    changed = sgs_parser.parse_observations(encode([{"data": "02/01/2024", "valor": "2"}]))
    extra = sgs_parser.parse_observations(
        encode([{"data": "01/01/2024", "valor": "1", "additional": "synthetic"}])
    )
    assert plain.layout_fingerprint == changed.layout_fingerprint != extra.layout_fingerprint
    assert extra.records == plain.records
    assert extra.warnings == ["SGS retornou campos adicionais: ['additional']"]
    assert plain.warnings == []
