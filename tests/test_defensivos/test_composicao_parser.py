from __future__ import annotations

import csv
import io
from pathlib import Path

import pandas as pd
import pydantic
import pytest

from agrobr.defensivos import models, parser
from agrobr.exceptions import ParseError
from tests import helpers

GOLDEN = Path(__file__).parents[1] / "golden_data" / "defensivos" / "selecao_20260906"


def form_csv(composition="A (Grupo) (10 g/L)", rows=None, extra=()):
    header = ["NR_REGISTRO", "MARCA_COMERCIAL", "INGREDIENTE_ATIVO", "CULTURA", "SITUACAO", *extra]
    rows = (
        rows
        if rows is not None
        else [["001", "Marca", composition, "Soja", "TRUE", *[""] * len(extra)]]
    )
    text = io.StringIO(newline="")
    writer = csv.writer(text, delimiter=";", lineterminator="\n")
    writer.writerow(header)
    writer.writerows(rows)
    return text.getvalue().encode()


def test_tecnicos_layout_legado_grupo_em_coluna_propria():
    raw = (
        "NR_REGISTRO;MARCA_COMERCIAL;INGREDIENTE_ATIVO;TITULAR_DE_REGISTRO;CLASSE;GRUPO_QUIMICI\n"
        "T00001;Glifosato Técnico;glifosato;Empresa;Herbicida;glicina substituída\n"
    ).encode()
    tables, details = parser.parse_tecnicos_bundle(raw)
    produto = tables["tecnicos"].iloc[0]
    componente = tables["composicao"].iloc[0]
    assert (produto.ingrediente_ativo, produto.grupo_quimico) == (
        "glifosato",
        "glicina substituída",
    )
    assert (componente.ingrediente_ativo, componente.grupo_quimico) == (
        "glifosato",
        "glicina substituída",
    )
    assert details["unparsed_count"] == 1


@pytest.mark.parametrize(
    "text,value,unit",
    [
        (".0032 g/L", 0.0032, "g/L"),
        ("2,5 g/kg", 2.5, "g/kg"),
        ("0 mg/mg", 0.0, "mg/mg"),
        ("480", 480.0, None),
        ("2.5e3 UFC/g", 2500.0, "UFC/g"),
        ("1 x10^9 UFC/g", 1e9, "UFC/g"),
        ("20 10^4 parasitoides/ha", 200000.0, "parasitoides/ha"),
        ("2.5 x 10^-3 g/L", 0.0025, "g/L"),
        ("3 % p/v", 3.0, "% p/v"),
    ],
)
def test_concentracoes_explicitas_sinteticas(text, value, unit):
    tables, details = parser.parse_formulados_bundle(form_csv(f"A (Grupo) ({text})"))
    component = tables["composicao"].iloc[0]
    assert component.concentracao_valor == pytest.approx(value)
    assert component.concentracao_unidade == unit
    assert component.concentracao_texto == text
    assert details["unparsed_count"] == 0


def test_nova_coluna_raw_nao_recebe_normalizacao_legada():
    composition = " A \x96 B (Grupo) (3 g/L) "
    tables, _ = parser.parse_formulados_bundle(form_csv(composition))
    product = tables["formulados"].iloc[0]
    assert product.ingrediente_ativo == "A – B (Grupo) (3 g/L)"
    assert product.composicao_texto == composition


@pytest.mark.parametrize(
    "text",
    [
        "200 1x10E10 UFC/g",
        "1 10*10 UFC/g",
        "10-20 g/L",
        "? g/L",
        "1e5000 g/L",
        "1e-5000 g/L",
        "-1 g/L",
    ],
)
def test_concentracao_ambigua_ou_irrepresentavel_preserva_texto(text):
    with helpers.sem_excecao():
        tables, details = parser.parse_formulados_bundle(form_csv(f"A (Grupo) ({text})"))
    component = tables["composicao"].iloc[0]
    assert pd.isna(component.concentracao_valor)
    assert component.concentracao_texto == text
    assert details["unparsed_count"] == 1
    assert details["warnings"]


def test_nr_registro_obrigatorio_header():
    raw = b"MARCA_COMERCIAL;INGREDIENTE_ATIVO;CULTURA\nA;B;C\n"
    with pytest.raises(ParseError, match="NR_REGISTRO"):
        parser.parse_formulados_bundle(raw)


@pytest.mark.parametrize("bundle", [parser.parse_formulados_bundle, parser.parse_tecnicos_bundle])
@pytest.mark.parametrize("raw", [b"", b" \r\n"])
def test_csv_vazio_gera_parseerror(bundle, raw):
    with pytest.raises(ParseError, match="CSV vazio"):
        bundle(raw)


@pytest.mark.parametrize(
    "bundle,raw,erro",
    [
        (
            parser.parse_formulados_bundle,
            b"NR_REGISTRO;NR_REGISTRO;MARCA_COMERCIAL;INGREDIENTE_ATIVO;CULTURA\n1;1;M;A;Soja\n",
            "duplicadas",
        ),
        (
            parser.parse_tecnicos_bundle,
            b"NR_REGISTRO;NUMERO_REGISTRO;CLASSE\n001;002;Herbicida\n",
            "Aliases divergentes",
        ),
        (
            parser.parse_tecnicos_bundle,
            b"NUMERO_REGISTRO;PRODUTO_TECNICO_MARCA_COMERCIAL\n001;Marca\n",
            "Colunas faltando",
        ),
    ],
    ids=["cabecalho_duplicado", "aliases_divergentes", "tecnico_sem_classe"],
)
def test_layout_incoerente_gera_parseerror(bundle, raw, erro):
    with pytest.raises(ParseError, match=erro):
        bundle(raw)


@pytest.mark.parametrize(
    "column,index,new",
    [
        ("marca_comercial", 1, "Outra marca"),
        ("ingrediente_ativo", 2, "B (Grupo) (5 g/L)"),
        ("situacao", 4, "FALSE"),
    ],
)
def test_produto_divergente_no_mesmo_registro_falha(column, index, new):
    first = ["001", "Marca", "A (Grupo) (5 g/L)", "Soja", "TRUE"]
    second = first.copy()
    second[index] = new
    with pytest.raises(ParseError, match=column):
        parser.parse_formulados_bundle(form_csv(rows=[first, second]))


def test_autorizacoes_identicas_apos_projecao_nao_sao_deduplicadas():
    first = ["001", "Marca", "A (Grupo) (5 g/L)", "Soja", "TRUE", "Empresa1"]
    second = [*first[:-1], "Empresa2"]
    tables, details = parser.parse_formulados_bundle(
        form_csv(rows=[first, second], extra=("EMPRESA_PAIS_TIPO",))
    )
    assert len(tables["autorizacoes"]) == 2
    assert len(tables["formulados"]) == len(tables["composicao"]) == 1
    assert tables["autorizacoes"].duplicated().sum() == 1
    assert "EMPRESA_PAIS_TIPO" in details["ignored_columns"]


@pytest.mark.parametrize("register", ["", " ", "001 02", "001\x00", "../"])
def test_registro_invalido_gera_parseerror(register):
    with pytest.raises(ParseError, match="nr_registro"):
        parser.parse_formulados_bundle(form_csv(rows=[[register, "Marca", "A", "Soja", "TRUE"]]))


def test_vazios_tipados_e_colunas_legadas_nullable():
    tables, details = parser.parse_formulados_bundle(form_csv(rows=[]))
    assert list(tables["formulados"].columns) == models.FORMULADOS_PRODUCT_COLS
    assert list(tables["autorizacoes"].columns) == models.AUTORIZACOES_COLS
    assert str(tables["composicao"].ordem_componente.dtype) == "Int64"
    assert str(tables["composicao"].concentracao_valor.dtype) == "Float64"
    assert all(table.empty for table in tables.values())
    assert details["source_rows"] == 0
    nonempty, _ = parser.parse_formulados_bundle(form_csv())
    assert nonempty["formulados"].formulacao.isna().all()
    assert nonempty["autorizacoes"].modalidade_de_emprego.isna().all()


def test_fingerprint_depende_header_e_nao_valores():
    _, first = parser.parse_formulados_bundle(form_csv())
    _, second = parser.parse_formulados_bundle(form_csv("B (Outro) (2 g/kg)"))
    _, changed = parser.parse_formulados_bundle(form_csv(extra=("NOVA_COLUNA",)))
    assert first["layout_fingerprint"] == second["layout_fingerprint"]
    assert first["layout_fingerprint"]["sha256"] != changed["layout_fingerprint"]["sha256"]
    assert first["layout_fingerprint"]["parser_version"] == 3


@pytest.mark.parametrize(
    "field,value",
    [
        ("ordem_componente", True),
        ("ordem_componente", 0),
        ("concentracao_valor", float("inf")),
        ("concentracao_valor", float("nan")),
        ("concentracao_valor", -1.0),
        ("componente_texto", ""),
        ("componente_texto", "  "),
        ("nr_registro", True),
        ("tipo", "outro"),
    ],
)
def test_modelo_componentes_rejeita_campos_invalidos(field, value):
    record = {
        "nr_registro": "001",
        "tipo": "formulados",
        "ordem_componente": 1,
        "componente_texto": "A",
        "concentracao_valor": None,
    }
    record[field] = value
    with pytest.raises(pydantic.ValidationError):
        models.AgrofitComponente.model_validate(record)
