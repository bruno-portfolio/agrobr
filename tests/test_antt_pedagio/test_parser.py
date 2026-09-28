from __future__ import annotations

import io

import pandas as pd
import pytest

from agrobr.alt.antt_pedagio import parser
from agrobr.exceptions import ParseError

V2_CSV_NO_HEADER = (
    "EcoRodovias;Anchieta;01/2024;4;Automatica;Crescente;30000\n"
    "EcoRodovias;Anchieta;01/2024;4;Manual;Crescente;2000\n"
    "EcoRodovias;Anchieta;01/2024;6;Automatica;Decrescente;15000\n"
    "EcoRodovias;Anchieta;02/2024;4;Automatica;Crescente;31000\n"
)


PRACAS_CSV = (
    "concessionaria;praca_de_pedagio;rodovia;uf;km_m;municipio;lat;lon;situacao\n"
    "CCR AutoBAn;Campinas;SP-348;SP;87+500;Campinas;-22.9;-47.0;Ativa\n"
    "EcoRodovias;Anchieta;SP-150;SP;40+200;Cubatao;-23.8;-46.3;Ativa\n"
    "Arteris;Jacarezinho;BR-153;PR;10+000;Jacarezinho;-23.1;-49.9;Ativa\n"
)


def parse(raw: str | bytes, year: int, **options: object) -> pd.DataFrame:
    content = raw.encode() if isinstance(raw, str) else raw
    return parser.parse_trafego_file(
        io.BytesIO(content), ano=year, frequencia="mensal", **options
    ).frame


def test_headerless_csv_rejected():
    with pytest.raises(ParseError, match="CSV sem cabeçalho; layout não suportado"):
        parse(V2_CSV_NO_HEADER, 2024)


@pytest.mark.parametrize(
    "category,kind,axles,heavy",
    [
        ("Veículo Passeio 3 eixos", "Passeio", 3, False),
        ("Veículo Comercial 2 eixos", "Comercial", 2, False),
        ("Veículo Comercial Acima de 10 Eixos", "Comercial", None, True),
        ("Moto", "Moto", None, False),
        ("Categoria desconhecida", None, None, None),
    ],
)
def test_textual_categories_preserved(category, kind, axles, heavy):
    columns = "concessionaria;praca;mes_ano;categoria_eixo;tipo_cobranca;sentido;quantidade\n"
    statuses = []
    frame = parse(
        columns + f"CONCER;P1;01/2025;{category};Manual;Crescente;1\n",
        2025,
        keep=lambda record: statuses.append(parser.heavy_vehicle_status(record)) is None,
    )
    row = frame.iloc[0]
    assert (None if pd.isna(row["n_eixos"]) else row["n_eixos"]) == axles
    assert (None if pd.isna(row["tipo_veiculo"]) else row["tipo_veiculo"]) == kind
    assert row["categoria_eixo"] == category
    assert statuses == [heavy]


@pytest.mark.parametrize(
    "function",
    [parser.parse_pracas, lambda raw: parse(raw, 2026)],
    ids=["pracas", "trafego"],
)
def test_bytes_empty_fails(function):
    with pytest.raises(ParseError, match="vazio"):
        function(b"")


def test_pracas_uf_normalized():
    try:
        frame = parser.parse_pracas(b"concessionaria;praca_de_pedagio;uf\nX;P; sc \n")
    except ParseError as exc:
        frame = exc
    assert not isinstance(frame, ParseError), frame
    assert frame["uf"].tolist() == ["SC"]


def test_pracas_known_columns_and_municipal_alias():
    frame = parser.parse_pracas(PRACAS_CSV.replace("municipio", "municipal").encode())
    assert frame["municipio"].tolist() == ["Campinas", "Cubatao", "Jacarezinho"]
    assert "municipal" in frame
    assert frame["lat"].dtype == frame["lon"].dtype == "float64"
    assert frame["uf"].tolist() == ["SP", "SP", "PR"]
