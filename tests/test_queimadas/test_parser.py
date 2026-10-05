from __future__ import annotations

import csv
import io
from pathlib import Path

import pytest

from agrobr.exceptions import ParseError
from agrobr.queimadas import parser
from agrobr.queimadas.parser import parse_focos_csv

GOLDEN = Path(__file__).parents[1] / "golden_data" / "queimadas" / "focos_sample" / "response.csv"


class TestParseFocosCsv:
    def test_empty_csv_raises_parse_error(self):
        with pytest.raises(ParseError):
            parse_focos_csv(b"id,lat,lon,data_hora_gmt,satelite\n")

    def test_missing_columns_raises_parse_error(self):
        csv = b"id,nome,valor\n1,teste,100\n"
        with pytest.raises(ParseError, match="Colunas obrigatorias ausentes"):
            parse_focos_csv(csv)

    def test_data_sai_em_datetime64_a_meia_noite_do_foco(self):
        df = parse_focos_csv(GOLDEN.read_bytes())

        assert df["data"].dtype.kind == "M"
        publicadas = [
            linha["data_hora_gmt"][:10]
            for linha in csv.DictReader(io.StringIO(GOLDEN.read_text(encoding="utf-8")))
        ]
        assert [f"{data:%Y-%m-%d %H:%M}" for data in df["data"]] == [
            f"{data} 00:00" for data in publicadas
        ]

    def test_estado_e_bioma_normalizados_uma_vez_por_valor(self, monkeypatch):
        chamadas: list[str] = []

        def contar(funcao):
            def contada(valor):
                chamadas.append(valor)
                return funcao(valor)

            return contada

        monkeypatch.setattr(parser, "estado_para_uf", contar(parser.estado_para_uf))
        monkeypatch.setattr(parser, "normalizar_bioma", contar(parser.normalizar_bioma))
        linhas = [
            f"{i},-10.5,-48.3,2024-08-01 {i % 24:02d}:{i % 60:02d}:00,AQUA_M-T,PALMAS,"
            f"{'TOCANTINS' if i % 2 else 'GOIÁS'},Cerrado"
            for i in range(200)
        ]
        corpo = "\n".join(["id,lat,lon,data_hora_gmt,satelite,municipio,estado,bioma", *linhas])

        df = parse_focos_csv(corpo.encode())

        assert sorted(chamadas) == ["Cerrado", "GOIÁS", "TOCANTINS"]
        assert df["uf"].value_counts().to_dict() == {"TO": 100, "GO": 100}
        assert df["hora_gmt"].tolist() == [f"{i % 24:02d}:{i % 60:02d}" for i in range(200)]
