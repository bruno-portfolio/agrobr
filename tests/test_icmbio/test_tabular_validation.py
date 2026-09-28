from __future__ import annotations

import csv
import io

import pytest

from agrobr.exceptions import ParseError
from agrobr.icmbio import models, parser


def _csv(**changes: str) -> bytes:
    row = dict.fromkeys(models.PROPERTY_NAMES, "NA")
    row.update(cnuc="000001", grupouc="pi", areahaalb="12.5", criacaoano="1981")
    row.update(changes)
    stream = io.StringIO()
    writer = csv.DictWriter(stream, fieldnames=models.PROPERTY_NAMES)
    writer.writeheader()
    writer.writerow(row)
    return stream.getvalue().encode()


@pytest.mark.parametrize(
    ("column", "value"),
    [
        ("areahaalb", "NaN"),
        ("areahaalb", "inf"),
        ("areahaalb", "-inf"),
        ("areahaalb", "NA"),
        ("areahaalb", " "),
        ("criacaoano", "NULL"),
        ("criacaoano", "1981.5"),
        ("criacaoano", " "),
        ("criacaoano", str(2**63)),
    ],
)
def test_tabular_numeros_invalidos(column: str, value: str):
    with pytest.raises(ParseError, match="UC invalida"):
        parser.parse_ucs_csv(_csv(**{column: value}))


@pytest.mark.parametrize("missing", models.PROPERTY_NAMES)
def test_tabular_cabecalho_incompleto_vazio(missing: str):
    header = ",".join(column for column in models.PROPERTY_NAMES if column != missing)
    with pytest.raises(ParseError, match="Colunas obrigatorias ausentes"):
        parser.parse_ucs_csv(f"{header}\n".encode())


@pytest.mark.parametrize("count", ["unknown", "-1", "1.0", "+1", "", " 1", "١"])
def test_feature_count_invalido(count: str):
    data = (
        '<wfs:FeatureCollection xmlns:wfs="http://www.opengis.net/wfs" '
        f'numberOfFeatures="{count}"/>'
    ).encode()
    with pytest.raises(ParseError, match="Contagem WFS invalida"):
        parser.parse_feature_count(data)


@pytest.mark.parametrize(
    "data",
    [
        b"not xml",
        b'<FeatureCollection numberOfFeatures="347"/>',
        b'<wfs:FeatureCollection xmlns:wfs="http://www.opengis.net/wfs"/>',
        b'<ExceptionReport numberOfFeatures="347"/>',
        b'<!DOCTYPE x [<!ENTITY x SYSTEM "file:///missing">]>'
        b'<wfs:FeatureCollection xmlns:wfs="http://www.opengis.net/wfs" '
        b'numberOfFeatures="347"/>',
    ],
)
def test_feature_count_xml_invalido(data: bytes):
    with pytest.raises(ParseError, match="Contagem WFS invalida"):
        parser.parse_feature_count(data)
