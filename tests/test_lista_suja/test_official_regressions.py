from __future__ import annotations

import csv
import hashlib
import io

import pytest

from agrobr.exceptions import ParseError
from agrobr.lista_suja import parser


@pytest.fixture(scope="module")
def official_csv(publication_files):
    return parser.parse_empregadores_bundle(
        publication_files["csv"], formato="csv", companion=publication_files["txt"]
    )


def test_original_captures_match_manifest(publication_files):
    for item in publication_files["manifest"]["artifacts"]:
        raw = (publication_files["root"] / item["file"]).read_bytes()
        assert len(raw) == item["bytes"]
        assert hashlib.sha256(raw).hexdigest() == item["sha256"]
    assert (
        hashlib.sha256(publication_files["pdf"]).hexdigest()
        == publication_files["manifest"]["pdf_sha256"]
    )


@pytest.mark.parametrize("inclusion", ["", "   "])
def test_inclusao_ausente_rejeitada_pelo_parser(publication_files, inclusion):
    rows = list(csv.reader(io.StringIO(publication_files["csv"].decode("cp1252")), delimiter=";"))
    rows[1][9] = inclusion
    stream = io.StringIO(newline="")
    csv.writer(stream, delimiter=";").writerows(rows)
    with pytest.raises(ParseError, match="inclusão no cadastro ausente"):
        parser.parse_empregadores_bundle(stream.getvalue().encode("cp1252"), formato="csv")


def test_official_csv_preserves_identity_nulls_and_published_counts(official_csv):
    frame, details = official_csv
    assert len(frame) == 579
    assert frame["id_registro"].tolist() == [str(i) for i in range(1, 580)]
    assert frame["cpf_cnpj"].nunique() == 567
    assert frame["cpf_cnpj"].map(lambda value: isinstance(value, str)).all()
    assert frame["trabalhadores_resgatados"].sum() == 4706
    assert frame.loc[frame["uf"].isna(), "id_registro"].tolist() == ["13", "140", "394"]
    assert frame.loc[frame["trabalhadores_resgatados"].isna(), "id_registro"].tolist() == [
        "140",
        "394",
    ]
    judicial = frame.set_index("id_registro").loc[["13", "140", "394"]]
    assert (
        judicial[["ano_acao_fiscal", "estabelecimento", "cnae", "data_decisao"]].isna().all().all()
    )
    assert judicial.loc["13", "trabalhadores_resgatados"] == 5
    assert str(frame["trabalhadores_resgatados"].dtype) == "Int64"
    assert str(frame["ano_acao_fiscal"].dtype) == "Int64"
    assert details["source_rows"] == details["output_rows"] == 579
    fingerprint = details["layout_fingerprint"]
    assert fingerprint["algorithm"] == "sha256"
    assert fingerprint["parser_version"] == 4
    assert len(fingerprint["sha256"]) == 64
