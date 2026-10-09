from __future__ import annotations

import zipfile
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from agrobr.acervo_fundiario import models, parser
from agrobr.exceptions import ParseError
from agrobr.utils.result import ATRIBUTO_AVISOS
from tests.helpers import levanta_exatamente


class TestSchemaDriftDetection:
    def test_missing_required_raises(self, tmp_path: Path):
        pytest.importorskip("geopandas")
        import geopandas as gpd
        import pyogrio
        from shapely.geometry import Polygon

        df = pd.DataFrame({"some_col": ["x"]})
        gdf = gpd.GeoDataFrame(df, geometry=[Polygon([(0, 0), (1, 0), (1, 1)])], crs="EPSG:4674")
        shp_dir = tmp_path / "broken_layer"
        shp_dir.mkdir()
        pyogrio.write_dataframe(gdf, shp_dir / "broken.shp", encoding="latin1")
        zip_path = tmp_path / "broken.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            for f in shp_dir.iterdir():
                zf.write(f, f.name)

        with pytest.raises(ParseError, match="Colunas obrigatorias"):
            parser.parse_sigef(zip_path, natureza="privado")


def _partes_do_shapefile(pasta: Path) -> dict[str, bytes]:
    gpd = pytest.importorskip("geopandas")
    geometria = pytest.importorskip("shapely.geometry")
    camada = gpd.GeoDataFrame({"txt": ["x"]}, geometry=[geometria.Point(-50, -15)], crs="EPSG:4674")
    camada.to_file(pasta / "a.shp", engine="pyogrio")
    return {parte.name: parte.read_bytes() for parte in sorted(pasta.glob("a.*"))}


def _zip(destino: Path, membros: dict[str, bytes]) -> Path:
    with zipfile.ZipFile(destino, "w") as arquivo:
        for nome, dados in membros.items():
            arquivo.writestr(nome, dados)
    return destino


def _vrt_do_arquivo_local(pasta: Path) -> bytes:
    segredo = pasta / "segredo.csv"
    segredo.write_text("chave,valor\nSEGREDO_LOCAL,1\n", encoding="utf-8")
    return (
        '<OGRVRTDataSource><OGRVRTLayer name="c">'
        f"<SrcDataSource>{segredo.as_posix()}</SrcDataSource><SrcLayer>segredo</SrcLayer>"
        "</OGRVRTLayer></OGRVRTDataSource>"
    ).encode()


LEITORES = {"tabular": parser._read_tabular, "geo": parser._read_geo}


@pytest.mark.parametrize("leitor", list(LEITORES))
@pytest.mark.parametrize(
    ("nome", "motivo"),
    [
        ("a.vrt", r"\.shp: 0, fora: \['a\.vrt'\]"),
        ("a.shp", "a.shp não tem o cabeçalho de shapefile"),
    ],
)
def test_vrt_no_zip_do_acervo_nao_chega_ao_gdal(tmp_path, leitor, nome, motivo):
    pytest.importorskip("geopandas")
    forjado = _zip(tmp_path / "forjado.zip", {nome: _vrt_do_arquivo_local(tmp_path)})

    with levanta_exatamente(ParseError, motivo):
        LEITORES[leitor](forjado)


@pytest.mark.parametrize(
    ("extra", "motivo"),
    [
        ({"b.vrt": b"<OGRVRTDataSource/>"}, r"\.shp: 1, fora: \['b\.vrt'\]"),
        ({"b.shp": b"\x00\x00\x27\x0a"}, r"\.shp: 2, fora: \[\]"),
        ({"../c.cpg": b"UTF-8"}, r"\.shp: 1, fora: \['\.\./c\.cpg'\]"),
    ],
)
def test_zip_do_acervo_so_com_as_partes_de_um_shapefile(tmp_path, extra, motivo):
    honesto = _partes_do_shapefile(tmp_path)
    forjado = _zip(tmp_path / "forjado.zip", {**honesto, **extra})

    with levanta_exatamente(ParseError, motivo):
        parser._read_tabular(forjado)


def test_zip_do_acervo_com_shapefile_segue_pelo_vsizip(tmp_path):
    honesto = _zip(tmp_path / "honesto.zip", _partes_do_shapefile(tmp_path))

    assert parser._shapefile(honesto) == f"/vsizip/{honesto.as_posix()}/a.shp"
    assert parser._read_tabular(honesto)["txt"].tolist() == ["x"]


def _assentamentos(pasta: Path, **campos: list[Any]) -> Path:
    gpd = pytest.importorskip("geopandas")
    geometria = pytest.importorskip("shapely.geometry")
    colunas: dict[str, list[Any]] = {
        **{campo: ["x", "y"] for campo in models.ASSENTAMENTOS_RENAME_MAP},
        "uf": ["DF", "DF"],
        "data_de_cr": ["01/01/2020", "01/01/2020"],
        "data_obten": ["01/01/2020", "01/01/2020"],
        "area_hecta": ["12.5", "1"],
        "area_calc_": [12.5, 1.0],
        "capacidade": [10, None],
        "num_famili": [5, 3],
        "fase": [3, 4],
        **campos,
    }
    camada = gpd.GeoDataFrame(
        colunas, geometry=[geometria.Point(-47.9, -15.8)] * 2, crs="EPSG:4674"
    )
    camada.to_file(pasta / "a.shp", engine="pyogrio")
    partes = {parte.name: parte.read_bytes() for parte in sorted(pasta.glob("a.*"))}
    return _zip(pasta / "assentamentos.zip", partes)


def test_inteiros_do_assentamento_saem_int64_com_e_sem_nulo(tmp_path):
    frame = parser.parse_assentamentos(_assentamentos(tmp_path))

    assert {
        coluna: str(frame[coluna].dtype) for coluna in ("capacidade", "num_familias", "fase")
    } == {
        "capacidade": "Int64",
        "num_familias": "Int64",
        "fase": "Int64",
    }
    assert frame["capacidade"].tolist() == [10, pd.NA]


def test_area_sai_float64_mesmo_sem_decimal(tmp_path):
    frame = parser.parse_assentamentos(_assentamentos(tmp_path, area_hecta=["12", "1"]))

    assert str(frame["area_ha"].dtype) == "float64"
    assert frame["area_ha"].tolist() == [12.0, 1.0]


GOLDEN = Path(__file__).parents[1] / "golden_data" / "acervo_fundiario"
FORA_DO_BRASIL = (10.0, 10.0, 10.1, 10.1)
LEITURAS = {
    "sigef": lambda bbox: parser.parse_sigef(
        GOLDEN / "sigef_publico_df_20261001/response.zip", natureza="publico", bbox=bbox
    ),
    "sigef_geo": lambda bbox: parser.parse_sigef_geo(
        GOLDEN / "sigef_publico_df_20261001/response.zip", natureza="publico", bbox=bbox
    ),
    "snci": lambda bbox: parser.parse_snci(GOLDEN / "snci_rr_20260922/response.zip", bbox=bbox),
    "snci_geo": lambda bbox: parser.parse_snci_geo(
        GOLDEN / "snci_rr_20260922/response.zip", bbox=bbox
    ),
    "assentamentos": lambda bbox: parser.parse_assentamentos(
        GOLDEN / "assentamentos_20260922/response.zip", bbox=bbox
    ),
    "assentamentos_geo": lambda bbox: parser.parse_assentamentos_geo(
        GOLDEN / "assentamentos_20260922/response.zip", bbox=bbox
    ),
}


@pytest.mark.parametrize("leitura", list(LEITURAS))
def test_vazio_sai_com_os_dtypes_do_cheio(leitura):
    pytest.importorskip("geopandas")
    texto = pd.Series([""]).dtype

    cheio = LEITURAS[leitura](None)
    vazio = LEITURAS[leitura](FORA_DO_BRASIL)

    assert len(cheio) and vazio.empty
    assert list(vazio.dtypes.items()) == list(cheio.dtypes.items())
    assert [c for c in cheio.columns if cheio[c].dtype == object and cheio[c].dtype != texto] == []


def test_inteiro_fracionario_levanta_parse_error_com_o_arquivo(tmp_path):
    zip_path = _assentamentos(tmp_path, capacidade=[1.5, 2.0])

    with levanta_exatamente(ParseError, r"capacidade com valor não inteiro \(assentamentos.zip\)"):
        parser.parse_assentamentos(zip_path)


def test_campo_renomeado_pelo_incra_levanta_parse_error_com_o_arquivo(tmp_path):
    zip_path = _assentamentos(tmp_path)
    gpd = pytest.importorskip("geopandas")
    camada = gpd.read_file(tmp_path / "a.shp").rename(columns={"num_famili": "familias"})
    camada.to_file(tmp_path / "a.shp", engine="pyogrio")
    partes = {parte.name: parte.read_bytes() for parte in sorted(tmp_path.glob("a.*"))}
    zip_path = _zip(tmp_path / "assentamentos.zip", partes)

    with levanta_exatamente(ParseError, r"assentamentos \(assentamentos.zip\): \['num_famili'\]"):
        parser.parse_assentamentos(zip_path)


def test_numero_fora_do_formato_vira_nulo_com_aviso(tmp_path):
    zip_path = _assentamentos(tmp_path, area_hecta=["2449,3310", "1"])

    with pytest.warns(UserWarning, match=r"1 valor\(es\) de area_ha fora do formato"):
        frame = parser.parse_assentamentos(zip_path)

    assert frame["area_ha"].isna().tolist() == [True, False]
    assert frame.attrs[ATRIBUTO_AVISOS] == [
        "acervo_fundiario: 1 valor(es) de area_ha fora do formato numérico viraram nulo (ex.: '2449,3310')"
    ]
