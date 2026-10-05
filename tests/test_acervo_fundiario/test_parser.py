from __future__ import annotations

import zipfile
from pathlib import Path

import pandas as pd
import pytest

from agrobr.acervo_fundiario import parser
from agrobr.exceptions import ParseError
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
