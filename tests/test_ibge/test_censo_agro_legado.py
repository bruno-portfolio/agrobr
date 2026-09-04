from __future__ import annotations

import io
import zipfile
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from agrobr.ibge import ftp_client, legacy_parser
from tests.helpers import make_mock_async_client, make_mock_response


def _build_fake_xls(rows: list[list]) -> bytes:
    import xlwt

    wb = xlwt.Workbook()
    ws = wb.add_sheet("Sheet1")
    for r_idx, row in enumerate(rows):
        for c_idx, val in enumerate(row):
            if val is not None:
                ws.write(r_idx, c_idx, val)
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


def _build_fake_zip(xls_bytes: bytes, xls_name: str = "Tnet  Dnet_35_Mn0300.xls") -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_STORED) as zf:
        zf.writestr(xls_name, xls_bytes)
    return buf.getvalue()


def _make_tecnologia_rows(n_rows: int = 3) -> list[list]:
    header_blank = [None] * 7
    rows = [header_blank] * 6
    rows.append(["          Totais", 1000, 2000, 3000, 4000, 5000, 6000])
    rows.append(["Araçatuba", 100, 200, 300, 400, 500, 600])
    rows.append(["   Andradina", 50, 100, 150, 200, 250, 300])
    if n_rows > 3:
        rows.append(["      Andradina", 10, 20, 30, 40, 50, 60])
    return rows


def _make_pessoal_rows() -> list[list]:
    rows = [[None] * 6] * 6
    rows.append(["          Totais", 915000, 705000, 29000, 210000, 22000])
    rows.append(["Araçatuba", 50000, 40000, 3000, 6000, 1000])
    return rows


def _make_maquinas_rows() -> list[list]:
    rows = [[None] * 6] * 6
    rows.append(["          Totais", 170000, 53000, 18000, 32000, 56000])
    rows.append(["Araçatuba", 5000, 2000, 1000, 1000, 1000])
    return rows


def _make_producao_animal_rows() -> list[list]:
    rows = [[None] * 5] * 6
    rows.append(["          Totais", 1847000000, 1278000, 58000, 614000000])
    rows.append(["Araçatuba", 100000, 500, 10, 50000])
    return rows


def _make_valor_producao_rows() -> list[list]:
    rows = [[None] * 6] * 6
    rows.append(["          Totais", 8412000000, 6010000000, 5602000000, 2403000000, 1369000000])
    rows.append(["Araçatuba", 100000, 80000, 75000, 20000, 15000])
    return rows


def _make_financeiro_rows() -> list[list]:
    rows = [[None] * 5] * 6
    rows.append(["          Totais", 1089000000, 750000000, 6135000000, 8666000000])
    rows.append(["Araçatuba", 50000, 30000, 200000, 300000])
    return rows


class TestFtpClientConstants:
    def test_ftp_base_url(self):
        assert "ftp.ibge.gov.br" in ftp_client.FTP_BASE
        assert "Censo_Agropecuario_1995_96" in ftp_client.FTP_BASE

    def test_legacy_temas_count(self):
        assert len(ftp_client.LEGACY_TEMAS) == 6

    def test_legacy_temas_names(self):
        expected = {
            "tecnologia",
            "pessoal_ocupado",
            "maquinas",
            "producao_animal",
            "valor_producao",
            "financeiro",
        }
        assert set(ftp_client.LEGACY_TEMAS.keys()) == expected

    def test_legacy_temas_tab_base_names(self):
        assert ftp_client.LEGACY_TEMAS["tecnologia"] == "Tab_3"
        assert ftp_client.LEGACY_TEMAS["pessoal_ocupado"] == "Tab_6"
        assert ftp_client.LEGACY_TEMAS["maquinas"] == "Tab_7"
        assert ftp_client.LEGACY_TEMAS["producao_animal"] == "Tab_9"
        assert ftp_client.LEGACY_TEMAS["valor_producao"] == "Tab_10"
        assert ftp_client.LEGACY_TEMAS["financeiro"] == "Tab_11"

    def test_uf_dirs_has_27_entries(self):
        assert len(ftp_client.UF_DIRS) == 27

    def test_uf_dirs_sp(self):
        assert ftp_client.UF_DIRS["SP"] == "Sao_Paulo"

    def test_uf_dirs_df(self):
        assert ftp_client.UF_DIRS["DF"] == "Distrito_Federal"

    def test_uf_dirs_ms(self):
        assert ftp_client.UF_DIRS["MS"] == "Mato_Grosso_do_Sul"

    def test_timeout_read_180s(self):
        assert ftp_client.TIMEOUT.read == 180.0


class TestFtpClientDownload:
    @pytest.mark.asyncio
    async def test_download_success(self):
        fake_content = b"x" * 600
        fake_zip = _build_fake_zip(fake_content)
        mock_response = make_mock_response(content=fake_zip)

        with (
            patch(
                "agrobr.ibge.ftp_client.retry_on_status",
                new_callable=AsyncMock,
                return_value=mock_response,
            ),
            patch("agrobr.ibge.ftp_client.httpx.AsyncClient") as mock_cls,
        ):
            mock_cls.return_value = make_mock_async_client()

            result = await ftp_client.download_legacy_zip("Tab_3", uf_dir="Sao_Paulo")

        assert len(result) == len(fake_zip)
        assert result == fake_zip

    @pytest.mark.asyncio
    async def test_download_too_small_raises(self):
        mock_response = make_mock_response(content=b"tiny")

        with (
            patch(
                "agrobr.ibge.ftp_client.retry_on_status",
                new_callable=AsyncMock,
                return_value=mock_response,
            ),
            patch("agrobr.ibge.ftp_client.httpx.AsyncClient") as mock_cls,
        ):
            mock_cls.return_value = make_mock_async_client()

            from agrobr.exceptions import SourceUnavailableError

            with pytest.raises(SourceUnavailableError, match="muito pequeno"):
                await ftp_client.download_legacy_zip("Tab_3")

    @pytest.mark.asyncio
    async def test_download_url_format(self):
        fake_zip = _build_fake_zip(b"x" * 600)
        mock_response = make_mock_response(content=fake_zip)

        captured_url = None

        async def fake_retry(func, **_kwargs):
            nonlocal captured_url
            await func()
            return mock_response

        with (
            patch("agrobr.ibge.ftp_client.retry_on_status", side_effect=fake_retry),
            patch("agrobr.ibge.ftp_client.httpx.AsyncClient") as mock_cls,
        ):
            mock_http = make_mock_async_client()
            mock_cls.return_value = mock_http

            await ftp_client.download_legacy_zip("Tab_6", uf_dir="Minas_Gerais")

            mock_http.get.assert_called_once()
            captured_url = mock_http.get.call_args[0][0]
            assert "Minas_Gerais/Tab_6Mn.zip" in captured_url

    @pytest.mark.asyncio
    async def test_download_brasil_url_no_mn_suffix(self):
        fake_zip = _build_fake_zip(b"x" * 600)
        mock_response = make_mock_response(content=fake_zip)

        async def fake_retry(func, **_kwargs):
            await func()
            return mock_response

        with (
            patch("agrobr.ibge.ftp_client.retry_on_status", side_effect=fake_retry),
            patch("agrobr.ibge.ftp_client.httpx.AsyncClient") as mock_cls,
        ):
            mock_http = make_mock_async_client()
            mock_cls.return_value = mock_http

            await ftp_client.download_legacy_zip("Tab_3", uf_dir="Brasil")

            mock_http.get.assert_called_once()
            captured_url = mock_http.get.call_args[0][0]
            assert "Brasil/Tab_3.zip" in captured_url
            assert "Mn" not in captured_url


class TestExtractXlsFromZip:
    def test_extract_single_xls(self):
        xls_data = b"fake_xls_data"
        zip_bytes = _build_fake_zip(xls_data, "test_file.xls")
        result = ftp_client.extract_xls_from_zip(zip_bytes)
        assert len(result) == 1
        assert result[0][0] == "test_file.xls"
        assert result[0][1] == xls_data

    def test_extract_with_pattern(self):
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("file_35_Mn0300.xls", b"data_sp")
            zf.writestr("file_31_Mn0300.xls", b"data_mg")
            zf.writestr("readme.txt", b"ignore")
        zip_bytes = buf.getvalue()

        result = ftp_client.extract_xls_from_zip(zip_bytes, pattern="_35_")
        assert len(result) == 1
        assert "_35_" in result[0][0]

    def test_extract_ignores_non_xls(self):
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("data.xls", b"xls_data")
            zf.writestr("readme.txt", b"text")
            zf.writestr("info.csv", b"csv_data")
        zip_bytes = buf.getvalue()

        result = ftp_client.extract_xls_from_zip(zip_bytes)
        assert len(result) == 1
        assert result[0][0] == "data.xls"

    def test_extract_empty_zip(self):
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("readme.txt", b"no xls here")
        result = ftp_client.extract_xls_from_zip(buf.getvalue())
        assert result == []

    def test_extract_no_pattern_match(self):
        zip_bytes = _build_fake_zip(b"data", "file_35.xls")
        result = ftp_client.extract_xls_from_zip(zip_bytes, pattern="_99_")
        assert result == []


class TestLegacyParserConstants:
    def test_parser_version(self):
        assert legacy_parser.PARSER_VERSION == 1

    def test_temas_legado_count(self):
        assert len(legacy_parser.TEMAS_LEGADO) == 6

    def test_temas_legado_names(self):
        expected = {
            "tecnologia",
            "pessoal_ocupado",
            "maquinas",
            "producao_animal",
            "valor_producao",
            "financeiro",
        }
        assert set(legacy_parser.TEMAS_LEGADO) == expected

    def test_tema_config_tecnologia_has_6_cols(self):
        assert len(legacy_parser._TEMA_COLS["tecnologia"]) == 6

    def test_tema_config_pessoal_has_5_cols(self):
        assert len(legacy_parser._TEMA_COLS["pessoal_ocupado"]) == 5

    def test_tema_config_maquinas_has_5_cols(self):
        assert len(legacy_parser._TEMA_COLS["maquinas"]) == 5

    def test_tema_config_producao_animal_has_4_cols(self):
        assert len(legacy_parser._TEMA_COLS["producao_animal"]) == 4

    def test_tema_config_valor_producao_has_5_cols(self):
        assert len(legacy_parser._TEMA_COLS["valor_producao"]) == 5

    def test_tema_config_financeiro_has_4_cols(self):
        assert len(legacy_parser._TEMA_COLS["financeiro"]) == 4


class TestDetectNivelGeo:
    def test_totais(self):
        assert legacy_parser._detect_nivel_geo("          Totais") == "totais"

    def test_mesorregiao(self):
        assert legacy_parser._detect_nivel_geo("Araçatuba") == "mesorregiao"

    def test_microrregiao(self):
        assert legacy_parser._detect_nivel_geo("   Andradina") == "microrregiao"

    def test_municipio(self):
        assert legacy_parser._detect_nivel_geo("      Andradina") == "municipio"

    def test_many_spaces_is_totais(self):
        assert legacy_parser._detect_nivel_geo("            Brasil") == "totais"


class TestParseLegacyXls:
    def test_marcador_nao_disponivel_vira_nulo(self):
        records = legacy_parser._extract_from_sheet(
            pd.DataFrame([["Brasil", ".."]]),
            "tecnologia",
            {1: ("assistencia_tecnica", "estabelecimentos", "estabelecimentos")},
        )

        assert records[0][6] is None

    def test_tema_invalido_raises(self):
        from agrobr.exceptions import ParseError

        with pytest.raises(ParseError, match="Tema não suportado"):
            legacy_parser.parse_legacy_xls(b"", tema="inexistente")

    def test_corrupt_xls_raises(self):
        from agrobr.exceptions import ParseError

        with pytest.raises(ParseError, match="Falha ao ler XLS"):
            legacy_parser.parse_legacy_xls(b"not_an_xls", tema="tecnologia")

    def test_output_columns(self):
        xls = _build_fake_xls(_make_tecnologia_rows())
        df = legacy_parser.parse_legacy_xls(xls, tema="tecnologia")
        expected_cols = [
            "ano",
            "localidade",
            "localidade_cod",
            "tema",
            "categoria",
            "variavel",
            "valor",
            "unidade",
            "fonte",
            "nivel_geo",
        ]
        assert list(df.columns) == expected_cols

    def test_ano_is_1995(self):
        xls = _build_fake_xls(_make_tecnologia_rows())
        df = legacy_parser.parse_legacy_xls(xls, tema="tecnologia")
        assert (df["ano"] == 1995).all()

    def test_fonte_is_legado(self):
        xls = _build_fake_xls(_make_tecnologia_rows())
        df = legacy_parser.parse_legacy_xls(xls, tema="tecnologia")
        assert (df["fonte"] == "ibge_censo_agro_legado").all()

    def test_tema_column(self):
        xls = _build_fake_xls(_make_tecnologia_rows())
        df = legacy_parser.parse_legacy_xls(xls, tema="tecnologia")
        assert (df["tema"] == "tecnologia").all()

    def test_valor_is_numeric(self):
        xls = _build_fake_xls(_make_tecnologia_rows())
        df = legacy_parser.parse_legacy_xls(xls, tema="tecnologia")
        assert df["valor"].dtype in ("float64", "Float64")


class TestParseTecnologia:
    def test_categorias(self):
        xls = _build_fake_xls(_make_tecnologia_rows())
        df = legacy_parser.parse_legacy_xls(xls, tema="tecnologia")
        cats = set(df["categoria"].unique())
        expected = {
            "assistencia_tecnica",
            "irrigacao",
            "adubos_corretivos",
            "controle_pragas",
            "conservacao_solo",
            "energia_eletrica",
        }
        assert cats == expected

    def test_variavel_is_estabelecimentos(self):
        xls = _build_fake_xls(_make_tecnologia_rows())
        df = legacy_parser.parse_legacy_xls(xls, tema="tecnologia")
        assert (df["variavel"] == "estabelecimentos").all()

    def test_unidade_is_estabelecimentos(self):
        xls = _build_fake_xls(_make_tecnologia_rows())
        df = legacy_parser.parse_legacy_xls(xls, tema="tecnologia")
        assert (df["unidade"] == "estabelecimentos").all()

    def test_row_count(self):
        xls = _build_fake_xls(_make_tecnologia_rows())
        df = legacy_parser.parse_legacy_xls(xls, tema="tecnologia")
        assert len(df) == 3 * 6

    def test_totais_valor(self):
        xls = _build_fake_xls(_make_tecnologia_rows())
        df = legacy_parser.parse_legacy_xls(xls, tema="tecnologia")
        totais = df[df["nivel_geo"] == "totais"]
        assist = totais[totais["categoria"] == "assistencia_tecnica"]["valor"].iloc[0]
        assert assist == 1000.0

    def test_hierarchy_levels(self):
        xls = _build_fake_xls(_make_tecnologia_rows(n_rows=4))
        df = legacy_parser.parse_legacy_xls(xls, tema="tecnologia")
        levels = set(df["nivel_geo"].unique())
        assert levels == {"totais", "mesorregiao", "microrregiao", "municipio"}


class TestParsePessoalOcupado:
    def test_categorias(self):
        xls = _build_fake_xls(_make_pessoal_rows())
        df = legacy_parser.parse_legacy_xls(xls, tema="pessoal_ocupado")
        cats = set(df["categoria"].unique())
        expected = {"total", "familiar", "permanentes", "temporarios", "parceiros_outra"}
        assert cats == expected

    def test_unidade_pessoas(self):
        xls = _build_fake_xls(_make_pessoal_rows())
        df = legacy_parser.parse_legacy_xls(xls, tema="pessoal_ocupado")
        assert (df["unidade"] == "pessoas").all()

    def test_row_count(self):
        xls = _build_fake_xls(_make_pessoal_rows())
        df = legacy_parser.parse_legacy_xls(xls, tema="pessoal_ocupado")
        assert len(df) == 2 * 5


class TestParseMaquinas:
    def test_categorias(self):
        xls = _build_fake_xls(_make_maquinas_rows())
        df = legacy_parser.parse_legacy_xls(xls, tema="maquinas")
        cats = set(df["categoria"].unique())
        expected = {"total_tratores", "menos_10cv", "10_50cv", "50_100cv", "mais_100cv"}
        assert cats == expected

    def test_unidade_unidades(self):
        xls = _build_fake_xls(_make_maquinas_rows())
        df = legacy_parser.parse_legacy_xls(xls, tema="maquinas")
        assert (df["unidade"] == "unidades").all()


class TestParseProducaoAnimal:
    def test_categorias(self):
        xls = _build_fake_xls(_make_producao_animal_rows())
        df = legacy_parser.parse_legacy_xls(xls, tema="producao_animal")
        cats = set(df["categoria"].unique())
        expected = {"leite_vaca", "leite_cabra", "la", "ovos_galinha"}
        assert cats == expected

    def test_mixed_unidades(self):
        xls = _build_fake_xls(_make_producao_animal_rows())
        df = legacy_parser.parse_legacy_xls(xls, tema="producao_animal")
        unidades = set(df["unidade"].unique())
        assert unidades == {"litros", "kg", "duzias"}

    def test_leite_vaca_valor(self):
        xls = _build_fake_xls(_make_producao_animal_rows())
        df = legacy_parser.parse_legacy_xls(xls, tema="producao_animal")
        totais = df[(df["nivel_geo"] == "totais") & (df["categoria"] == "leite_vaca")]
        assert totais["valor"].iloc[0] == 1847000000.0


class TestParseValorProducao:
    def test_categorias(self):
        xls = _build_fake_xls(_make_valor_producao_rows())
        df = legacy_parser.parse_legacy_xls(xls, tema="valor_producao")
        cats = set(df["categoria"].unique())
        expected = {"total", "vegetal", "vegetal_subtipo", "animal", "animal_subtipo"}
        assert cats == expected

    def test_unidade_reais(self):
        xls = _build_fake_xls(_make_valor_producao_rows())
        df = legacy_parser.parse_legacy_xls(xls, tema="valor_producao")
        assert (df["unidade"] == "R$").all()


class TestParseFinanceiro:
    def test_categorias(self):
        xls = _build_fake_xls(_make_financeiro_rows())
        df = legacy_parser.parse_legacy_xls(xls, tema="financeiro")
        cats = set(df["categoria"].unique())
        expected = {"investimentos", "financiamentos", "despesas", "receitas"}
        assert cats == expected

    def test_unidade_reais(self):
        xls = _build_fake_xls(_make_financeiro_rows())
        df = legacy_parser.parse_legacy_xls(xls, tema="financeiro")
        assert (df["unidade"] == "R$").all()

    def test_row_count(self):
        xls = _build_fake_xls(_make_financeiro_rows())
        df = legacy_parser.parse_legacy_xls(xls, tema="financeiro")
        assert len(df) == 2 * 4


class TestEmptyAndEdgeCases:
    def test_empty_xls_returns_empty_df(self):
        xls = _build_fake_xls([[None] * 7] * 10)
        df = legacy_parser.parse_legacy_xls(xls, tema="tecnologia")
        assert len(df) == 0
        assert list(df.columns) == legacy_parser._OUTPUT_COLS

    def test_localidade_stripped(self):
        rows = [[None] * 7] * 6
        rows.append(["          Totais", 1, 2, 3, 4, 5, 6])
        xls = _build_fake_xls(rows)
        df = legacy_parser.parse_legacy_xls(xls, tema="tecnologia")
        assert df["localidade"].iloc[0] == "Totais"

    def test_localidade_cod_is_na(self):
        xls = _build_fake_xls(_make_tecnologia_rows())
        df = legacy_parser.parse_legacy_xls(xls, tema="tecnologia")
        assert df["localidade_cod"].isna().all()


class TestCensoAgroLegadoApi:
    @pytest.mark.asyncio
    async def test_tema_invalido_raises(self):
        with pytest.raises(ValueError, match="não suportado"):
            from agrobr.ibge import legacy_api

            await legacy_api.censo_agro_legado("inexistente")

    @pytest.mark.asyncio
    async def test_uf_invalida_raises(self):
        from agrobr.ibge import legacy_api

        with pytest.raises(ValueError, match="inválida"):
            await legacy_api.censo_agro_legado("tecnologia", uf="XX")

    @pytest.mark.asyncio
    async def test_fluxo_completo_mock(self):
        xls = _build_fake_xls(_make_tecnologia_rows())
        fake_zip = _build_fake_zip(xls)

        with (
            patch(
                "agrobr.ibge.legacy_api.ftp_client.download_legacy_zip",
                new_callable=AsyncMock,
                return_value=fake_zip,
            ),
        ):
            from agrobr.ibge import legacy_api

            df = await legacy_api.censo_agro_legado("tecnologia")

        assert len(df) > 0
        assert (df["ano"] == 1995).all()
        assert (df["fonte"] == "ibge_censo_agro_legado").all()
        assert "nivel_geo" not in df.columns

    @pytest.mark.asyncio
    async def test_nivel_brasil_filtra_totais(self):
        xls = _build_fake_xls(_make_tecnologia_rows())
        fake_zip = _build_fake_zip(xls)

        with patch(
            "agrobr.ibge.legacy_api.ftp_client.download_legacy_zip",
            new_callable=AsyncMock,
            return_value=fake_zip,
        ):
            from agrobr.ibge import legacy_api

            df = await legacy_api.censo_agro_legado("tecnologia", nivel="brasil")

        locs = df["localidade"].unique()
        assert "Totais" in locs
        assert "Araçatuba" not in locs

    @pytest.mark.asyncio
    async def test_nivel_municipio_filtra_municipio(self):
        xls = _build_fake_xls(_make_tecnologia_rows(n_rows=4))
        fake_zip = _build_fake_zip(xls)

        with patch(
            "agrobr.ibge.legacy_api.ftp_client.download_legacy_zip",
            new_callable=AsyncMock,
            return_value=fake_zip,
        ):
            from agrobr.ibge import legacy_api

            df = await legacy_api.censo_agro_legado("tecnologia", nivel="municipio")

        assert len(df) > 0
        assert "Totais" not in df["localidade"].values

    @pytest.mark.asyncio
    async def test_return_meta(self):
        xls = _build_fake_xls(_make_tecnologia_rows())
        fake_zip = _build_fake_zip(xls)

        with patch(
            "agrobr.ibge.legacy_api.ftp_client.download_legacy_zip",
            new_callable=AsyncMock,
            return_value=fake_zip,
        ):
            from agrobr.ibge import legacy_api

            result = await legacy_api.censo_agro_legado("tecnologia", return_meta=True)

        assert isinstance(result, tuple)
        df, meta = result
        assert len(df) > 0
        assert meta.source == "ibge_censo_agro_legado"
        assert meta.dataset == "censo_agropecuario_legado"
        assert meta.parser_version == 1

    @pytest.mark.asyncio
    async def test_output_columns(self):
        xls = _build_fake_xls(_make_tecnologia_rows())
        fake_zip = _build_fake_zip(xls)

        with patch(
            "agrobr.ibge.legacy_api.ftp_client.download_legacy_zip",
            new_callable=AsyncMock,
            return_value=fake_zip,
        ):
            from agrobr.ibge import legacy_api

            df = await legacy_api.censo_agro_legado("tecnologia")

        expected = [
            "ano",
            "localidade",
            "localidade_cod",
            "tema",
            "categoria",
            "variavel",
            "valor",
            "unidade",
            "fonte",
        ]
        assert list(df.columns) == expected

    @pytest.mark.asyncio
    async def test_empty_zip_returns_empty(self):
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("readme.txt", b"no xls")
        fake_zip = buf.getvalue()

        with patch(
            "agrobr.ibge.legacy_api.ftp_client.download_legacy_zip",
            new_callable=AsyncMock,
            return_value=fake_zip,
        ):
            from agrobr.ibge import legacy_api

            df = await legacy_api.censo_agro_legado("tecnologia")

        assert len(df) == 0

    @pytest.mark.asyncio
    async def test_sorted_output(self):
        xls = _build_fake_xls(_make_tecnologia_rows())
        fake_zip = _build_fake_zip(xls)

        with patch(
            "agrobr.ibge.legacy_api.ftp_client.download_legacy_zip",
            new_callable=AsyncMock,
            return_value=fake_zip,
        ):
            from agrobr.ibge import legacy_api

            df = await legacy_api.censo_agro_legado("tecnologia", nivel="uf")

        if len(df) > 1:
            locs = df["localidade"].tolist()
            assert locs == sorted(locs)


class TestTemasLegadoHelper:
    @pytest.mark.asyncio
    async def test_returns_list(self):
        from agrobr.ibge import legacy_api

        result = await legacy_api.temas_censo_agro_legado()
        assert isinstance(result, list)
        assert len(result) == 6

    @pytest.mark.asyncio
    async def test_contains_all_temas(self):
        from agrobr.ibge import legacy_api

        result = await legacy_api.temas_censo_agro_legado()
        expected = {
            "tecnologia",
            "pessoal_ocupado",
            "maquinas",
            "producao_animal",
            "valor_producao",
            "financeiro",
        }
        assert set(result) == expected


class TestCensoAgroLegadoContract:
    def test_contract_registered(self):
        from agrobr.contracts import has_contract

        assert has_contract("censo_agropecuario_legado")

    def test_contract_validates_valid_df(self):
        from agrobr.contracts import validate_dataset

        df = pd.DataFrame(
            {
                "ano": [1995],
                "localidade": ["São Paulo"],
                "localidade_cod": [pd.NA],
                "tema": ["tecnologia"],
                "categoria": ["assistencia_tecnica"],
                "variavel": ["estabelecimentos"],
                "valor": [1000.0],
                "unidade": ["estabelecimentos"],
                "fonte": ["ibge_censo_agro_legado"],
            }
        )
        df["localidade_cod"] = df["localidade_cod"].astype("Int64")
        validate_dataset(df, "censo_agropecuario_legado")

    def test_contract_rejects_wrong_ano(self):
        from agrobr.contracts import validate_dataset
        from agrobr.exceptions import ContractViolationError

        df = pd.DataFrame(
            {
                "ano": [2017],
                "localidade": ["São Paulo"],
                "localidade_cod": [pd.NA],
                "tema": ["tecnologia"],
                "categoria": ["assistencia_tecnica"],
                "variavel": ["estabelecimentos"],
                "valor": [1000.0],
                "unidade": ["estabelecimentos"],
                "fonte": ["ibge_censo_agro_legado"],
            }
        )
        df["localidade_cod"] = df["localidade_cod"].astype("Int64")
        with pytest.raises(ContractViolationError):
            validate_dataset(df, "censo_agropecuario_legado")

    def test_contract_rejects_negative_valor(self):
        from agrobr.contracts import validate_dataset
        from agrobr.exceptions import ContractViolationError

        df = pd.DataFrame(
            {
                "ano": [1995],
                "localidade": ["São Paulo"],
                "localidade_cod": [pd.NA],
                "tema": ["tecnologia"],
                "categoria": ["assistencia_tecnica"],
                "variavel": ["estabelecimentos"],
                "valor": [-100.0],
                "unidade": ["estabelecimentos"],
                "fonte": ["ibge_censo_agro_legado"],
            }
        )
        df["localidade_cod"] = df["localidade_cod"].astype("Int64")
        with pytest.raises(ContractViolationError):
            validate_dataset(df, "censo_agropecuario_legado")

    def test_contract_version(self):
        from agrobr.contracts.ibge import IBGE_CENSO_AGRO_LEGADO_V1

        assert IBGE_CENSO_AGRO_LEGADO_V1.version == "1.0"
        assert IBGE_CENSO_AGRO_LEGADO_V1.name == "ibge.censo_agro_legado"

    def test_contract_primary_key(self):
        from agrobr.contracts.ibge import IBGE_CENSO_AGRO_LEGADO_V1

        assert IBGE_CENSO_AGRO_LEGADO_V1.primary_key == [
            "ano",
            "tema",
            "categoria",
            "variavel",
            "localidade",
        ]

    def test_contract_column_count(self):
        from agrobr.contracts.ibge import IBGE_CENSO_AGRO_LEGADO_V1

        assert len(IBGE_CENSO_AGRO_LEGADO_V1.columns) == 9

    def test_contract_guarantees(self):
        from agrobr.contracts.ibge import IBGE_CENSO_AGRO_LEGADO_V1

        assert len(IBGE_CENSO_AGRO_LEGADO_V1.guarantees) == 4
        assert any("1995" in g for g in IBGE_CENSO_AGRO_LEGADO_V1.guarantees)


class TestCensoAgroLegadoDataset:
    def test_dataset_registered(self):
        from agrobr.datasets.registry import list_datasets

        datasets = list_datasets()
        assert "censo_agropecuario_legado" in datasets

    def test_dataset_info(self):
        from agrobr.datasets.censo_agropecuario_legado import (
            CENSO_AGROPECUARIO_LEGADO_INFO,
        )

        assert CENSO_AGROPECUARIO_LEGADO_INFO.name == "censo_agropecuario_legado"
        assert CENSO_AGROPECUARIO_LEGADO_INFO.update_frequency == "never"
        assert CENSO_AGROPECUARIO_LEGADO_INFO.license == "livre"

    def test_dataset_products(self):
        from agrobr.datasets.censo_agropecuario_legado import (
            CENSO_AGROPECUARIO_LEGADO_INFO,
        )

        expected = {
            "tecnologia",
            "pessoal_ocupado",
            "maquinas",
            "producao_animal",
            "valor_producao",
            "financeiro",
        }
        assert set(CENSO_AGROPECUARIO_LEGADO_INFO.products) == expected

    def test_dataset_source_count(self):
        from agrobr.datasets.censo_agropecuario_legado import (
            CENSO_AGROPECUARIO_LEGADO_INFO,
        )

        assert len(CENSO_AGROPECUARIO_LEGADO_INFO.sources) == 1

    def test_dataset_source_name(self):
        from agrobr.datasets.censo_agropecuario_legado import (
            CENSO_AGROPECUARIO_LEGADO_INFO,
        )

        assert CENSO_AGROPECUARIO_LEGADO_INFO.sources[0].name == "ibge_censo_agro_legado"


class TestCachePolicyLegado:
    def test_policy_exists(self):
        from agrobr.cache.policies import POLICIES

        assert "ibge_censo_agro_legado" in POLICIES

    def test_policy_ttl_90_days(self):
        from agrobr.cache.policies import POLICIES, TTL

        policy = POLICIES["ibge_censo_agro_legado"]
        assert policy.ttl_seconds == TTL.DAYS_90.value

    def test_policy_not_smart_expiry(self):
        from agrobr.cache.policies import POLICIES

        policy = POLICIES["ibge_censo_agro_legado"]
        assert policy.smart_expiry is False


class TestIbgeExports:
    def test_censo_agro_legado_exported(self):
        from agrobr import ibge

        assert hasattr(ibge, "censo_agro_legado")
        assert callable(ibge.censo_agro_legado)

    def test_temas_legado_exported(self):
        from agrobr import ibge

        assert hasattr(ibge, "temas_censo_agro_legado")
        assert callable(ibge.temas_censo_agro_legado)

    def test_datasets_export(self):
        from agrobr import datasets

        assert hasattr(datasets, "censo_agropecuario_legado")
        assert callable(datasets.censo_agropecuario_legado)
