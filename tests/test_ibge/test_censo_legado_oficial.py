from __future__ import annotations

import io
import zipfile
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from agrobr.contracts import validate_dataset
from agrobr.exceptions import InvalidParameterError, ParseError
from agrobr.ibge import ftp_client, legacy_api, legacy_parser

FIXTURES = Path(__file__).parents[1] / "golden_data/ibge/censo_legado_oficial"


@pytest.mark.asyncio
async def test_api_municipio_filtra_hierarquia():
    content = (FIXTURES / "Goias_Tab_11Mn.zip").read_bytes()
    with patch.object(
        ftp_client, "download_legacy_zip", new_callable=AsyncMock, return_value=content
    ):
        frame = await legacy_api.censo_agro_legado("financeiro", uf="GO", nivel="municipio")
    assert len(frame) == 232 * 4
    assert "Centro Goiano" not in frame["localidade"].tolist()
    validate_dataset(frame, "censo_agropecuario_legado")


def test_layout_sem_cabecalhos_rejeitado():
    with pytest.raises(ParseError, match="Falha ao ler XLS"):
        legacy_parser.parse_legacy_xls(b"invalid", "financeiro")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kwargs,message",
    [
        ({"nivel": "outro"}, "Nível inválido: 'outro'"),
        ({"uf": "ZZ"}, "UF inválida: 'ZZ'"),
        ({"uf": "GO", "nivel": "brasil"}, "O filtro uf exige"),
    ],
)
async def test_api_parametros_invalidos(kwargs, message):
    with (
        patch.object(ftp_client, "download_legacy_zip", new_callable=AsyncMock) as download,
        pytest.raises(InvalidParameterError, match=message),
    ):
        await legacy_api.censo_agro_legado("financeiro", **kwargs)
    download.assert_not_awaited()


@pytest.mark.asyncio
async def test_api_zip_sem_tabela_falha_explicita():
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        archive.writestr("readme.txt", "no tables")
    with (
        patch.object(
            ftp_client,
            "download_legacy_zip",
            new_callable=AsyncMock,
            return_value=output.getvalue(),
        ),
        pytest.raises(ParseError, match="sem tabelas"),
    ):
        await legacy_api.censo_agro_legado("financeiro", uf="GO")


@pytest.mark.asyncio
async def test_api_cabecalho_uf_incompativel_rejeitado():
    content = (FIXTURES / "Goias_Tab_11Mn.zip").read_bytes()
    with (
        patch.object(
            ftp_client, "download_legacy_zip", new_callable=AsyncMock, return_value=content
        ),
        pytest.raises(ParseError, match="diverge"),
    ):
        await legacy_api.censo_agro_legado("financeiro", uf="SP")
