from __future__ import annotations

import io
import zipfile
from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx
import pytest
from bs4 import BeautifulSoup

from agrobr.exceptions import SourceUnavailableError
from agrobr.ibge import ftp_client, legacy_parser
from tests.helpers import make_mock_async_client, make_mock_response

FIXTURES = Path(__file__).parents[1] / "golden_data/ibge/censo_legado_oficial"


def test_hifens_oficiais_html_representam_zero():
    count = 0
    for table, theme in [
        (3, "tecnologia"),
        (6, "pessoal_ocupado"),
        (7, "maquinas"),
        (9, "producao_animal"),
    ]:
        name, data = ftp_client.extract_tables_from_zip(
            (FIXTURES / f"Goias_Tab_{table}Mn.zip").read_bytes()
        )[0]
        soup = BeautifulSoup(data.decode("cp1252"), "lxml")
        frame = legacy_parser.parse_legacy_html(data, theme, "GO", name)
        for row_index, label in enumerate(soup.select('td[align="left"]')):
            cells = label.find_parent("tr").find_all("td")
            width = len(cells) - 1
            for column, cell in enumerate(cells[1:]):
                if cell.get_text().strip() == "-":
                    assert frame.iloc[row_index * width + column]["valor"] == 0
                    count += 1
    assert count == 447


@pytest.mark.asyncio
async def test_erro_http_preserva_causa_e_url():
    response = make_mock_response(status_code=404)
    response.raise_for_status.side_effect = httpx.HTTPStatusError(
        "not found", request=httpx.Request("GET", "https://ftp.ibge.gov.br"), response=response
    )
    with (
        patch.object(ftp_client, "retry_on_status", new_callable=AsyncMock, return_value=response),
        patch.object(ftp_client.httpx, "AsyncClient", return_value=make_mock_async_client()),
        pytest.raises(SourceUnavailableError, match="/Acre/tab_3mn.zip") as error,
    ):
        await ftp_client.download_legacy_zip("Tab_3", "Acre")
    assert isinstance(error.value.__cause__, httpx.HTTPStatusError)


def test_zip_legado_ignora_membros_que_nao_sao_tabela():
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        for name in ("Tab_3Mn.XLS", "leiame.txt", "tab_3mn.htm", "notas.pdf"):
            archive.writestr(name, name)
    tables = ftp_client.extract_tables_from_zip(output.getvalue())
    assert [name for name, _data in tables] == ["Tab_3Mn.XLS", "tab_3mn.htm"]
