from __future__ import annotations

from pathlib import Path

import httpx
import pytest

from agrobr.exceptions import SourceUnavailableError
from agrobr.mapbiomas import client

CONFIRMATION = Path(__file__).parent / "fixtures/drive_collection11_confirmation.html"
FILE_ID = "1otOqymHuixvkRGVl65zTTNyfaHo46Gqk"


@pytest.mark.parametrize(
    "original,replacement",
    [
        (FILE_ID, "another-file"),
        ("https://drive.usercontent.google.com/download", "https://example.com/download"),
    ],
)
def test_confirmacao_drive_nao_segue_outro_arquivo_ou_host(original: str, replacement: str):
    html = CONFIRMATION.read_text(encoding="utf-8").replace(original, replacement)
    url = client._build_xlsx_url("BIOME_STATE_MUNICIPALITY", colecao=11)
    response = httpx.Response(200, text=html, request=httpx.Request("GET", url))
    with pytest.raises(SourceUnavailableError, match="Confirmação"):
        client._drive_confirmation_url(response, url)
