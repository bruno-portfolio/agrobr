import warnings
from datetime import date
from pathlib import Path
from unittest.mock import AsyncMock, patch

import openpyxl
import pytest

from agrobr import datasets
from agrobr.conab._serie_historica import api
from agrobr.conab._serie_historica.api import produtos_disponiveis
from agrobr.exceptions import InvalidParameterError
from tests import helpers

LEVANTAMENTO_SET_2026 = (
    Path(__file__).resolve().parents[2]
    / "golden_data/reconciliacao_conab_20260918/7cd4df7946e5c57f.xlsx"
)
MANIFEST = helpers.load_serie_historica_manifest()


def _safras_do_levantamento(aba: str) -> set[str]:
    livro = openpyxl.load_workbook(LEVANTAMENTO_SET_2026, read_only=True)
    cabecalho = next(livro[aba].iter_rows(min_row=6, max_row=6, values_only=True))
    livro.close()
    rotulos = {str(v).removeprefix("Safra ") for v in cabecalho if str(v).startswith("Safra ")}
    return {f"20{r[:2]}/{r[3:]}" if "/" in r else r for r in rotulos}


class TestSerieHistorica:
    @pytest.mark.asyncio
    async def test_invalid_product_raises_before_download(self):
        with (
            patch.object(api.client, "download_xls", new_callable=AsyncMock) as download,
            helpers.collect_failures() as check,
        ):
            with check("erro"), pytest.raises(InvalidParameterError, match="Disponíveis"):
                await api.serie_historica("banana")
            with check("antes_da_rede"):
                assert download.await_count == 0


@pytest.mark.parametrize(
    ("produto", "aba", "hoje", "avisa"),
    [
        ("soja", "Soja", date(2026, 9, 22), True),
        ("canola", "Canola", date(2026, 9, 22), True),
        ("soja", "Soja", date(2026, 10, 15), False),
    ],
)
async def test_safras_ainda_no_levantamento_mensal_avisam_revisao(
    produto, aba, hoje, avisa, monkeypatch
):
    case = next(item for item in MANIFEST["cases"] if item["product"] == produto)
    helpers.install_serie_historica_http(monkeypatch, case)
    monkeypatch.setattr(api, "_hoje", lambda: hoje)
    with warnings.catch_warnings(record=True) as avisos:
        warnings.simplefilter("always")
        frame = await datasets.serie_historica_safra(produto)
    mensagens = [str(a.message) for a in avisos if "levantamento mensal" in str(a.message)]
    publicadas = sorted(set(frame["safra"]) & _safras_do_levantamento(aba))
    assert publicadas
    if not avisa:
        assert mensagens == []
        return
    assert len(mensagens) == 1
    assert mensagens[0].startswith(f"Safra {', '.join(publicadas)} de {produto}:")
    assert "conab.safras" in mensagens[0]


class TestProdutosDisponiveis:
    def test_contains_main_products(self):
        result = produtos_disponiveis()
        products = {item["produto"] for item in result}
        assert "soja" in products
        assert "milho" in products
        assert "arroz" in products
        assert "cafe" in products
        assert "cana" in products
        assert "feijao_caupi" in products
        assert "feijao_cores" in products
        assert "feijao_preto" in products
