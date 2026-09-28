from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from agrobr.exceptions import SourceUnavailableError
from tests.helpers import levanta_exatamente, sem_excecao


class TestIsAvailable:
    def test_returns_bool(self):
        from agrobr.http.browser import is_available

        result = is_available()
        assert isinstance(result, bool)


class TestFetchWithBrowser:
    @pytest.mark.asyncio
    async def test_cloudflare_block_detected(self):
        from agrobr.http import browser

        mock_response = MagicMock()
        mock_response.status = 403

        mock_page = AsyncMock()
        mock_page.goto = AsyncMock(return_value=mock_response)
        mock_page.content = AsyncMock(return_value="<html>Cloudflare challenge</html>")
        mock_page.wait_for_selector = AsyncMock()
        mock_page.wait_for_timeout = AsyncMock()
        mock_page.add_init_script = AsyncMock()

        mock_context = AsyncMock()
        mock_context.new_page = AsyncMock(return_value=mock_page)
        mock_context.close = AsyncMock()

        mock_browser_inst = AsyncMock()
        mock_browser_inst.is_connected = MagicMock(return_value=True)
        mock_browser_inst.new_context = AsyncMock(return_value=mock_context)

        with (
            patch.object(browser, "_playwright_available", True),
            patch.object(browser, "_get_browser", AsyncMock(return_value=mock_browser_inst)),
            pytest.raises(SourceUnavailableError, match="Cloudflare"),
        ):
            await browser.fetch_with_browser("https://example.com", source="test")

    @pytest.mark.asyncio
    async def test_with_wait_selector(self):
        from agrobr.http import browser

        mock_response = MagicMock()
        mock_response.status = 200

        mock_page = AsyncMock()
        mock_page.goto = AsyncMock(return_value=mock_response)
        mock_page.content = AsyncMock(return_value="<html><table></table></html>")
        mock_page.wait_for_selector = AsyncMock()
        mock_page.wait_for_timeout = AsyncMock()
        mock_page.add_init_script = AsyncMock()

        mock_context = AsyncMock()
        mock_context.new_page = AsyncMock(return_value=mock_page)
        mock_context.close = AsyncMock()

        mock_browser_inst = AsyncMock()
        mock_browser_inst.is_connected = MagicMock(return_value=True)
        mock_browser_inst.new_context = AsyncMock(return_value=mock_context)

        with (
            patch.object(browser, "_playwright_available", True),
            patch.object(browser, "_get_browser", AsyncMock(return_value=mock_browser_inst)),
        ):
            html = await browser.fetch_with_browser(
                "https://example.com", source="test", wait_selector="table"
            )
        assert "table" in html
        mock_page.wait_for_selector.assert_called_once()

    @pytest.mark.asyncio
    async def test_wait_selector_timeout_still_returns(self):
        from agrobr.http import browser

        mock_response = MagicMock()
        mock_response.status = 200

        mock_page = AsyncMock()
        mock_page.goto = AsyncMock(return_value=mock_response)
        mock_page.content = AsyncMock(return_value="<html>content</html>")
        mock_page.wait_for_selector = AsyncMock(side_effect=Exception("timeout"))
        mock_page.wait_for_timeout = AsyncMock()
        mock_page.add_init_script = AsyncMock()

        mock_context = AsyncMock()
        mock_context.new_page = AsyncMock(return_value=mock_page)
        mock_context.close = AsyncMock()

        mock_browser_inst = AsyncMock()
        mock_browser_inst.is_connected = MagicMock(return_value=True)
        mock_browser_inst.new_context = AsyncMock(return_value=mock_context)

        with (
            patch.object(browser, "_playwright_available", True),
            patch.object(browser, "_get_browser", AsyncMock(return_value=mock_browser_inst)),
        ):
            html = await browser.fetch_with_browser(
                "https://example.com", source="test", wait_selector="table"
            )
        assert "content" in html


class TestGetBrowser:
    @pytest.mark.asyncio
    async def test_not_available_raises(self):
        from agrobr.http import browser

        with (
            patch.object(browser, "_playwright_available", False),
            pytest.raises(SourceUnavailableError, match="Playwright"),
        ):
            await browser._get_browser()


class TestCloseBrowser:
    @pytest.mark.asyncio
    async def test_close_when_active(self):
        from agrobr.http import browser

        mock_browser_inst = AsyncMock()
        mock_pw = AsyncMock()

        loop = asyncio.get_running_loop()
        with patch.object(browser, "_sessions", {loop: [(mock_pw, mock_browser_inst)]}):
            await browser.close_browser()
            mock_browser_inst.close.assert_called_once()
            mock_pw.stop.assert_called_once()
            assert not browser._sessions


def _navegador(resposta, conteudo="<html>ok</html>", goto_erro=None):
    pagina = AsyncMock()
    pagina.goto = AsyncMock(return_value=resposta, side_effect=goto_erro)
    pagina.content = AsyncMock(return_value=conteudo)
    pagina.wait_for_selector = AsyncMock()
    pagina.wait_for_timeout = AsyncMock()
    pagina.add_init_script = AsyncMock()
    contexto = AsyncMock()
    contexto.new_page = AsyncMock(return_value=pagina)
    contexto.close = AsyncMock()
    navegador = AsyncMock()
    navegador.is_connected = MagicMock(return_value=True)
    navegador.new_context = AsyncMock(return_value=contexto)
    return navegador, pagina


@pytest.mark.parametrize(
    ("resposta", "goto_erro", "mensagem"),
    [
        (None, None, "No response received"),
        (MagicMock(status=200), RuntimeError("navegador caiu"), "navegador caiu"),
    ],
)
async def test_falha_do_navegador_vira_fonte_indisponivel_com_o_motivo(
    resposta, goto_erro, mensagem
):
    from agrobr.http import browser

    navegador, _ = _navegador(resposta, goto_erro=goto_erro)
    with (
        patch.object(browser, "_playwright_available", True),
        patch.object(browser, "_get_browser", AsyncMock(return_value=navegador)),
        levanta_exatamente(SourceUnavailableError, match=mensagem),
    ):
        await browser.fetch_with_browser("https://example.com", source="test")


async def test_pagina_200_que_cita_challenge_nao_e_bloqueio_nem_espera_seletor():
    from agrobr.http import browser

    conteudo = "<html>Weekly challenge results</html>"
    navegador, pagina = _navegador(MagicMock(status=200), conteudo=conteudo)
    with (
        patch.object(browser, "_playwright_available", True),
        patch.object(browser, "_get_browser", AsyncMock(return_value=navegador)),
        sem_excecao(),
    ):
        html = await browser.fetch_with_browser("https://example.com", source="test")
    assert html == conteudo
    pagina.wait_for_selector.assert_not_awaited()
