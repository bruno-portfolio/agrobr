from __future__ import annotations

import asyncio
import threading
import time
from datetime import date, datetime

import httpx

from agrobr import _log
from agrobr.constants import MIN_CSV_SIZE, MIN_ZIP_SIZE, URLS, Fonte, HTTPSettings
from agrobr.exceptions import InvalidParameterError, ResourceLimitError, SourceUnavailableError
from agrobr.http import responses
from agrobr.http.retry import retry_on_status
from agrobr.http.settings import get_timeout
from agrobr.http.user_agents import UserAgentRotator
from agrobr.utils import io as io_utils

logger = _log.get_logger(__name__)

BASE_URL_ZIP = URLS[Fonte.B3]["ajustes_zip"]
BASE_URL_ARQUIVOS = URLS[Fonte.B3]["arquivos"]

TIMEOUT = get_timeout()
TIMEOUT_DOWNLOAD = get_timeout(read=120.0)
_OI_LOCK = threading.Lock()
_OI_POLL_SECONDS = 0.05
_oi_vezes = 0


class PregaoNaoPublicadoError(SourceUnavailableError):
    def __init__(self, url: str, data: str, conteudo: bytes) -> None:
        super().__init__(
            source="b3",
            url=url,
            last_error=f"ZIP vazio ({len(conteudo)} bytes) — pregão de {data} ainda não publicado",
        )
        self.conteudo = conteudo


def validate_oi_date(value: str) -> date:
    try:
        parsed = date.fromisoformat(value)
        if parsed.isoformat() != value:
            raise ValueError(value)
        return parsed
    except (TypeError, ValueError) as exc:
        raise InvalidParameterError("data deve ser uma data válida no formato AAAA-MM-DD") from exc


async def fetch_ajustes_zip(data: str) -> tuple[bytes, str]:
    dt = datetime.strptime(data, "%d/%m/%Y")
    filename = f"PR{dt.strftime('%y%m%d')}.zip"
    url = f"{BASE_URL_ZIP}?filelist={filename}"

    async with httpx.AsyncClient(
        timeout=TIMEOUT_DOWNLOAD,
        headers=UserAgentRotator.get_bot_headers(),
        follow_redirects=True,
    ) as http:
        logger.debug("b3_zip_request", url=url)
        response = await retry_on_status(
            lambda: http.get(url),
            source="b3",
        )

        if response.status_code == 404:
            raise SourceUnavailableError(source="b3", url=url, last_error="HTTP 404")

        responses.raise_for_status(response, source="b3")
        content = response.content

        if len(content) <= 100:
            raise PregaoNaoPublicadoError(url, data, content)
        io_utils.validate_download(
            content,
            kinds=("zip",),
            source="b3",
            url=url,
            min_size=MIN_ZIP_SIZE,
        )

        logger.info("b3_zip_fetch_ok", source="b3", size=len(content))
        return content, url


async def fetch_posicoes_abertas(data: str) -> tuple[bytes, str]:
    validate_oi_date(data)
    await _ocupar_vez_oi()
    try:
        return await _fetch_posicoes_abertas(data)
    finally:
        _OI_LOCK.release()


async def _ocupar_vez_oi() -> None:
    """Espera a vez do par token→CSV, com teto de ``timeout_read`` sem a vez trocar de mãos.

    Numa fila que anda (``gather`` de várias datas, várias threads), a vez troca a cada consulta e o
    prazo recomeça. Se o dono não solta (tarefa do loop parado pelo ``agrobr.sync``), seguir sem a
    vez arriscaria invalidar o token dele. A espera levanta ``ResourceLimitError``, e não
    ``SourceUnavailableError``, para quem busca o pregão recente não tomar a vez ocupada por pregão
    não publicado e devolver um dia mais velho.
    """
    global _oi_vezes
    teto = HTTPSettings().timeout_read
    vista, prazo = _oi_vezes, time.monotonic() + teto
    while not _OI_LOCK.acquire(blocking=False):
        if _oi_vezes != vista:
            vista, prazo = _oi_vezes, time.monotonic() + teto
        elif time.monotonic() >= prazo:
            raise ResourceLimitError(
                source="b3",
                url=BASE_URL_ARQUIVOS,
                reason=(
                    "outra consulta das posições em aberto segura a vez do par token→CSV há mais de "
                    f"{teto:g} s sem soltar, em geral uma tarefa do loop que chamou o agrobr.sync; "
                    "repita a consulta ou serialize as chamadas, e dentro de um loop prefira o await "
                    "na API async"
                ),
            )
        await asyncio.sleep(_OI_POLL_SECONDS)
    _oi_vezes += 1


def ticket_url(data: str) -> str:
    return (
        f"{BASE_URL_ARQUIVOS}/requestname"
        f"?fileName=DerivativesOpenPosition&date={data}&recaptchaToken="
    )


async def _fetch_posicoes_abertas(data: str) -> tuple[bytes, str]:
    token_url = ticket_url(data)
    async with httpx.AsyncClient(
        timeout=TIMEOUT_DOWNLOAD, headers=UserAgentRotator.get_bot_headers(), follow_redirects=True
    ) as http:
        logger.debug("b3_oi_token_request", url=token_url)
        token_resp = await retry_on_status(
            lambda: http.get(token_url),
            source="b3_arquivos",
        )

        if token_resp.status_code in (400, 404):
            logger.info("b3_oi_nao_publicado", data=data, status=token_resp.status_code)
            return b"", token_url
        responses.raise_for_status(token_resp, source="b3")

        token_data = responses.parse_json_response(token_resp, source="b3", url=token_url)
        token = token_data.get("token")
        if not token:
            raise SourceUnavailableError(
                source="b3", url=token_url, last_error="Token vazio na resposta"
            )

        download_url = f"{BASE_URL_ARQUIVOS}?token={token}"
        logger.debug("b3_oi_download", url=BASE_URL_ARQUIVOS)
        csv_resp = await retry_on_status(
            lambda: http.get(download_url),
            source="b3_arquivos",
        )

        if csv_resp.status_code == 404:
            logger.info("b3_oi_nao_publicado", data=data, status=404)
            return b"", token_url
        if csv_resp.status_code == 400:
            raise SourceUnavailableError(
                source="b3", url=BASE_URL_ARQUIVOS, last_error=f"HTTP 400: {csv_resp.text[:500]}"
            )
        responses.raise_for_status(csv_resp, source="b3")

        csv_bytes = csv_resp.content
        if len(csv_bytes) < MIN_CSV_SIZE:
            raise SourceUnavailableError(
                source="b3",
                url=BASE_URL_ARQUIVOS,
                last_error=(
                    f"Posicoes abertas CSV too small ({len(csv_bytes)} bytes), "
                    f"expected derivative position data"
                ),
            )

        logger.info("b3_oi_fetch_ok", source="b3", size=len(csv_bytes))
        return csv_bytes, str(csv_resp.url).replace(token, "[REDACTED]")
