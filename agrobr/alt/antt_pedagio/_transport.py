from __future__ import annotations

import asyncio
import hashlib
import re
import tempfile
from contextlib import ExitStack
from datetime import UTC, datetime
from typing import BinaryIO, Literal, cast

import httpx

from agrobr import constants
from agrobr.exceptions import ParseError, ResourceLimitError, SourceUnavailableError
from agrobr.http import responses, retry

from . import acquisition, catalog


class _StopRetry(Exception):
    def __init__(self, primary: Exception) -> None:
        super().__init__(str(primary))
        self.primary = primary


class Transport:
    def __init__(self, bundle: acquisition.TrafegoAcquisition) -> None:
        self.bundle = bundle
        self.logical_index = -1

    def fail(self, url: str, reason: str) -> SourceUnavailableError:
        return SourceUnavailableError(source="antt_pedagio", url=url, last_error=reason)

    def _validate_headers(self, response: httpx.Response, size: int, expected: int | None) -> None:
        if response.status_code != 200:
            responses.raise_for_status(response, source="antt_pedagio")
            raise self.fail(
                str(response.url), f"HTTP {response.status_code}; esperado corpo completo HTTP 200"
            )
        length = response.headers.get("content-length")
        if length is not None:
            if re.fullmatch(r"[0-9]+", length) is None:
                raise self.fail(str(response.url), "Content-Length inválido")
            if (length.lstrip("0") or "0") != str(size):
                raise self.fail(
                    str(response.url),
                    f"Content-Length divergente: anunciado={length}, recebido={size}",
                )
        content_range = response.headers.get("content-range")
        if content_range is not None:
            match = re.fullmatch(r"bytes 0-([0-9]+)/([0-9]+)", content_range)
            if (
                match is None
                or (match[1].lstrip("0") or "0") != str(size - 1)
                or (match[2].lstrip("0") or "0") != str(size)
            ):
                raise self.fail(str(response.url), "Content-Range não descreve o recurso completo")
        if expected is not None and size != expected:
            raise self.fail(
                str(response.url), f"Tamanho CKAN divergente: anunciado={expected}, recebido={size}"
            )

    async def _attempt(
        self,
        http: httpx.AsyncClient,
        url: str,
        role: Literal["catalog", "trafego", "pracas"],
        file: BinaryIO,
        limit: int,
        expected: int | None,
        resource_id: str | None,
        year: int | None,
        attempt: int,
    ) -> httpx.Response:
        file.seek(0)
        file.truncate()
        receipt = acquisition.AttemptReceipt(
            role=role,
            logical_index=self.logical_index,
            attempt=attempt,
            url=url,
            resource_id=resource_id,
            year=year,
            started_at=datetime.now(UTC),
        )
        self.bundle.attempts.append(receipt)
        digest = hashlib.sha256()
        response: httpx.Response | None = None
        primary: BaseException | None = None
        try:
            response = await http.send(
                http.build_request("GET", url, headers={"Accept-Encoding": "identity"}), stream=True
            )
            receipt.status = response.status_code
            allowed = {
                "content-type",
                "content-length",
                "content-range",
                "content-encoding",
                "date",
                "etag",
                "last-modified",
                "retry-after",
                "location",
            }
            receipt.headers = {
                key: value for key, value in response.headers.items() if key in allowed
            }
            encoding = response.headers.get("content-encoding", "identity").strip().lower()
            if encoding not in ("", "identity"):
                raise self.fail(
                    url,
                    f"Content-Encoding inesperado: {encoding}; identity obrigatório antes da leitura",
                )
            if response.status_code == 206:
                raise self.fail(url, "HTTP 206 inesperado; aquisição exige HTTP 200 completo")
            async for chunk in response.aiter_bytes():
                digest.update(chunk)
                receipt.size_bytes += len(chunk)
                self.bundle.transfer_bytes += len(chunk)
                if (
                    receipt.size_bytes > limit
                    or self.bundle.transfer_bytes > constants.ANTT_MAX_TRANSFER_BYTES
                    or (
                        role != "catalog"
                        and self.bundle.spool_bytes + receipt.size_bytes
                        > constants.ANTT_MAX_SPOOL_BYTES
                    )
                ):
                    raise ResourceLimitError(
                        "antt_pedagio", "Limite de bytes excedido após chunk recebido", url=url
                    )
                view = memoryview(chunk)
                for offset in range(0, len(view), constants.ANTT_STREAM_CHUNK_BYTES):
                    file.write(view[offset : offset + constants.ANTT_STREAM_CHUNK_BYTES])
            receipt.complete_body = True
            if response.status_code == 200:
                self._validate_headers(response, receipt.size_bytes, expected)
                file.seek(0)
                prefix = file.read(256).lstrip(b"\xef\xbb\xbf \t\r\n").lower()
                file.seek(0)
                if prefix.startswith((b"<!doctype", b"<html")):
                    raise self.fail(url, "HTML em vez do recurso ANTT; possível WAF ou manutenção")
                if not receipt.size_bytes:
                    raise ParseError(
                        source="antt_pedagio",
                        parser_version=3,
                        reason="Corpo vazio",
                    )
            elif not retry.should_retry_status(response.status_code):
                responses.raise_for_status(response, source="antt_pedagio")
            return response
        except (
            httpx.HTTPError,
            ParseError,
            SourceUnavailableError,
            ResourceLimitError,
            OSError,
            asyncio.CancelledError,
        ) as exc:
            primary = exc
            receipt.error_type = type(exc).__name__
            receipt.error_message = str(exc)
            if isinstance(exc, httpx.CloseError):
                receipt.close_error_type = type(exc).__name__
                receipt.close_error_message = str(exc)
            raise
        finally:
            close_error: httpx.HTTPError | OSError | None = None
            try:
                if response is not None:
                    try:
                        await response.aclose()
                    except (httpx.HTTPError, OSError) as exc:
                        close_error = exc
                        receipt.close_error_type = type(exc).__name__
                        receipt.close_error_message = str(exc)
                    receipt.closed = response.is_closed
            finally:
                receipt.sha256 = digest.hexdigest()
                receipt.finished_at = datetime.now(UTC)
            if close_error is not None and primary is None:
                raise self.fail(
                    url, f"Falha ao fechar resposta: {type(close_error).__name__}"
                ) from close_error

    async def fetch(
        self,
        http: httpx.AsyncClient,
        url: str,
        *,
        role: Literal["catalog", "trafego", "pracas"],
        limit: int,
        expected: int | None = None,
        resource_id: str | None = None,
        year: int | None = None,
    ) -> tuple[BinaryIO, int]:
        catalog.validate_download_url(url)
        if expected is not None and (
            expected > limit
            or (
                role != "catalog"
                and self.bundle.spool_bytes + expected > constants.ANTT_MAX_SPOOL_BYTES
            )
        ):
            raise ResourceLimitError(
                "antt_pedagio", "Tamanho CKAN excede orçamento antes do GET", url=url
            )
        self.logical_index += 1
        attempt = 0
        with ExitStack() as stack:
            temporary = stack.enter_context(tempfile.TemporaryFile(mode="w+b"))
            file = cast(BinaryIO, temporary)

            async def receive() -> httpx.Response:
                nonlocal attempt
                attempt += 1
                try:
                    return await self._attempt(
                        http, url, role, file, limit, expected, resource_id, year, attempt
                    )
                except retry.RETRIABLE_EXCEPTIONS as exc:
                    if self.bundle.attempts[-1].close_error_type is not None:
                        raise _StopRetry(exc) from exc
                    raise

            try:
                response = await retry.retry_on_status(
                    receive,
                    source="antt_pedagio",
                    max_attempts=constants.ANTT_MAX_ATTEMPTS,
                    base_delay=constants.ANTT_RETRY_BASE_SECONDS,
                )
                if response.status_code != 200:
                    raise self.fail(url, f"HTTP {response.status_code}; esperado 200")
                file.seek(0)
                stack.pop_all()
                return file, len(self.bundle.attempts) - 1
            except BaseException as exc:
                primary = exc.primary if isinstance(exc, _StopRetry) else exc
                try:
                    stack.close()
                except OSError as close_error:
                    self.bundle.spool_close_errors.append(
                        {
                            "file_index": None,
                            "resource_id": resource_id,
                            "ano": year,
                            "error_type": type(close_error).__name__,
                            "error_message": str(close_error),
                            "at": datetime.now(UTC).isoformat(),
                        }
                    )
                finally:
                    primary.__dict__["antt_acquisition"] = self.bundle.details()
                if primary is not exc:
                    raise primary from None
                raise
