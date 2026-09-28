from __future__ import annotations

import hashlib
import io
import time
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any, Generic, Protocol, TypeVar

import httpx

from agrobr.exceptions import SourceUnavailableError
from agrobr.http import responses, retry, user_agents


class Receipt(Protocol):
    logical_index: int
    status: int | None
    fetched_at: datetime
    size_bytes: int | None
    sha256: str | None
    headers: dict[str, str]
    complete_body: bool
    error_type: str | None


ResourceT = TypeVar("ResourceT", bound=Receipt)
RoleT = TypeVar("RoleT", bound=str)


class Transport(Generic[ResourceT, RoleT]):
    def __init__(
        self,
        *,
        source: str,
        timeout: httpx.Timeout,
        initial_role: RoleT,
        resource_factory: Callable[[dict[str, Any]], ResourceT],
        body_limit: Callable[[], int],
        total_limit: Callable[[], int],
    ) -> None:
        self.source, self.timeout = source, timeout
        self._resource_factory = resource_factory
        self._body_limit, self._total_limit = body_limit, total_limit
        self.resources: list[ResourceT] = []
        self.role = initial_role
        self.logical_index = -1
        self.attempt_index = 0
        self.url = ""
        self.total_bytes = 0
        self.fetch_ms = 0

    def _request_fields(self) -> dict[str, Any]:
        return {}

    def _finished(self, _resource: ResourceT) -> None:
        pass

    def _close_failure(self, _resource: ResourceT, _exc: httpx.HTTPError) -> None:
        pass

    def session(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            timeout=self.timeout,
            headers=user_agents.UserAgentRotator.get_bot_headers(),
            follow_redirects=False,
            event_hooks={"request": [self.request]},
        )

    async def request(self, request: httpx.Request) -> None:
        if request.method != "GET" or str(request.url) != self.url:
            raise SourceUnavailableError(
                source=self.source,
                url=str(request.url),
                last_error="Requisição fora da seleção planejada",
            )
        request.extensions[f"{self.source}_resource_index"] = len(self.resources)
        self.resources.append(
            self._resource_factory(
                dict(
                    role=self.role,
                    logical_index=self.logical_index,
                    attempt_index=self.attempt_index,
                    requested_url=self.url,
                    url=str(request.url),
                    parameters=dict(request.url.params),
                    status=None,
                    fetched_at=datetime.now(UTC),
                    **self._request_fields(),
                )
            )
        )
        self.attempt_index += 1

    async def read_response(self, response: httpx.Response) -> bytes:
        index = response.request.extensions[f"{self.source}_resource_index"]
        resource = self.resources[index]
        resource.status = response.status_code
        resource.fetched_at = datetime.now(UTC)
        allowed = {
            "content-type",
            "content-length",
            "content-encoding",
            "date",
            "etag",
            "last-modified",
            "retry-after",
            "location",
        }
        resource.headers = {key: value for key, value in response.headers.items() if key in allowed}
        content = io.BytesIO()
        digest = hashlib.sha256()
        try:
            async for chunk in response.aiter_bytes():
                content.write(chunk)
                digest.update(chunk)
                self.total_bytes += len(chunk)
                if content.tell() > self._body_limit() or self.total_bytes > self._total_limit():
                    raise SourceUnavailableError(
                        source=self.source,
                        url=self.url,
                        last_error="Limite operacional de bytes excedido após chunk recebido",
                    )
            resource.complete_body = True
            return content.getvalue()
        except (httpx.HTTPError, SourceUnavailableError) as exc:
            resource.error_type = type(exc).__name__
            raise
        finally:
            resource.size_bytes = content.tell()
            resource.sha256 = digest.hexdigest()
            self._finished(resource)
            try:
                await response.aclose()
            except httpx.HTTPError as exc:
                self._close_failure(resource, exc)
                if resource.error_type is None:
                    resource.error_type = type(exc).__name__
                    raise

    async def _attempt(self, http: httpx.AsyncClient) -> httpx.Response:
        start_index = len(self.resources)
        try:
            response = await http.send(http.build_request("GET", self.url), stream=True)
            content = await self.read_response(response)
            buffered = httpx.Response(
                response.status_code, content=content, request=response.request
            )
            buffered.headers = response.headers
            return buffered
        except httpx.HTTPError as exc:
            if len(self.resources) > start_index:
                self.resources[-1].error_type = type(exc).__name__
                self._finished(self.resources[-1])
            raise

    async def fetch(
        self,
        http: httpx.AsyncClient,
        url: str,
        role: RoleT,
    ) -> bytes:
        self.logical_index += 1
        self.attempt_index = 0
        self.url, self.role = url, role
        start = time.monotonic()
        try:
            response = await retry.retry_on_status(lambda: self._attempt(http), source=self.source)
            response.raise_for_status()
            responses.raise_for_service_error(response, source=self.source, url=url)
            return response.content
        except SourceUnavailableError as exc:
            if self.resources and self.resources[-1].logical_index == self.logical_index:
                self.resources[-1].error_type = type(exc).__name__
            raise
        except httpx.HTTPError as exc:
            if self.resources and self.resources[-1].logical_index == self.logical_index:
                self.resources[-1].error_type = type(exc).__name__
            detail = (
                f"HTTP {exc.response.status_code}"
                if isinstance(exc, httpx.HTTPStatusError)
                else type(exc).__name__
            )
            raise SourceUnavailableError(
                source=self.source, url=url, last_error=f"{role}: {detail}"
            ) from exc
        finally:
            self.fetch_ms += int((time.monotonic() - start) * 1000)
