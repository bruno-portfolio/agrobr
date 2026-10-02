from __future__ import annotations

import hashlib
import os
from datetime import UTC, datetime
from typing import Any, Literal, TypeVar
from urllib.parse import unquote

import httpx
import pydantic

from agrobr import _log, constants
from agrobr.exceptions import InvalidParameterError, ParseError, SourceUnavailableError
from agrobr.http.retry import retry_on_status
from agrobr.http.settings import get_timeout
from agrobr.http.user_agents import UserAgentRotator

from . import acquisition, models
from . import query as query_utils

logger = _log.get_logger(__name__)
BASE_URL_AUTH = constants.URLS[constants.Fonte.COMTRADE]["auth"]
BASE_URL_GUEST = constants.URLS[constants.Fonte.COMTRADE]["guest"]
TIMEOUT = get_timeout(read=120.0)
_Envelope = TypeVar("_Envelope", bound=pydantic.BaseModel)


class _AuthenticationRejected(Exception):
    def __init__(self, status_code: int) -> None:
        self.status_code = status_code
        super().__init__("Autenticação Comtrade rejeitada")


def _get_api_key(api_key: str | None = None) -> str | None:
    key = api_key if api_key is not None else os.environ.get("AGROBR_COMTRADE_API_KEY", "")
    return key if key else None


def _build_headers(api_key: str | None) -> dict[str, str]:
    headers = UserAgentRotator.get_bot_headers()
    if api_key:
        headers["Ocp-Apim-Subscription-Key"] = api_key
    return headers


def _max_records(api_key: str | None) -> int:
    return constants.COMTRADE_AUTH_MAX_RECORDS if api_key else constants.COMTRADE_GUEST_MAX_RECORDS


def _redact(value: str | None, key: str | None) -> str | None:
    return value.replace(key, "[REDACTED]") if value is not None and key else value


def _fail(reason: str) -> ParseError:
    return ParseError(source="comtrade", parser_version=2, reason=reason)


def _has_secret(value: Any, key: str) -> bool:
    if isinstance(value, str):
        return key in value
    if isinstance(value, dict):
        return any(_has_secret(name, key) or _has_secret(item, key) for name, item in value.items())
    if isinstance(value, list):
        return any(_has_secret(item, key) for item in value)
    return False


def _parse_envelope(model: type[_Envelope], raw: bytes, key: str | None = None) -> _Envelope:
    try:
        parsed = model.model_validate_json(raw)
    except pydantic.ValidationError as exc:
        fields = sorted({str(error["loc"][0]) for error in exc.errors() if error["loc"]})
        raise _fail(f"Envelope Comtrade inválido: {fields}") from None
    if key and _has_secret(parsed.model_dump(), key):
        raise _fail("Resposta contém eco de credencial; conteúdo rejeitado")
    return parsed


async def _retry_get(
    http: httpx.AsyncClient, target: httpx.URL, headers: dict[str, str]
) -> httpx.Response:
    async def get() -> httpx.Response:
        try:
            return await http.get(target, headers=headers, follow_redirects=False)
        except httpx.TransportError as exc:
            raise type(exc)(
                "Falha de transporte Comtrade; detalhes omitidos",
                request=httpx.Request("GET", target),
            ) from None

    return await retry_on_status(get, source="comtrade")


async def _get_same_origin(
    http: httpx.AsyncClient,
    url: str,
    params: dict[str, str],
    headers: dict[str, str],
    key: str | None,
) -> httpx.Response:
    target = httpx.URL(url).copy_merge_params(params)
    origin = (target.scheme, target.host, target.port)
    for _ in range(http.max_redirects + 1):
        response = await _retry_get(http, target, headers)
        if not response.has_redirect_location:
            return response
        target = response.url.join(response.headers["location"])
        if (target.scheme, target.host, target.port) != origin or (
            key and key in unquote(str(target))
        ):
            raise SourceUnavailableError(
                source="comtrade", last_error="Redirect incompatível com transporte autorizado"
            )
    raise SourceUnavailableError(source="comtrade", last_error="Limite de redirects excedido")


def _parameters(
    query: acquisition.TradeQuery,
    partition: acquisition.TradePartition,
    limit: int,
    role: Literal["count", "data"],
) -> dict[str, str]:
    params = {
        "reporterCode": str(query.reporter),
        "flowCode": query.flow,
        "cmdCode": ",".join(partition.hs_codes),
        "period": ",".join(partition.periods),
        "includeDesc": "True",
        "partner2Code": query.partner2_code,
        "motCode": query.mot_code,
        "customsCode": query.customs_code,
        "maxRecords": str(limit),
    }
    if query.partner is not None:
        params["partnerCode"] = str(query.partner)
    if role == "count":
        params["countOnly"] = "true"
    return params


async def _request(
    http: httpx.AsyncClient,
    query: acquisition.TradeQuery,
    partition: acquisition.TradePartition,
    access: Literal["guest", "authenticated"],
    key: str | None,
    role: Literal["count", "data"],
    resources: list[acquisition.TradeResource],
) -> tuple[bytes, acquisition.TradeResource]:
    base = BASE_URL_AUTH if access == "authenticated" else BASE_URL_GUEST
    url = f"{base}/{query.type_code}/{query.freq}/{query.classification}"
    limit = _max_records(key if access == "authenticated" else None)
    params = _parameters(query, partition, limit, role)
    headers = _build_headers(key if access == "authenticated" else None)

    try:
        response = await _get_same_origin(http, url, params, headers, key)
    except (httpx.HTTPError, SourceUnavailableError):
        raise SourceUnavailableError(
            source="comtrade",
            url=url,
            last_error="Falha de transporte ou status após retry; aquisição interrompida",
        ) from None
    raw = response.content
    requested = str(httpx.Request("GET", url, params=params).url)
    resource = acquisition.TradeResource(
        partition_id=partition.partition_id,
        role=role,
        access=access,
        requested_url=_redact(requested, key) or "",
        url=_redact(str(response.url), key) or "",
        parameters=params,
        fetched_at=datetime.now(UTC),
        sha256=hashlib.sha256(raw).hexdigest(),
        size_bytes=len(raw),
        status_code=response.status_code,
        content_type=_redact(response.headers.get("content-type"), key),
        etag=_redact(response.headers.get("etag"), key),
        last_modified=_redact(response.headers.get("last-modified"), key),
        requested_limit=limit,
        effective_limit=limit if access == "guest" else None,
        limit_basis="documented_preview_limit"
        if access == "guest"
        else "requested_authenticated_limit_unverified",
    )
    resources.append(resource)
    if response.status_code in {401, 403} and access == "authenticated":
        raise _AuthenticationRejected(response.status_code)
    if not response.is_success:
        raise SourceUnavailableError(
            source="comtrade",
            url=resource.url,
            last_error=f"HTTP {response.status_code}; aquisição interrompida",
        )
    if key and key.encode() in raw:
        raise _fail("Resposta contém eco de credencial; conteúdo rejeitado")
    return raw, resource


def _record_key(record: models.TradeRecord) -> tuple[Any, ...]:
    raw = record.model_dump(by_alias=True)
    return tuple(
        raw[name]
        for name in (
            "period",
            "reporterCode",
            "partnerCode",
            "cmdCode",
            "flowCode",
            "classificationCode",
        )
    )


def _validate_records(
    records: list[models.TradeRecord],
    query: acquisition.TradeQuery,
    partition: acquisition.TradePartition,
) -> None:
    seen: set[tuple[Any, ...]] = set()
    for record in records:
        raw = record.model_dump(by_alias=True)
        expected = {
            "typeCode": query.type_code,
            "freqCode": query.freq,
            "reporterCode": query.reporter,
            "flowCode": query.flow,
            "partner2Code": int(query.partner2_code),
            "motCode": int(query.mot_code),
            "customsCode": query.customs_code,
        }
        if query.partner is not None:
            expected["partnerCode"] = query.partner
        if any(raw.get(name) != value for name, value in expected.items()):
            raise _fail("Registro com dimensões incompatíveis com a consulta")
        if raw["period"] not in partition.periods or raw["cmdCode"] not in partition.hs_codes:
            raise _fail("Registro fora do período ou HS solicitado")
        if raw.get("classificationSearchCode") != query.classification:
            raise _fail("Classificação de busca incompatível")
        identity = _record_key(record)
        if identity in seen:
            raise _fail("Chave de registro duplicada na aquisição")
        seen.add(identity)


async def _data(
    http: httpx.AsyncClient,
    query: acquisition.TradeQuery,
    partition: acquisition.TradePartition,
    access: Literal["guest", "authenticated"],
    key: str | None,
    resources: list[acquisition.TradeResource],
) -> tuple[list[models.TradeRecord], acquisition.TradeResource]:
    raw, resource = await _request(http, query, partition, access, key, "data", resources)
    envelope = _parse_envelope(acquisition.TradeEnvelope, raw, key)
    resource.received_count = len(envelope.data)
    resource.reported_count = envelope.count
    _validate_records(envelope.data, query, partition)
    return envelope.data, resource


async def _refine(
    http: httpx.AsyncClient,
    query: acquisition.TradeQuery,
    partition: acquisition.TradePartition,
    access: Literal["guest", "authenticated"],
    key: str | None,
    resources: list[acquisition.TradeResource],
    records: list[models.TradeRecord],
    resource: acquisition.TradeResource,
    expected: int | None = None,
) -> tuple[list[models.TradeRecord], list[str]]:
    if expected is not None and len(records) > expected:
        raise _fail("Quantidade de dados excede contagem independente")
    truncated = (
        len(records) < expected
        if expected is not None
        else len(records) >= (resource.effective_limit or resource.requested_limit)
    )
    children = query_utils.split_partition(partition) if truncated else []
    if not children:
        resource.accepted = True
        return records, [partition.partition_id]
    resource.replaced_by = [child.partition_id for child in children]
    accepted: list[models.TradeRecord] = []
    leaves: list[str] = []
    for child in children:
        child_records, child_resource = await _data(http, query, child, access, key, resources)
        resolved, child_leaves = await _refine(
            http, query, child, access, key, resources, child_records, child_resource
        )
        accepted.extend(resolved)
        leaves.extend(child_leaves)
    _validate_records(accepted, query, partition)
    parent_by_key = {_record_key(record): record for record in records}
    accepted_keys = {_record_key(record) for record in accepted}
    if not parent_by_key.keys() <= accepted_keys:
        raise _fail("Chave da resposta pai desapareceu na união das partições")
    for record in accepted:
        parent = parent_by_key.get(_record_key(record))
        if parent is not None and parent != record:
            raise _fail("Registro revisado entre resposta pai e partições")
    if expected is not None and len(accepted) > expected:
        raise _fail("União de partições excede contagem independente")
    return accepted, leaves


async def _run_plan(
    http: httpx.AsyncClient,
    query: acquisition.TradeQuery,
    access: Literal["guest", "authenticated"],
    key: str | None,
    resources: list[acquisition.TradeResource],
) -> tuple[list[models.TradeRecord], acquisition.TradeCoverage]:
    partitions = query_utils.plan_partitions(query, access)
    accepted: list[models.TradeRecord] = []
    coverage: list[acquisition.PartitionCoverage] = []
    for partition in partitions:
        count_raw, count_resource = await _request(
            http, query, partition, access, key, "count", resources
        )
        count = _parse_envelope(acquisition.TradeCountResult, count_raw, key)
        count_resource.reported_count = count.count
        count_resource.accepted = True
        records, resource = await _data(http, query, partition, access, key, resources)
        records, leaves = await _refine(
            http, query, partition, access, key, resources, records, resource, count.count
        )
        state: Literal["complete", "partial", "unknown"] = (
            "complete" if len(records) == count.count else "partial"
        )
        coverage.append(
            acquisition.PartitionCoverage(
                partition_id=partition.partition_id,
                state=state,
                reason="independent_count_matches_disjoint_union"
                if state == "complete"
                else "independent_count_exceeds_available_leaves",
                received_count=len(records),
                expected_count=count.count,
                limit=resource.effective_limit,
                saturated=resource.effective_limit is not None
                and resource.received_count == resource.effective_limit,
                leaf_partition_ids=leaves,
            )
        )
        accepted.extend(records)
    _validate_records(accepted, query, query_utils.make_partition(query.periods, query.hs_codes))
    state = "partial" if any(item.state == "partial" for item in coverage) else "complete"
    return accepted, acquisition.TradeCoverage(
        state=state,
        basis="independent_count_and_disjoint_partitions",
        initial_partitions=len(partitions),
        completed_partitions=sum(len(item.leaf_partition_ids) for item in coverage),
        received_count=len(accepted),
        expected_count=sum(item.expected_count or 0 for item in coverage),
        partitions=coverage,
    )


async def fetch_trade_acquisition(
    query: acquisition.TradeQuery,
    *,
    api_key: str | None = None,
    require_complete: bool = False,
) -> acquisition.TradeAcquisition:
    query_utils.validate_access_options(api_key, require_complete)
    if not isinstance(query, acquisition.TradeQuery):
        raise InvalidParameterError("query deve ser TradeQuery validada")
    try:
        query = acquisition.TradeQuery.model_validate(query.model_dump())
    except pydantic.ValidationError:
        raise InvalidParameterError("Consulta Comtrade inválida") from None
    key = _get_api_key(api_key)
    query_utils.validate_access_options(key, require_complete)
    resources: list[acquisition.TradeResource] = []
    attempts = ["comtrade_authenticated"] if key else ["comtrade_guest"]
    warnings = []
    fallback = None
    access: Literal["guest", "authenticated"] = "authenticated" if key else "guest"
    async with httpx.AsyncClient(timeout=TIMEOUT, follow_redirects=True) as http:
        try:
            records, coverage = await _run_plan(http, query, access, key, resources)
        except _AuthenticationRejected as exc:
            for resource in resources:
                resource.accepted = False
            warnings.append(
                f"Chave do Comtrade recusada (HTTP {exc.status_code}): confira "
                "AGROBR_COMTRADE_API_KEY ou o argumento api_key=; plano reiniciado integralmente "
                "no preview público."
            )
            fallback = {
                "from": "authenticated",
                "to": "guest",
                "reason": "authentication_rejected",
                "status_code": exc.status_code,
            }
            logger.warning("comtrade_auth_failed_fallback_guest", source="comtrade")
            access = "guest"
            attempts.append("comtrade_guest")
            records, coverage = await _run_plan(http, query, access, None, resources)
    if coverage.state != "complete":
        warnings.append(
            f"Cobertura {coverage.state}: {coverage.received_count} registros de {coverage.expected_count} contados."
        )
        if require_complete:
            raise SourceUnavailableError(
                source="comtrade",
                last_error=f"Cobertura {coverage.state}; require_complete exige completude",
                attempted_sources=attempts,
            )
    return acquisition.TradeAcquisition(
        query=query,
        records=records,
        resources=resources,
        coverage=coverage,
        attempted_sources=attempts,
        selected_source=f"comtrade_{access}",
        warnings=warnings,
        fallback=fallback,
    )
