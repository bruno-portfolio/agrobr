from __future__ import annotations

import copy
import functools
import importlib
import inspect
import warnings
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field, replace
from datetime import date
from typing import TYPE_CHECKING, Any, ClassVar, cast

import httpx

from agrobr import _log

if TYPE_CHECKING:
    import pandas as pd

    from agrobr.models import MetaInfo

from agrobr import constants
from agrobr.datasets.deterministic import get_snapshot
from agrobr.exceptions import (
    CacheMigrationError,
    ContractViolationError,
    InvalidParameterError,
    ParseError,
    ResourceLimitError,
    SourceFallbackWarning,
    SourceUnavailableError,
)
from agrobr.utils import result as result_utils

logger = _log.get_logger(__name__)
_TIPOS_POLARS = {
    "int": "Int64",
    "float": "Float64",
    "Decimal": "Float64",
    "str": "String",
    "bool": "Boolean",
}

from agrobr.contracts import _auto_discover_contracts  # noqa: E402

_auto_discover_contracts()


@dataclass
class DatasetSource:
    name: str
    priority: int
    fetch_fn: Callable[..., Awaitable[tuple[pd.DataFrame, Any]]]
    enabled: bool = True
    description: str = ""


@dataclass
class DatasetInfo:
    name: str
    description: str
    sources: list[DatasetSource] = field(default_factory=list)
    products: list[str] = field(default_factory=list)
    contract_version: str = "1.0"
    update_frequency: str = "daily"
    typical_latency: str = "D+0"
    source_url: str = ""
    source_institution: str = ""
    min_date: str | None = None
    unit: str | None = None
    license: str = "livre"

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "sources": [s.name for s in self.sources],
            "products": list(self.products),
            "contract_version": self.contract_version,
            "update_frequency": self.update_frequency,
            "typical_latency": self.typical_latency,
            "source_url": self.source_url,
            "source_institution": self.source_institution,
            "min_date": self.min_date,
            "unit": self.unit,
            "license": self.license,
            "licenses": {s.name: constants.licenca_da_fonte(s.name) for s in self.sources},
        }


def _unpack_result(
    result: pd.DataFrame | tuple[pd.DataFrame, Any],
) -> tuple[pd.DataFrame, MetaInfo | None]:
    if isinstance(result, tuple):
        return result[0], result[1]
    return result, None


def _no_contrato(result: Any, contract_name: str | None) -> Any:
    """Põe a saída pandas na ordem do contrato e as datas do contrato em ``datetime64``.

    As colunas fora do contrato vão ao fim, na ordem da fonte. A data só é convertida quando a
    coluna ``object`` traz ``datetime.date``/``datetime.datetime``; o ``MetaInfo.columns``
    acompanha a ordem nova.
    """
    import pandas as pd

    from agrobr.contracts import ColumnType, get_contract, has_contract

    frame, meta = _unpack_result(result)
    if not isinstance(frame, pd.DataFrame) or not contract_name or not has_contract(contract_name):
        return result
    colunas = [c for c in get_contract(contract_name).columns if c.name in frame.columns]
    nomes = [c.name for c in colunas]
    ordem = [*nomes, *(c for c in frame.columns if c not in nomes)]
    datas = {
        c.name: pd.to_datetime(frame[c.name])
        for c in colunas
        if c.type in (ColumnType.DATE, ColumnType.DATETIME) and _datas_python(frame[c.name])
    }
    if not datas and ordem == list(frame.columns):
        return result
    frame = frame.assign(**datas)[ordem]
    if meta is not None and hasattr(meta, "columns"):
        meta = replace(meta, columns=list(frame.columns))
    return (frame, meta) if isinstance(result, tuple) else frame


def _datas_python(serie: pd.Series) -> bool:
    valores = serie.dropna()
    return serie.dtype == object and len(valores) > 0 and all(isinstance(v, date) for v in valores)


def _tipar_pelo_contrato(result: Any, contract_name: str | None) -> Any:
    """Dá a cada coluna do contrato o tipo polars dele e a ordem do contrato.

    Data vira ``Datetime("ns")`` quando a coluna vem ``Null`` ou ``Date``; a unidade das colunas
    ``Datetime`` com dado segue a saída do pandas. As colunas fora do contrato vão ao fim.
    """
    from agrobr.contracts import ColumnType, get_contract, has_contract

    frame, meta = _unpack_result(result)
    if (
        not type(frame).__module__.startswith("polars")
        or not contract_name
        or not has_contract(contract_name)
    ):
        return result
    pl = importlib.import_module("polars")
    datas = (ColumnType.DATE, ColumnType.DATETIME)
    tipos = {
        column.name: pl.Datetime("ns")
        if column.type in datas
        else getattr(pl, _TIPOS_POLARS[column.type])
        for column in get_contract(contract_name).columns
        if column.name in frame.columns
        and (column.type not in datas or frame.schema[column.name] in (pl.Null, pl.Date))
    }
    nomes = [c.name for c in get_contract(contract_name).columns if c.name in frame.columns]
    ordem = [*nomes, *(c for c in frame.columns if c not in nomes)]
    tipado = cast(Any, frame).cast(tipos).select(ordem)
    return (tipado, meta) if isinstance(result, tuple) else tipado


def _with_output_format(
    fetch: Callable[..., Awaitable[pd.DataFrame | tuple[pd.DataFrame, Any]]],
) -> Callable[..., Awaitable[pd.DataFrame | tuple[pd.DataFrame, Any]]]:
    signature = inspect.signature(fetch)
    native = "as_polars" in signature.parameters

    @functools.wraps(fetch)
    async def wrapped(
        self: BaseDataset, *args: Any, **kwargs: Any
    ) -> pd.DataFrame | tuple[pd.DataFrame, Any]:
        options = dict(kwargs)
        if not native:
            options["as_polars"] = kwargs.pop("as_polars", False)
        bound = signature.bind(self, *args, **kwargs)
        if "produto" in bound.arguments:
            bound.arguments["produto"] = self._produto_do_dataset(bound.arguments["produto"])
        if native:
            result = await fetch(*bound.args, **bound.kwargs)
            bound.apply_defaults()
            options = dict(bound.arguments)
            for position, (name, parameter) in enumerate(signature.parameters.items()):
                if position == 0 or parameter.kind is inspect.Parameter.VAR_POSITIONAL:
                    options.pop(name, None)
                elif parameter.kind is inspect.Parameter.VAR_KEYWORD:
                    options.update(options.pop(name, {}))
            result = _no_contrato(result, self._contract_name(**options))
        else:
            result = _no_contrato(
                await fetch(*bound.args, **bound.kwargs), self._contract_name(**options)
            )
            if options["as_polars"]:
                frame, meta = _unpack_result(result)
                result = result_utils.finalize_result(
                    frame, meta, as_polars=True, return_meta=isinstance(result, tuple)
                )
        if not options["as_polars"]:
            frame, meta = _unpack_result(result)
            em_ns = result_utils.datas_em_ns(frame)
            if em_ns is frame:
                return result
            return (em_ns, meta) if isinstance(result, tuple) else em_ns
        return cast(
            "pd.DataFrame | tuple[pd.DataFrame, Any]",
            _tipar_pelo_contrato(result, self._contract_name(**options)),
        )

    return wrapped


def _motivo_registrado_pela_fonte(meta: MetaInfo | None) -> str:
    prefixo = "source_fetch_failed: "
    falhas = [
        aviso for aviso in (meta.validation_warnings if meta else []) if aviso.startswith(prefixo)
    ]
    return f" ({falhas[0].removeprefix(prefixo)[:120]})" if falhas else ""


def aviso_deterministico(dataset: str, snapshot: str) -> str:
    return (
        f"{dataset}: o modo determinístico não se aplica a este dataset; o dado é o corrente, "
        f"e não o de {snapshot}"
    )


class BaseDataset(ABC):
    info: DatasetInfo
    honra_deterministico: ClassVar[bool] = False

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Aplica o formato de saída e o schema do contrato ao ``fetch`` de cada dataset."""
        super().__init_subclass__(**kwargs)
        fetch = cls.__dict__.get("fetch")
        if fetch is not None:
            cast(Any, cls).fetch = _with_output_format(fetch)

    def _resolve_provenance(
        self, source_name: str, source_meta: MetaInfo | None, attempted: list[str]
    ) -> tuple[str, list[str]]:
        if source_meta and (
            len(source_meta.attempted_sources) > 1 or source_meta.selected_source == "cache"
        ):
            resolved = list(dict.fromkeys(attempted[:-1] + source_meta.attempted_sources))
            return source_meta.selected_source or source_name, resolved
        return source_name, attempted

    def _build_meta(
        self,
        df: pd.DataFrame,
        source_name: str,
        source_meta: MetaInfo | None,
        attempted: list[str],
        snapshot: str | None,
        *,
        from_cache: bool = False,
        contract_name: str | None = None,
    ) -> MetaInfo:
        from agrobr.contracts import get_contract
        from agrobr.models import MetaInfo as _MetaInfo
        from agrobr.utils.time import utcnow

        now = utcnow()
        version = (
            get_contract(contract_name).version if contract_name else self.info.contract_version
        )
        selected, resolved_attempted = self._resolve_provenance(source_name, source_meta, attempted)
        data_sources = list(source_meta.data_sources) if source_meta else []
        if data_sources and "fonte" in df.columns:
            data_sources = sorted(df["fonte"].dropna().unique().tolist())
        source_details = getattr(source_meta, "source_details", {})
        avisos = list(source_meta.validation_warnings) if source_meta else []
        if snapshot is not None and not self.honra_deterministico:
            avisos.append(aviso_deterministico(self.info.name, snapshot))
        return _MetaInfo(
            source=f"datasets.{self.info.name}/{selected}",
            source_url=source_meta.source_url if source_meta else "",
            source_method="dataset",
            fetched_at=source_meta.fetched_at if source_meta else now,
            fetch_duration_ms=source_meta.fetch_duration_ms if source_meta else 0,
            parse_duration_ms=source_meta.parse_duration_ms if source_meta else 0,
            records_count=len(df),
            columns=df.columns.tolist(),
            from_cache=from_cache or bool(source_meta and source_meta.from_cache),
            cache_key=source_meta.cache_key if source_meta else None,
            cache_expires_at=source_meta.cache_expires_at if source_meta else None,
            raw_content_hash=source_meta.raw_content_hash if source_meta else None,
            raw_content_size=source_meta.raw_content_size if source_meta else 0,
            validation_passed=source_meta.validation_passed if source_meta else True,
            validation_warnings=avisos,
            parser_version=source_meta.parser_version if source_meta else 1,
            schema_version=version,
            dataset=self.info.name,
            contract_version=version,
            snapshot=snapshot,
            attempted_sources=resolved_attempted,
            selected_source=selected,
            data_sources=data_sources,
            fetch_timestamp=source_meta.fetch_timestamp if source_meta else None,
            source_details=copy.deepcopy(source_details)
            if isinstance(source_details, dict)
            else {},
        )

    @abstractmethod
    async def fetch(
        self,
        produto: str,
        return_meta: bool = False,
        **kwargs: Any,
    ) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
        pass

    def _produto_do_dataset(self, produto: Any) -> Any:
        """O nome que este dataset usa para ``produto``, por qualquer sinônimo do ``normalize``.

        Sem produto do dataset com o mesmo canônico, a entrada volta como veio, e a validação de
        cada dataset segue igual.
        """
        if not isinstance(produto, str) or not self.info.products or produto in self.info.products:
            return produto
        from agrobr.normalize import crops

        canonico = crops.normalizar_cultura(produto)
        return next(
            (nome for nome in self.info.products if crops.normalizar_cultura(nome) == canonico),
            produto,
        )

    def _validate_produto(self, produto: str) -> None:
        if produto not in self.info.products:
            raise InvalidParameterError(
                f"Produto '{produto}' não suportado por {self.info.name}. "
                f"Válidos: {self.info.products}"
            )

    def _contract_name(self, **_kwargs: Any) -> str | None:
        return self.info.name

    def _validate_contract(self, df: pd.DataFrame, **kwargs: Any) -> None:
        from agrobr.contracts import has_contract, validate_dataset

        name = self._contract_name(**kwargs)
        if name and has_contract(name):
            validate_dataset(df, name)
        else:
            logger.debug("contract_missing", dataset=self.info.name)

    async def _try_sources(
        self,
        produto: str,
        **kwargs: Any,
    ) -> tuple[pd.DataFrame, str, Any, list[str]]:
        from agrobr.contracts import get_contract, has_contract

        self._validate_produto(produto)
        snapshot = get_snapshot()
        if snapshot is not None and not self.honra_deterministico:
            warnings.warn(aviso_deterministico(self.info.name, snapshot), UserWarning, stacklevel=3)
        errors: list[tuple[str, str, str]] = []
        attempted: list[str] = []
        ultimo_erro: Exception | None = None

        for source in sorted(self.info.sources, key=lambda s: s.priority):
            if not source.enabled:
                continue

            attempted.append(source.name)

            try:
                df, meta = await source.fetch_fn(produto, **kwargs)
                logger.info(
                    "source_success",
                    dataset=self.info.name,
                    source=source.name,
                    rows=len(df),
                    attempted_sources=attempted,
                )
                if len(df) == 0:
                    logger.warning(
                        "source_empty_result",
                        dataset=self.info.name,
                        source=source.name,
                        hint="Filtros podem não casar com os dados atuais da fonte",
                    )
                    contract_name = self._contract_name(**kwargs)
                    if contract_name and has_contract(contract_name):
                        df = get_contract(contract_name).empty_frame()
                selected, resolved = self._resolve_provenance(source.name, meta, attempted)
                if len(resolved) > 1:
                    reason = (
                        f" ({errors[0][1]}: {errors[0][2][:120]})"
                        if errors
                        else _motivo_registrado_pela_fonte(meta)
                    )
                    warnings.warn(
                        f"{self.info.name}: fonte primária '{resolved[0]}' indisponível"
                        f"{reason}; usando fallback '{selected}'",
                        SourceFallbackWarning,
                        stacklevel=2,
                    )
                return df, source.name, meta, attempted

            except (TypeError, SourceFallbackWarning, CacheMigrationError, ResourceLimitError):
                raise

            except InvalidParameterError:
                raise

            except (httpx.HTTPError, httpx.TimeoutException, OSError) as e:
                logger.warning(
                    "source_network_error",
                    dataset=self.info.name,
                    source=source.name,
                    error_type="network",
                    error=str(e),
                )
                errors.append((source.name, "network", str(e)))
                ultimo_erro = e

            except ParseError as e:
                logger.warning(
                    "source_parse_error",
                    dataset=self.info.name,
                    source=source.name,
                    error_type="parse",
                    error=str(e),
                )
                errors.append((source.name, "parse", str(e)))
                ultimo_erro = e

            except ContractViolationError as e:
                logger.warning(
                    "source_contract_error",
                    dataset=self.info.name,
                    source=source.name,
                    error_type="contract",
                    error=str(e),
                )
                errors.append((source.name, "contract", str(e)))
                ultimo_erro = e

            except SourceUnavailableError as e:
                logger.warning(
                    "source_unavailable",
                    dataset=self.info.name,
                    source=source.name,
                    error_type="unavailable",
                    error=str(e),
                )
                errors.append((source.name, "unavailable", str(e)))
                ultimo_erro = e

        if isinstance(ultimo_erro, ParseError) and all(
            category == "parse" for _, category, _ in errors
        ):
            raise ParseError(
                source=f"{self.info.name}/{produto}",
                parser_version=ultimo_erro.parser_version,
                reason="Todas as fontes falharam por layout",
                errors=errors,
                attempted_sources=attempted,
            ) from ultimo_erro

        raise SourceUnavailableError(
            source=f"{self.info.name}/{produto}",
            errors=errors,
            attempted_sources=attempted,
        ) from ultimo_erro
