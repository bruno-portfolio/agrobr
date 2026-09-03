from __future__ import annotations

import warnings
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import httpx
import structlog

if TYPE_CHECKING:
    import pandas as pd

    from agrobr.models import MetaInfo

from agrobr.exceptions import (
    ContractViolationError,
    InvalidParameterError,
    ParseError,
    SourceFallbackWarning,
    SourceUnavailableError,
)

logger = structlog.get_logger()

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
            "products": self.products,
            "contract_version": self.contract_version,
            "update_frequency": self.update_frequency,
            "typical_latency": self.typical_latency,
            "source_url": self.source_url,
            "source_institution": self.source_institution,
            "min_date": self.min_date,
            "unit": self.unit,
            "license": self.license,
        }


def _unpack_result(
    result: pd.DataFrame | tuple[pd.DataFrame, Any],
) -> tuple[pd.DataFrame, MetaInfo | None]:
    if isinstance(result, tuple):
        return result[0], result[1]
    return result, None


class BaseDataset(ABC):
    info: DatasetInfo

    def _build_meta(
        self,
        df: pd.DataFrame,
        source_name: str,
        source_meta: MetaInfo | None,
        attempted: list[str],
        snapshot: str | None,
        *,
        from_cache: bool = False,
    ) -> MetaInfo:
        from agrobr.models import MetaInfo as _MetaInfo
        from agrobr.utils.time import utcnow

        now = utcnow()
        selected = source_name
        resolved_attempted = attempted
        if source_meta and len(source_meta.attempted_sources) > 1:
            resolved_attempted = attempted[:-1] + source_meta.attempted_sources
            selected = source_meta.selected_source
        return _MetaInfo(
            source=f"datasets.{self.info.name}/{selected}",
            source_url=source_meta.source_url if source_meta else "",
            source_method="dataset",
            fetched_at=source_meta.fetched_at if source_meta else now,
            records_count=len(df),
            columns=df.columns.tolist(),
            from_cache=from_cache or bool(source_meta and source_meta.from_cache),
            parser_version=source_meta.parser_version if source_meta else 1,
            dataset=self.info.name,
            contract_version=self.info.contract_version,
            snapshot=snapshot,
            attempted_sources=resolved_attempted,
            selected_source=selected,
            fetch_timestamp=now,
        )

    @abstractmethod
    async def fetch(
        self,
        produto: str,
        return_meta: bool = False,
        **kwargs: Any,
    ) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
        pass

    def _validate_produto(self, produto: str) -> None:
        if produto not in self.info.products:
            raise InvalidParameterError(
                f"Produto '{produto}' não suportado por {self.info.name}. "
                f"Válidos: {self.info.products}"
            )

    def _validate_contract(self, df: pd.DataFrame) -> None:
        from agrobr.contracts import has_contract, validate_dataset

        if has_contract(self.info.name):
            validate_dataset(df, self.info.name)
        else:
            logger.debug("contract_missing", dataset=self.info.name)

    async def _try_sources(
        self,
        produto: str,
        **kwargs: Any,
    ) -> tuple[pd.DataFrame, str, Any, list[str]]:
        self._validate_produto(produto)
        errors: list[tuple[str, str, str]] = []
        attempted: list[str] = []

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
                        hint="Filtros podem nao casar com os dados atuais da fonte",
                    )
                if len(attempted) > 1:
                    warnings.warn(
                        f"{self.info.name}: fonte primária '{attempted[0]}' indisponível "
                        f"({errors[0][1]}: {errors[0][2][:120]}); usando fallback "
                        f"'{source.name}'",
                        SourceFallbackWarning,
                        stacklevel=2,
                    )
                return df, source.name, meta, attempted

            except TypeError:
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

            except ParseError as e:
                logger.warning(
                    "source_parse_error",
                    dataset=self.info.name,
                    source=source.name,
                    error_type="parse",
                    error=str(e),
                )
                errors.append((source.name, "parse", str(e)))

            except ContractViolationError as e:
                logger.warning(
                    "source_contract_error",
                    dataset=self.info.name,
                    source=source.name,
                    error_type="contract",
                    error=str(e),
                )
                errors.append((source.name, "contract", str(e)))

            except SourceUnavailableError as e:
                logger.warning(
                    "source_unavailable",
                    dataset=self.info.name,
                    source=source.name,
                    error_type="unavailable",
                    error=str(e),
                )
                errors.append((source.name, "unavailable", str(e)))

            except Exception as e:
                logger.warning(
                    "source_unexpected_error",
                    dataset=self.info.name,
                    source=source.name,
                    error_type="unexpected",
                    error=str(e),
                )
                errors.append((source.name, "unexpected", str(e)))

        raise SourceUnavailableError(
            source=f"{self.info.name}/{produto}",
            errors=errors,
        )
