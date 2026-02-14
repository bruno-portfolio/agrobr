from __future__ import annotations

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
    ParseError,
    SourceUnavailableError,
)

logger = structlog.get_logger()

import agrobr.contracts.cepea  # noqa: F401, E402
import agrobr.contracts.conab  # noqa: F401, E402
import agrobr.contracts.datasets  # noqa: F401, E402
import agrobr.contracts.ibge  # noqa: F401, E402


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


class BaseDataset(ABC):
    info: DatasetInfo

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
            raise ValueError(
                f"Produto '{produto}' não suportado por {self.info.name}. "
                f"Válidos: {self.info.products}"
            )

    def _validate_contract(self, df: pd.DataFrame) -> None:
        from agrobr.contracts import has_contract, validate_dataset

        if has_contract(self.info.name):
            validate_dataset(df, self.info.name)

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
                return df, source.name, meta, attempted

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
