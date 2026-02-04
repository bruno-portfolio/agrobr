"""Benchmark suite para testes de performance do agrobr."""

from __future__ import annotations

import statistics
import time
from collections.abc import Callable, Coroutine
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import structlog

logger = structlog.get_logger()


@dataclass
class BenchmarkResult:
    """Resultado de um benchmark."""

    name: str
    iterations: int
    total_time_ms: float
    mean_time_ms: float
    median_time_ms: float
    min_time_ms: float
    max_time_ms: float
    std_dev_ms: float
    times_ms: list[float] = field(default_factory=list)
    timestamp: datetime = field(default_factory=datetime.now)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Converte para dicionario."""
        return {
            "name": self.name,
            "iterations": self.iterations,
            "total_time_ms": round(self.total_time_ms, 2),
            "mean_time_ms": round(self.mean_time_ms, 2),
            "median_time_ms": round(self.median_time_ms, 2),
            "min_time_ms": round(self.min_time_ms, 2),
            "max_time_ms": round(self.max_time_ms, 2),
            "std_dev_ms": round(self.std_dev_ms, 2),
            "timestamp": self.timestamp.isoformat(),
            "metadata": self.metadata,
        }

    def summary(self) -> str:
        """Retorna resumo formatado."""
        return (
            f"{self.name}: "
            f"mean={self.mean_time_ms:.2f}ms, "
            f"median={self.median_time_ms:.2f}ms, "
            f"min={self.min_time_ms:.2f}ms, "
            f"max={self.max_time_ms:.2f}ms "
            f"({self.iterations} iterations)"
        )


@dataclass
class BenchmarkSuite:
    """Suite de benchmarks."""

    name: str
    results: list[BenchmarkResult] = field(default_factory=list)
    timestamp: datetime = field(default_factory=datetime.now)

    def add_result(self, result: BenchmarkResult) -> None:
        """Adiciona resultado."""
        self.results.append(result)

    def to_dict(self) -> dict[str, Any]:
        """Converte para dicionario."""
        return {
            "name": self.name,
            "timestamp": self.timestamp.isoformat(),
            "results": [r.to_dict() for r in self.results],
        }

    def summary(self) -> str:
        """Retorna resumo formatado."""
        lines = [f"Benchmark Suite: {self.name}", "=" * 50]
        for result in self.results:
            lines.append(result.summary())
        return "\n".join(lines)


async def benchmark_async(
    name: str,
    func: Callable[..., Coroutine[Any, Any, Any]],
    iterations: int = 10,
    warmup: int = 1,
    **kwargs: Any,
) -> BenchmarkResult:
    """
    Executa benchmark de funcao async.

    Args:
        name: Nome do benchmark
        func: Funcao async a testar
        iterations: Numero de iteracoes
        warmup: Iteracoes de aquecimento
        **kwargs: Argumentos para a funcao

    Returns:
        BenchmarkResult com estatisticas
    """
    for _ in range(warmup):
        await func(**kwargs)

    times: list[float] = []
    for _ in range(iterations):
        start = time.perf_counter()
        await func(**kwargs)
        elapsed = (time.perf_counter() - start) * 1000
        times.append(elapsed)

    return BenchmarkResult(
        name=name,
        iterations=iterations,
        total_time_ms=sum(times),
        mean_time_ms=statistics.mean(times),
        median_time_ms=statistics.median(times),
        min_time_ms=min(times),
        max_time_ms=max(times),
        std_dev_ms=statistics.stdev(times) if len(times) > 1 else 0,
        times_ms=times,
        metadata={"warmup": warmup, "kwargs": str(kwargs)},
    )


def benchmark_sync(
    name: str,
    func: Callable[..., Any],
    iterations: int = 10,
    warmup: int = 1,
    **kwargs: Any,
) -> BenchmarkResult:
    """
    Executa benchmark de funcao sincrona.

    Args:
        name: Nome do benchmark
        func: Funcao a testar
        iterations: Numero de iteracoes
        warmup: Iteracoes de aquecimento
        **kwargs: Argumentos para a funcao

    Returns:
        BenchmarkResult com estatisticas
    """
    for _ in range(warmup):
        func(**kwargs)

    times: list[float] = []
    for _ in range(iterations):
        start = time.perf_counter()
        func(**kwargs)
        elapsed = (time.perf_counter() - start) * 1000
        times.append(elapsed)

    return BenchmarkResult(
        name=name,
        iterations=iterations,
        total_time_ms=sum(times),
        mean_time_ms=statistics.mean(times),
        median_time_ms=statistics.median(times),
        min_time_ms=min(times),
        max_time_ms=max(times),
        std_dev_ms=statistics.stdev(times) if len(times) > 1 else 0,
        times_ms=times,
        metadata={"warmup": warmup, "kwargs": str(kwargs)},
    )


async def run_api_benchmarks(iterations: int = 5) -> BenchmarkSuite:
    """
    Executa benchmarks das APIs principais.

    Args:
        iterations: Numero de iteracoes por benchmark

    Returns:
        BenchmarkSuite com resultados
    """
    from agrobr import cepea, conab, ibge

    suite = BenchmarkSuite(name="agrobr_api_benchmarks")

    try:
        result = await benchmark_async(
            "cepea.indicador(soja, offline=True)",
            cepea.indicador,
            iterations=iterations,
            produto="soja",
            offline=True,
        )
        suite.add_result(result)
    except Exception as e:
        logger.warning("benchmark_failed", name="cepea.indicador", error=str(e))

    try:
        result = await benchmark_async(
            "cepea.produtos()",
            cepea.produtos,
            iterations=iterations,
        )
        suite.add_result(result)
    except Exception as e:
        logger.warning("benchmark_failed", name="cepea.produtos", error=str(e))

    try:
        result = await benchmark_async(
            "conab.produtos()",
            conab.produtos,
            iterations=iterations,
        )
        suite.add_result(result)
    except Exception as e:
        logger.warning("benchmark_failed", name="conab.produtos", error=str(e))

    try:
        result = await benchmark_async(
            "ibge.produtos_pam()",
            ibge.produtos_pam,
            iterations=iterations,
        )
        suite.add_result(result)
    except Exception as e:
        logger.warning("benchmark_failed", name="ibge.produtos_pam", error=str(e))

    return suite


def run_contract_benchmarks(iterations: int = 100) -> BenchmarkSuite:
    """
    Executa benchmarks de validacao de contratos.

    Args:
        iterations: Numero de iteracoes por benchmark

    Returns:
        BenchmarkSuite com resultados
    """
    import pandas as pd

    from agrobr.contracts.cepea import CEPEA_INDICADOR_V1

    suite = BenchmarkSuite(name="contract_validation_benchmarks")

    df_small = pd.DataFrame(
        {
            "data": pd.date_range("2024-01-01", periods=10),
            "produto": ["soja"] * 10,
            "praca": ["paranagua"] * 10,
            "valor": [150.0] * 10,
            "unidade": ["BRL/sc60kg"] * 10,
            "fonte": ["cepea"] * 10,
            "metodologia": [None] * 10,
            "anomalies": [None] * 10,
        }
    )

    result = benchmark_sync(
        "contract.validate(10 rows)",
        CEPEA_INDICADOR_V1.validate,
        iterations=iterations,
        df=df_small,
    )
    suite.add_result(result)

    df_large = pd.DataFrame(
        {
            "data": pd.date_range("2020-01-01", periods=1000),
            "produto": ["soja"] * 1000,
            "praca": ["paranagua"] * 1000,
            "valor": [150.0] * 1000,
            "unidade": ["BRL/sc60kg"] * 1000,
            "fonte": ["cepea"] * 1000,
            "metodologia": [None] * 1000,
            "anomalies": [None] * 1000,
        }
    )

    result = benchmark_sync(
        "contract.validate(1000 rows)",
        CEPEA_INDICADOR_V1.validate,
        iterations=iterations,
        df=df_large,
    )
    suite.add_result(result)

    return suite


def run_semantic_benchmarks(iterations: int = 50) -> BenchmarkSuite:
    """
    Executa benchmarks de validacao semantica.

    Args:
        iterations: Numero de iteracoes por benchmark

    Returns:
        BenchmarkSuite com resultados
    """
    import pandas as pd

    from agrobr.validators.semantic import validate_semantic

    suite = BenchmarkSuite(name="semantic_validation_benchmarks")

    df = pd.DataFrame(
        {
            "data": pd.date_range("2024-01-01", periods=100),
            "valor": [150.0 + i * 0.5 for i in range(100)],
            "produto": ["soja"] * 100,
            "produtividade": [3500.0] * 100,
            "area_plantada": [1000.0] * 100,
            "area_colhida": [950.0] * 100,
            "safra": ["2024/25"] * 100,
        }
    )

    result = benchmark_sync(
        "validate_semantic(100 rows)",
        validate_semantic,
        iterations=iterations,
        df=df,
    )
    suite.add_result(result)

    return suite


__all__ = [
    "BenchmarkResult",
    "BenchmarkSuite",
    "benchmark_async",
    "benchmark_sync",
    "run_api_benchmarks",
    "run_contract_benchmarks",
    "run_semantic_benchmarks",
]
