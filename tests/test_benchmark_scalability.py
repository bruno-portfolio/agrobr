"""Benchmark de escalabilidade e performance do agrobr.

Seções:
  1. Memory Profiling — baseline, datasets grandes, leak detection
  2. Volume de Dados — parsers 1x/10x/100x, complexidade
  3. Cache DuckDB sob Carga — concorrência, volume, TTL
  4. Rate Limiting e Concorrência HTTP — semáforo, multi-fonte, backoff
  5. Async Performance — event loop blocking, fase breakdown
  6. Sync Wrapper — stress sequencial, overhead

Roda com:  pytest tests/test_benchmark_scalability.py -v -s
"""

from __future__ import annotations

import asyncio
import contextlib
import gc
import os
import pickle
import statistics
import time
import tracemalloc
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from agrobr.cache.duckdb_store import DuckDBStore
from agrobr.constants import CacheSettings, Fonte, HTTPSettings
from agrobr.http.rate_limiter import RateLimiter
from agrobr.models import Indicador

GOLDEN_DIR = Path(__file__).parent / "golden_data"

FLAG_THRESHOLD_MS = 1000
MEMORY_LEAK_TOLERANCE_KB = 512


def _fmt(ms: float) -> str:
    if ms < 1:
        return f"{ms * 1000:.0f}µs"
    if ms < 1000:
        return f"{ms:.1f}ms"
    return f"{ms / 1000:.2f}s"


def _mb(b: int) -> float:
    return b / (1024 * 1024)


def _kb(b: int) -> float:
    return b / 1024


# ============================================================================
# Helpers — synthetic data generators
# ============================================================================


def _generate_cepea_html(num_rows: int) -> str:
    rows = []
    base = date(2024, 1, 1)
    for i in range(num_rows):
        d = base + timedelta(days=i)
        valor = 140.0 + (i % 30) * 0.5
        var = round((-1) ** i * 0.3, 2)
        rows.append(f"<tr><td>{d:%d/%m/%Y}</td><td>{valor:.2f}</td><td>{var:+.1f}%</td></tr>")
    return f"""
    <html><head><title>CEPEA</title></head><body>
    <table class="indicador" id="tblIndicador">
        <tr><th>Data</th><th>Valor (R$/sc 60kg)</th><th>Variação</th></tr>
        {"".join(rows)}
    </table>
    <p>Indicador CEPEA/ESALQ</p>
    </body></html>
    """


def _generate_na_html(num_rows: int) -> str:
    rows = []
    base = date(2024, 1, 1)
    for i in range(num_rows):
        d = base + timedelta(days=i)
        valor = f"R$ {140 + i % 30},{(i * 7) % 100:02d}"
        var = f"{(-1) ** i * 0.3:+.2f}%"
        rows.append(f"<tr><td>{d:%d/%m/%Y}</td><td>{valor}</td><td>{var}</td></tr>")
    return f"""
    <html><body>
    <table class="cot-fisicas">
        <tr><th>Data</th><th>Valor R$</th><th>Variação</th></tr>
        {"".join(rows)}
    </table>
    </body></html>
    """


def _generate_cache_data(size_bytes: int = 1024) -> bytes:
    return os.urandom(size_bytes)


def _generate_indicadores(n: int) -> list[dict[str, Any]]:
    base = datetime(2020, 1, 1)
    return [
        {
            "produto": "soja",
            "praca": f"praca_{i % 10}",
            "data": base + timedelta(days=i),
            "valor": 130.0 + (i % 50),
            "unidade": "BRL/sc60kg",
            "fonte": "cepea",
        }
        for i in range(n)
    ]


# ============================================================================
# 1. MEMORY PROFILING
# ============================================================================


class TestMemoryProfiling:
    def test_import_baseline(self):
        gc.collect()
        tracemalloc.start()
        snapshot_before = tracemalloc.take_snapshot()

        import agrobr  # noqa: F401
        from agrobr import cepea, conab, constants, models  # noqa: F401

        snapshot_after = tracemalloc.take_snapshot()
        tracemalloc.stop()

        stats = snapshot_after.compare_to(snapshot_before, "lineno")
        total_delta = sum(s.size_diff for s in stats if s.size_diff > 0)
        print(f"\n  [MEM] import agrobr baseline: {_mb(total_delta):.2f} MB")
        assert total_delta < 50 * 1024 * 1024, f"Import uses {_mb(total_delta):.1f} MB (>50 MB)"

    def test_cepea_parser_memory(self):
        from agrobr.cepea.parsers.v1 import CepeaParserV1

        html = _generate_cepea_html(1000)
        parser = CepeaParserV1()

        gc.collect()
        tracemalloc.start()
        snap_before = tracemalloc.take_snapshot()

        results = parser.parse(html, "soja")

        snap_after = tracemalloc.take_snapshot()
        peak = tracemalloc.get_traced_memory()[1]
        tracemalloc.stop()

        stats = snap_after.compare_to(snap_before, "lineno")
        delta = sum(s.size_diff for s in stats if s.size_diff > 0)
        print(
            f"\n  [MEM] CEPEA parse 1000 rows: delta={_mb(delta):.2f} MB, peak={_mb(peak):.2f} MB, records={len(results)}"
        )
        assert delta < 100 * 1024 * 1024

    def test_na_parser_memory(self):
        from agrobr.noticias_agricolas.parser import parse_indicador

        html = _generate_na_html(1000)

        gc.collect()
        tracemalloc.start()
        snap_before = tracemalloc.take_snapshot()

        results = parse_indicador(html, produto="soja")

        snap_after = tracemalloc.take_snapshot()
        peak = tracemalloc.get_traced_memory()[1]
        tracemalloc.stop()

        stats = snap_after.compare_to(snap_before, "lineno")
        delta = sum(s.size_diff for s in stats if s.size_diff > 0)
        print(
            f"\n  [MEM] NA parse 1000 rows: delta={_mb(delta):.2f} MB, peak={_mb(peak):.2f} MB, records={len(results)}"
        )
        assert delta < 100 * 1024 * 1024

    def test_duckdb_cache_memory(self, tmp_path):
        settings = CacheSettings(cache_dir=tmp_path, db_name="bench.duckdb")
        store = DuckDBStore(settings)

        gc.collect()
        tracemalloc.start()
        snap_before = tracemalloc.take_snapshot()

        for i in range(1000):
            store.cache_set(f"key_{i}", _generate_cache_data(512), Fonte.CEPEA, ttl_seconds=3600)

        snap_after = tracemalloc.take_snapshot()
        peak = tracemalloc.get_traced_memory()[1]
        tracemalloc.stop()

        stats = snap_after.compare_to(snap_before, "lineno")
        delta = sum(s.size_diff for s in stats if s.size_diff > 0)
        print(
            f"\n  [MEM] DuckDB 1000 cache writes: delta={_mb(delta):.2f} MB, peak={_mb(peak):.2f} MB"
        )
        store.close()
        assert delta < 200 * 1024 * 1024

    def test_memory_release_after_parse(self):
        from agrobr.cepea.parsers.v1 import CepeaParserV1

        html = _generate_cepea_html(2000)
        parser = CepeaParserV1()

        gc.collect()
        tracemalloc.start()

        results = parser.parse(html, "soja")
        mem_with_data = tracemalloc.get_traced_memory()[0]

        del results
        gc.collect()
        mem_after_del = tracemalloc.get_traced_memory()[0]

        tracemalloc.stop()

        freed = mem_with_data - mem_after_del
        print(
            f"\n  [MEM] Leak check: with_data={_kb(mem_with_data):.0f}KB, after_del={_kb(mem_after_del):.0f}KB, freed={_kb(freed):.0f}KB"
        )
        assert freed > 0 or mem_after_del < mem_with_data + MEMORY_LEAK_TOLERANCE_KB * 1024

    def test_pydantic_model_memory(self):
        gc.collect()
        tracemalloc.start()
        snap_before = tracemalloc.take_snapshot()

        indicadores = [
            Indicador(
                fonte=Fonte.CEPEA,
                produto="soja",
                data=date(2024, 1, 1) + timedelta(days=i),
                valor=Decimal("145.50"),
                unidade="BRL/sc60kg",
            )
            for i in range(5000)
        ]

        snap_after = tracemalloc.take_snapshot()
        tracemalloc.stop()

        stats = snap_after.compare_to(snap_before, "lineno")
        delta = sum(s.size_diff for s in stats if s.size_diff > 0)
        per_model = delta / 5000
        print(
            f"\n  [MEM] 5000 Indicador models: total={_mb(delta):.2f} MB, per_model={per_model:.0f} bytes"
        )
        assert per_model < 10_000, f"Indicador too large: {per_model:.0f} bytes/model"
        del indicadores


# ============================================================================
# 2. VOLUME DE DADOS — parser scaling
# ============================================================================


class TestVolumeScaling:
    @pytest.mark.parametrize("num_rows", [10, 100, 1000])
    def test_cepea_parser_scaling(self, num_rows):
        from agrobr.cepea.parsers.v1 import CepeaParserV1

        html = _generate_cepea_html(num_rows)
        parser = CepeaParserV1()

        start = time.perf_counter()
        results = parser.parse(html, "soja")
        elapsed_ms = (time.perf_counter() - start) * 1000

        flag = " *** FLAG" if elapsed_ms > FLAG_THRESHOLD_MS else ""
        print(
            f"\n  [VOL] CEPEA parse {num_rows} rows: {_fmt(elapsed_ms)}, got {len(results)} records{flag}"
        )
        assert len(results) == num_rows
        assert elapsed_ms < FLAG_THRESHOLD_MS * 10, f"CEPEA parser too slow at {num_rows} rows"

    @pytest.mark.parametrize("num_rows", [10, 100, 1000])
    def test_na_parser_scaling(self, num_rows):
        from agrobr.noticias_agricolas.parser import parse_indicador

        html = _generate_na_html(num_rows)

        start = time.perf_counter()
        results = parse_indicador(html, produto="soja")
        elapsed_ms = (time.perf_counter() - start) * 1000

        flag = " *** FLAG" if elapsed_ms > FLAG_THRESHOLD_MS else ""
        print(
            f"\n  [VOL] NA parse {num_rows} rows: {_fmt(elapsed_ms)}, got {len(results)} records{flag}"
        )
        assert len(results) == num_rows
        assert elapsed_ms < FLAG_THRESHOLD_MS * 10

    def test_cepea_parser_linearity(self):
        from agrobr.cepea.parsers.v1 import CepeaParserV1

        parser = CepeaParserV1()
        times = {}

        for n in [100, 1000]:
            html = _generate_cepea_html(n)
            start = time.perf_counter()
            parser.parse(html, "soja")
            times[n] = (time.perf_counter() - start) * 1000

        ratio = times[1000] / times[100] if times[100] > 0 else float("inf")
        print(
            f"\n  [VOL] CEPEA linearity: 100->{_fmt(times[100])}, 1000->{_fmt(times[1000])}, ratio={ratio:.1f}x (ideal~10x)"
        )
        assert ratio < 20, (
            f"CEPEA parser is super-linear: {ratio:.1f}x for 10x data (O(n²) suspected)"
        )

    def test_na_parser_linearity(self):
        from agrobr.noticias_agricolas.parser import parse_indicador

        times = {}

        for n in [100, 1000]:
            html = _generate_na_html(n)
            start = time.perf_counter()
            parse_indicador(html, produto="soja")
            times[n] = (time.perf_counter() - start) * 1000

        ratio = times[1000] / times[100] if times[100] > 0 else float("inf")
        print(
            f"\n  [VOL] NA linearity: 100->{_fmt(times[100])}, 1000->{_fmt(times[1000])}, ratio={ratio:.1f}x (ideal~10x)"
        )
        assert ratio < 20, f"NA parser is super-linear: {ratio:.1f}x for 10x data"

    def test_pydantic_validation_scaling(self):
        times = {}
        for n in [100, 1000, 5000]:
            start = time.perf_counter()
            indicadores = [
                Indicador(
                    fonte=Fonte.CEPEA,
                    produto="soja",
                    data=date(2024, 1, 1) + timedelta(days=i),
                    valor=Decimal("145.50"),
                    unidade="BRL/sc60kg",
                )
                for i in range(n)
            ]
            times[n] = (time.perf_counter() - start) * 1000
            del indicadores

        ratio = times[5000] / times[1000] if times[1000] > 0 else float("inf")
        print(
            f"\n  [VOL] Pydantic scaling: 100->{_fmt(times[100])}, 1000->{_fmt(times[1000])}, 5000->{_fmt(times[5000])}, ratio(5k/1k)={ratio:.1f}x"
        )
        assert ratio < 8, f"Pydantic validation super-linear: {ratio:.1f}x for 5x data"

    def test_can_parse_scaling(self):
        from agrobr.cepea.parsers.v1 import CepeaParserV1

        parser = CepeaParserV1()
        times = {}

        for n in [100, 1000, 5000]:
            html = _generate_cepea_html(n)
            start = time.perf_counter()
            can, conf = parser.can_parse(html)
            times[n] = (time.perf_counter() - start) * 1000
            assert can

        print(
            f"\n  [VOL] can_parse scaling: 100->{_fmt(times[100])}, 1000->{_fmt(times[1000])}, 5000->{_fmt(times[5000])}"
        )
        ratio = times[5000] / times[100] if times[100] > 0 else float("inf")
        assert ratio < 100, f"can_parse scales badly: {ratio:.1f}x for 50x data"


# ============================================================================
# 3. CACHE DUCKDB SOB CARGA
# ============================================================================


class TestCacheStress:
    @pytest.fixture()
    def store(self, tmp_path) -> DuckDBStore:
        settings = CacheSettings(cache_dir=tmp_path, db_name="stress.duckdb")
        s = DuckDBStore(settings)
        yield s
        s.close()

    def test_sequential_writes_10k(self, store):
        start = time.perf_counter()
        for i in range(10_000):
            store.cache_set(f"key_{i}", _generate_cache_data(256), Fonte.CEPEA, ttl_seconds=3600)
        elapsed = (time.perf_counter() - start) * 1000

        flag = " *** FLAG" if elapsed > 10_000 else ""
        print(f"\n  [CACHE] 10k sequential writes: {_fmt(elapsed)}{flag}")
        assert elapsed < 60_000

    def test_sequential_reads_10k(self, store):
        for i in range(10_000):
            store.cache_set(f"key_{i}", _generate_cache_data(256), Fonte.CEPEA, ttl_seconds=3600)

        start = time.perf_counter()
        hits = 0
        for i in range(10_000):
            data, stale = store.cache_get(f"key_{i}")
            if data:
                hits += 1
        elapsed = (time.perf_counter() - start) * 1000

        print(f"\n  [CACHE] 10k sequential reads: {_fmt(elapsed)}, hits={hits}")
        assert hits == 10_000
        assert elapsed < 30_000

    def test_mixed_read_write(self, store):
        for i in range(5000):
            store.cache_set(f"key_{i}", _generate_cache_data(256), Fonte.CEPEA, ttl_seconds=3600)

        start = time.perf_counter()
        for i in range(5000):
            store.cache_get(f"key_{i}")
            store.cache_set(
                f"key_{i + 5000}", _generate_cache_data(256), Fonte.CONAB, ttl_seconds=3600
            )
        elapsed = (time.perf_counter() - start) * 1000

        print(f"\n  [CACHE] 5k mixed read/write: {_fmt(elapsed)}")
        assert elapsed < 60_000

    @pytest.mark.parametrize("n_records", [10_000, 50_000])
    def test_indicadores_upsert_scaling(self, store, n_records):
        indicadores = _generate_indicadores(n_records)

        start = time.perf_counter()
        count = store.indicadores_upsert(indicadores)
        elapsed = (time.perf_counter() - start) * 1000

        flag = " *** FLAG" if elapsed > FLAG_THRESHOLD_MS * n_records / 1000 else ""
        print(
            f"\n  [CACHE] indicadores_upsert {n_records}: {_fmt(elapsed)}, inserted={count}{flag}"
        )
        assert count == n_records

    def test_indicadores_query_scaling(self, store):
        store.indicadores_upsert(_generate_indicadores(50_000))

        start = time.perf_counter()
        results = store.indicadores_query("soja")
        elapsed = (time.perf_counter() - start) * 1000

        print(f"\n  [CACHE] query 50k indicadores: {_fmt(elapsed)}, returned={len(results)}")
        assert len(results) == 50_000
        assert elapsed < 5_000

    def test_ttl_check_scaling(self, store):
        for i in range(10_000):
            ttl = 1 if i % 2 == 0 else 3600
            store.cache_set(f"key_{i}", _generate_cache_data(128), Fonte.CEPEA, ttl_seconds=ttl)

        conn = store._get_conn()
        past = datetime.utcnow() - timedelta(hours=1)
        conn.execute(
            "UPDATE cache_entries SET expires_at = ? WHERE key LIKE 'key_%' AND CAST(REPLACE(key, 'key_', '') AS INT) % 2 = 0",
            [past],
        )

        start = time.perf_counter()
        stale_count = 0
        for i in range(10_000):
            data, stale = store.cache_get(f"key_{i}")
            if stale:
                stale_count += 1
        elapsed = (time.perf_counter() - start) * 1000

        print(f"\n  [CACHE] TTL check 10k entries: {_fmt(elapsed)}, stale={stale_count}")
        assert stale_count == 5000
        assert elapsed < 30_000

    def test_db_file_size_scaling(self, tmp_path):
        settings = CacheSettings(cache_dir=tmp_path, db_name="size_test.duckdb")
        store = DuckDBStore(settings)

        sizes = {}
        for target in [1000, 5000, 10_000]:
            for i in range(sizes.get("last_i", 0), target):
                store.cache_set(
                    f"key_{i}", _generate_cache_data(512), Fonte.CEPEA, ttl_seconds=3600
                )
            sizes["last_i"] = target

            db_path = tmp_path / "size_test.duckdb"
            if db_path.exists():
                sizes[target] = db_path.stat().st_size

        store.close()

        for n, size in sizes.items():
            if n == "last_i":
                continue
            print(f"\n  [CACHE] DB size at {n} entries: {_mb(size):.2f} MB")

        if 1000 in sizes and 10_000 in sizes:
            ratio = sizes[10_000] / sizes[1000]
            print(f"  [CACHE] Size ratio 10k/1k: {ratio:.1f}x (ideal~10x)")
            assert ratio < 15, f"DB size scales poorly: {ratio:.1f}x for 10x data"


# ============================================================================
# 4. RATE LIMITING E CONCORRÊNCIA HTTP
# ============================================================================


class TestRateLimitingConcurrency:
    def setup_method(self):
        RateLimiter.reset()

    @pytest.mark.asyncio
    async def test_rate_limiter_enforces_delay(self):
        times = []
        for _ in range(3):
            start = time.perf_counter()
            async with RateLimiter.acquire(Fonte.IBGE):
                pass
            times.append(time.perf_counter() - start)

        [times[i] - 0 for i in range(1, len(times))]
        settings = HTTPSettings()
        expected_delay = settings.rate_limit_ibge

        print(
            f"\n  [RATE] IBGE rate limiter - delays: {[f'{d:.3f}s' for d in times]}, expected>={expected_delay}s"
        )
        assert any(t >= expected_delay * 0.8 for t in times[1:]), "Rate limiter not enforcing delay"

    @pytest.mark.asyncio
    async def test_concurrent_same_source_serialized(self):
        call_times: list[float] = []

        async def worker():
            async with RateLimiter.acquire(Fonte.CEPEA):
                call_times.append(time.monotonic())
                await asyncio.sleep(0.01)

        start = time.perf_counter()
        await asyncio.gather(*[worker() for _ in range(5)])
        total = (time.perf_counter() - start) * 1000

        if len(call_times) >= 2:
            intervals = [call_times[i] - call_times[i - 1] for i in range(1, len(call_times))]
            min_interval = min(intervals)
        else:
            min_interval = 0

        print(
            f"\n  [RATE] 5 concurrent CEPEA: total={_fmt(total)}, min_interval={min_interval:.3f}s"
        )
        assert total > 500, "Requests not serialized — rate limiter bypassed"

    @pytest.mark.asyncio
    async def test_concurrent_different_sources_parallel(self):
        sources = [Fonte.CEPEA, Fonte.CONAB, Fonte.IBGE]
        completed: list[str] = []

        async def worker(fonte: Fonte):
            async with RateLimiter.acquire(fonte):
                await asyncio.sleep(0.05)
                completed.append(fonte.value)

        start = time.perf_counter()
        await asyncio.gather(*[worker(s) for s in sources])
        total = (time.perf_counter() - start) * 1000

        print(f"\n  [RATE] 3 different sources parallel: total={_fmt(total)}, order={completed}")
        assert total < 5000, "Different sources should run in parallel"

    @pytest.mark.asyncio
    async def test_backoff_does_not_block_event_loop(self):
        from agrobr.http.retry import retry_async

        call_count = 0
        event_loop_alive = False

        async def failing_fn():
            nonlocal call_count
            call_count += 1
            raise Exception("simulated failure")

        async def heartbeat():
            nonlocal event_loop_alive
            for _ in range(20):
                await asyncio.sleep(0.05)
                event_loop_alive = True

        hb_task = asyncio.create_task(heartbeat())

        with pytest.raises(Exception, match="simulated failure"):
            await retry_async(
                failing_fn,
                max_attempts=3,
                base_delay=0.1,
                max_delay=0.5,
                retriable_exceptions=(Exception,),
            )

        await asyncio.sleep(0.1)
        hb_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await hb_task

        print(f"\n  [RATE] backoff: calls={call_count}, event_loop_alive={event_loop_alive}")
        assert event_loop_alive, "Event loop was blocked during backoff"
        assert call_count == 3

    @pytest.mark.asyncio
    async def test_throughput_per_source(self):
        results: dict[str, float] = {}

        for fonte in [Fonte.CEPEA, Fonte.CONAB, Fonte.IBGE]:
            RateLimiter.reset()
            count = 0
            start = time.perf_counter()
            deadline = start + 2.0

            while time.perf_counter() < deadline:
                async with RateLimiter.acquire(fonte):
                    count += 1

            elapsed = time.perf_counter() - start
            rps = count / elapsed
            results[fonte.value] = rps

        print("\n  [RATE] Throughput per source:")
        for fonte, rps in results.items():
            print(f"    {fonte}: {rps:.2f} req/s")

        RateLimiter.reset()


# ============================================================================
# 5. ASYNC PERFORMANCE
# ============================================================================


class TestAsyncPerformance:
    @pytest.mark.asyncio
    async def test_parser_does_not_block_event_loop(self):
        from agrobr.cepea.parsers.v1 import CepeaParserV1

        html = _generate_cepea_html(2000)
        parser = CepeaParserV1()

        heartbeat_count = 0

        async def heartbeat():
            nonlocal heartbeat_count
            while True:
                await asyncio.sleep(0.01)
                heartbeat_count += 1

        hb = asyncio.create_task(heartbeat())

        loop = asyncio.get_event_loop()
        start = time.perf_counter()
        results = await loop.run_in_executor(None, parser.parse, html, "soja")
        parse_time = (time.perf_counter() - start) * 1000

        await asyncio.sleep(0.05)
        hb.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await hb

        print(
            f"\n  [ASYNC] Parser in executor: {_fmt(parse_time)}, heartbeats={heartbeat_count}, records={len(results)}"
        )
        assert heartbeat_count > 0, "Event loop was blocked during parsing"

    @pytest.mark.asyncio
    async def test_phase_breakdown(self):
        from agrobr.cepea.parsers.v1 import CepeaParserV1

        html = _generate_cepea_html(500)
        parser = CepeaParserV1()

        t0 = time.perf_counter()
        from bs4 import BeautifulSoup

        BeautifulSoup(html, "lxml")
        t_parse_html = (time.perf_counter() - t0) * 1000

        t0 = time.perf_counter()
        results = parser.parse(html, "soja")
        t_full_parse = (time.perf_counter() - t0) * 1000

        t0 = time.perf_counter()
        serialized = pickle.dumps(results)
        t_serialize = (time.perf_counter() - t0) * 1000

        print("\n  [ASYNC] Phase breakdown (500 rows):")
        print(f"    HTML->BS4:    {_fmt(t_parse_html)}")
        print(f"    Full parse:   {_fmt(t_full_parse)}")
        print(f"    Serialize:    {_fmt(t_serialize)}")
        print(f"    Data size:    {len(serialized) / 1024:.1f} KB")

        bottleneck = "CPU-bound (parsing)" if t_full_parse > t_serialize * 2 else "I/O-bound"
        print(f"    Bottleneck:   {bottleneck}")

    @pytest.mark.asyncio
    async def test_concurrent_parsers(self):
        from agrobr.cepea.parsers.v1 import CepeaParserV1
        from agrobr.noticias_agricolas.parser import parse_indicador

        cepea_html = _generate_cepea_html(500)
        na_html = _generate_na_html(500)
        parser = CepeaParserV1()

        loop = asyncio.get_event_loop()

        start = time.perf_counter()
        parser.parse(cepea_html, "soja")
        parse_indicador(na_html, produto="soja")
        sequential_ms = (time.perf_counter() - start) * 1000

        start = time.perf_counter()
        r1_f, r2_f = await asyncio.gather(
            loop.run_in_executor(None, parser.parse, cepea_html, "soja"),
            loop.run_in_executor(None, parse_indicador, na_html, "soja"),
        )
        parallel_ms = (time.perf_counter() - start) * 1000

        speedup = sequential_ms / parallel_ms if parallel_ms > 0 else 0
        print(
            f"\n  [ASYNC] Concurrent parsers: sequential={_fmt(sequential_ms)}, parallel={_fmt(parallel_ms)}, speedup={speedup:.2f}x"
        )


# ============================================================================
# 6. SYNC WRAPPER
# ============================================================================


class TestSyncWrapperStress:
    def test_sequential_calls(self):
        from agrobr.sync import run_sync

        async def simple_coro(x):
            return x * 2

        start = time.perf_counter()
        for i in range(100):
            result = run_sync(simple_coro(i))
            assert result == i * 2
        elapsed = (time.perf_counter() - start) * 1000

        per_call = elapsed / 100
        flag = " *** FLAG" if per_call > 10 else ""
        print(
            f"\n  [SYNC] 100 sequential run_sync: total={_fmt(elapsed)}, per_call={_fmt(per_call)}{flag}"
        )
        assert per_call < 100, f"run_sync overhead too high: {per_call:.1f}ms per call"

    def test_sync_wrapper_overhead(self):
        from agrobr.sync import sync_wrapper

        async def async_noop():
            return 42

        sync_noop = sync_wrapper(async_noop)

        start = time.perf_counter()
        for _ in range(100):
            result = sync_noop()
            assert result == 42
        sync_elapsed = (time.perf_counter() - start) * 1000

        async def measure_async():
            start = time.perf_counter()
            for _ in range(100):
                await async_noop()
            return (time.perf_counter() - start) * 1000

        import asyncio

        async_elapsed = asyncio.run(measure_async())

        overhead_ratio = sync_elapsed / async_elapsed if async_elapsed > 0 else float("inf")
        print(
            f"\n  [SYNC] Overhead: async={_fmt(async_elapsed)}, sync={_fmt(sync_elapsed)}, ratio={overhead_ratio:.1f}x"
        )

    def test_sync_wrapper_with_real_work(self):
        from agrobr.sync import sync_wrapper

        async def async_parse():
            from agrobr.cepea.parsers.v1 import CepeaParserV1

            html = _generate_cepea_html(100)
            parser = CepeaParserV1()
            return parser.parse(html, "soja")

        sync_parse = sync_wrapper(async_parse)

        times = []
        for _ in range(10):
            start = time.perf_counter()
            results = sync_parse()
            times.append((time.perf_counter() - start) * 1000)
            assert len(results) == 100

        mean_ms = statistics.mean(times)
        std_ms = statistics.stdev(times) if len(times) > 1 else 0
        print(f"\n  [SYNC] 10x sync_parse(100 rows): mean={_fmt(mean_ms)}, std={_fmt(std_ms)}")

    def test_event_loop_reuse(self):
        from agrobr.sync import run_sync

        loop_ids = set()

        async def capture_loop():
            loop = asyncio.get_event_loop()
            loop_ids.add(id(loop))
            return True

        for _ in range(20):
            run_sync(capture_loop())

        print(f"\n  [SYNC] Event loop IDs across 20 calls: {len(loop_ids)} unique")

    def test_exception_propagation_stress(self):
        from agrobr.sync import run_sync

        async def failing(i):
            raise ValueError(f"error_{i}")

        caught = 0
        start = time.perf_counter()
        for i in range(50):
            try:
                run_sync(failing(i))
            except ValueError as e:
                assert f"error_{i}" in str(e)
                caught += 1
        elapsed = (time.perf_counter() - start) * 1000

        print(f"\n  [SYNC] 50 exception propagations: {_fmt(elapsed)}, caught={caught}")
        assert caught == 50


# ============================================================================
# Golden data volume multiplier tests
# ============================================================================


class TestGoldenDataScaling:
    def _load_golden_cepea_html(self) -> str | None:
        cepea_dir = GOLDEN_DIR / "cepea"
        if not cepea_dir.exists():
            return None
        for case_dir in cepea_dir.iterdir():
            if case_dir.is_dir() and (case_dir / "response.html").exists():
                return (case_dir / "response.html").read_text(encoding="utf-8")
        return None

    def test_golden_cepea_10x(self):
        html = self._load_golden_cepea_html()
        if html is None:
            pytest.skip("No CEPEA golden data")

        from bs4 import BeautifulSoup

        soup = BeautifulSoup(html, "lxml")
        tables = soup.find_all("table")
        if not tables:
            pytest.skip("No tables in golden HTML")

        table = tables[0]
        rows = table.find_all("tr")[1:]
        if not rows:
            pytest.skip("No data rows")

        original_count = len(rows)
        for _ in range(9):
            for row in rows[:original_count]:
                import copy

                new_row = copy.copy(row)
                table.append(new_row)

        multiplied_html = str(soup)

        from agrobr.cepea.parsers.v1 import CepeaParserV1

        parser = CepeaParserV1()

        start = time.perf_counter()
        try:
            results = parser.parse(multiplied_html, "soja")
            elapsed = (time.perf_counter() - start) * 1000
            print(
                f"\n  [GOLDEN] CEPEA 10x ({original_count}->{original_count * 10} rows): {_fmt(elapsed)}, parsed={len(results)}"
            )
        except Exception as e:
            elapsed = (time.perf_counter() - start) * 1000
            print(f"\n  [GOLDEN] CEPEA 10x FAILED: {e} after {_fmt(elapsed)}")

    def _load_golden_na_html(self) -> str | None:
        na_dir = GOLDEN_DIR / "na"
        if not na_dir.exists():
            return None
        for case_dir in na_dir.iterdir():
            if case_dir.is_dir() and (case_dir / "response.html").exists():
                return (case_dir / "response.html").read_text(encoding="utf-8")
        return None

    def test_golden_na_10x(self):
        html = self._load_golden_na_html()
        if html is None:
            pytest.skip("No NA golden data")

        from bs4 import BeautifulSoup

        soup = BeautifulSoup(html, "lxml")
        tables = soup.find_all("table")
        if not tables:
            pytest.skip("No tables in golden HTML")

        table = tables[0]
        tbody = table.find("tbody")
        container = tbody if tbody else table
        rows = container.find_all("tr")
        data_rows = [r for r in rows if r.find("td")]
        if not data_rows:
            pytest.skip("No data rows")

        original_count = len(data_rows)
        for _ in range(9):
            for row in data_rows[:original_count]:
                import copy

                new_row = copy.copy(row)
                container.append(new_row)

        multiplied_html = str(soup)

        from agrobr.noticias_agricolas.parser import parse_indicador

        start = time.perf_counter()
        try:
            results = parse_indicador(multiplied_html, produto="soja")
            elapsed = (time.perf_counter() - start) * 1000
            print(
                f"\n  [GOLDEN] NA 10x ({original_count}->{original_count * 10} rows): {_fmt(elapsed)}, parsed={len(results)}"
            )
        except Exception as e:
            elapsed = (time.perf_counter() - start) * 1000
            print(f"\n  [GOLDEN] NA 10x FAILED: {e} after {_fmt(elapsed)}")
