# Benchmark Report — agrobr

Run date: 2026-02-11
Platform: Windows, Python 3.11+

## Summary

| Section | Tests | Passed | Skipped/Timeout | Time |
|---|---|---|---|---|
| 1. Memory Profiling | 6 | 6 | 0 | 5.50s |
| 2. Volume Scaling | 10 | 10 | 0 | 1.70s |
| 3. Cache DuckDB Stress | 8 | 8 | 0 | ~32s |
| 4. Rate Limiting & Concurrency | 5 | 5 | 0 | 21.28s |
| 5. Async Performance | 3 | 3 | 0 | 1.61s |
| 6. Sync Wrapper Stress | 5 | 5 | 0 | 1.36s |
| 7. Golden Data Scaling | 2 | 2 | 0 | 1.08s |
| **Total** | **39** | **39** | **0** | **~65s** |

---

## 1. Memory Profiling

| Test | Threshold | Description |
|---|---|---|
| `test_import_baseline` | < 50 MB | Memory delta after importing agrobr core modules |
| `test_cepea_parser_memory` | < 100 MB | Memory delta parsing 1000 CEPEA rows |
| `test_na_parser_memory` | < 100 MB | Memory delta parsing 1000 NA rows |
| `test_duckdb_cache_memory` | < 200 MB | Memory delta after 1000 cache writes (512B each) |
| `test_memory_release_after_parse` | freed > 0 OR leak < 512 KB | Verifies GC releases parser results |
| `test_pydantic_model_memory` | < 10,000 bytes/model | Per-model memory for 5000 Indicador instances |

**Result: ALL PASSED**

---

## 2. Volume Scaling

| Test | Threshold | Description |
|---|---|---|
| `test_cepea_parser_scaling[10/100/1000]` | < 10,000 ms; rows_out == rows_in | CEPEA parser at 3 volume levels |
| `test_na_parser_scaling[10/100/1000]` | < 10,000 ms; rows_out == rows_in | NA parser at 3 volume levels |
| `test_cepea_parser_linearity` | ratio(1000/100) < 20x | Confirms O(n) — rejects O(n^2) |
| `test_na_parser_linearity` | ratio(1000/100) < 20x | Confirms O(n) — rejects O(n^2) |
| `test_pydantic_validation_scaling` | ratio(5000/1000) < 8x | Pydantic v2 validation scales linearly |
| `test_can_parse_scaling` | ratio(5000/100) < 100x | Fingerprint check does not degrade at volume |

**Result: ALL PASSED**

---

## 3. Cache DuckDB Stress

| Test | Threshold | Result |
|---|---|---|
| `test_sequential_writes_10k` | < 60,000 ms | PASSED |
| `test_sequential_reads_10k` | < 30,000 ms; 10k hits | PASSED |
| `test_mixed_read_write` | < 60,000 ms | PASSED |
| `test_indicadores_upsert_scaling[10000]` | completes; count == n | PASSED (4.8s) |
| `test_indicadores_upsert_scaling[50000]` | completes; count == n | PASSED (25.9s) |
| `test_indicadores_query_scaling` | < 5,000 ms; 50k returned | PASSED |
| `test_ttl_check_scaling` | < 30,000 ms; 5k stale | PASSED |
| `test_db_file_size_scaling` | ratio(10k/1k) < 15x | PASSED |

### Performance Note

- **`indicadores_upsert` optimized** (Issue #9): temp table + INSERT SELECT replaces row-by-row INSERT.
  10k: 34s→4.8s, 50k: 187s→25.9s (7x speedup). Scaling now linear (50k/10k ≈ 5.4x).

---

## 4. Rate Limiting & Concurrency

| Test | Threshold | Description |
|---|---|---|
| `test_rate_limiter_enforces_delay` | delay >= 80% of configured rate | IBGE rate limiter honors configured delay |
| `test_concurrent_same_source_serialized` | total > 500 ms for 5 requests | Same-source requests are serialized |
| `test_concurrent_different_sources_parallel` | total < 5,000 ms for 3 sources | Different sources run concurrently |
| `test_backoff_does_not_block_event_loop` | heartbeat alive; 3 retries | Exponential backoff uses async sleep |
| `test_throughput_per_source` | (informational, no assert) | Measures req/s per fonte over 2s window |

**Result: ALL PASSED**

---

## 5. Async Performance

| Test | Threshold | Description |
|---|---|---|
| `test_parser_does_not_block_event_loop` | heartbeat_count > 0 | Parser runs in executor, loop stays responsive |
| `test_phase_breakdown` | (informational, no assert) | Measures HTML parsing, full parse, serialization phases |
| `test_concurrent_parsers` | (informational, no assert) | Compares sequential vs parallel parser execution |

**Result: ALL PASSED**

---

## 6. Sync Wrapper Stress

| Test | Threshold | Description |
|---|---|---|
| `test_sequential_calls` | < 100 ms/call | 100 sequential `run_sync()` calls |
| `test_sync_wrapper_overhead` | (informational) | Ratio of sync vs async execution time |
| `test_sync_wrapper_with_real_work` | rows == 100 per call | 10x sync_parse with 100 CEPEA rows each |
| `test_event_loop_reuse` | (informational) | Counts unique event loop IDs across 20 calls |
| `test_exception_propagation_stress` | caught == 50 | 50 exceptions propagate correctly through sync wrapper |

**Result: ALL PASSED**

---

## 7. Golden Data Scaling

| Test | Threshold | Description |
|---|---|---|
| `test_golden_cepea_10x` | does not crash | Multiplies golden CEPEA HTML by 10x, parses |
| `test_golden_na_10x` | does not crash | Multiplies golden NA HTML by 10x, parses |

**Result: ALL PASSED** (tests use `try/except` internally — measures degradation, does not assert)

---

## Key Thresholds Reference

| Constant | Value | Used In |
|---|---|---|
| `FLAG_THRESHOLD_MS` | 1,000 ms | Volume scaling, cache upsert flagging |
| `MEMORY_LEAK_TOLERANCE_KB` | 512 KB | Memory release after parse |

## Action Items

~~1. **`indicadores_upsert` performance at 50k records** — RESOLVED. Temp table approach gives 7x speedup.~~

No open action items.
