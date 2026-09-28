# UN Comtrade — international trade

The integration provides bilateral merchandise trade (type C), exports/imports, annual or monthly periods and HS classification. The mirror compares export declarations with reverse imports. See the [API selectors](../api/comtrade.md) and [contract 2.1](../contracts/comercio_internacional.md).

## Routes and options

| Layer | Delivery |
|---|---|
| Public preview | No key, one period per request and an independent count |
| Authenticated acquisition | Optional key transport and full preview replanning on 401/403 |
| Bilateral | Explicit World or all published partners; individual HS, agricultural alias or textual list |
| Mirror | Outer 1:1 join, numeric identity and each declaration's HS revision |
| Semantic dataset | Same selectors, full contract including empty output, metadata, sync and Polars |

Routes: `https://comtradeapi.un.org/public/v1/preview/C/{freq}/HS` and `https://comtradeapi.un.org/data/v1/get/C/{freq}/HS`. The client uses async httpx, timeouts, retries, rate limits and Pydantic validation. Credentials do not enter resources or metadata.

## Evidence and limits

The September 2026 probe of BR/X/2023, five HS codes and all partners returned 500 preview rows against an independent count of 516. The union of disjoint HS queries preserved all 516 records, including the missing 16. Soybeans returned one explicit World row versus 63 published partners when the parameter was omitted. These observations support query semantics; they are not global database counts.

A single period/HS may still exceed available access. `require_complete=True` rejects such output; the default warns and records coverage. Revisions and failed blocks do not become successful empty responses. Authenticated behavior was tested with replay, without a real key.

Availability depends on country, period and classification. Services, tariffs, bulk, a dynamic country catalog, transport/customs detail, harmonization and frozen revision history are outside this increment.

The Brazil side comes from Brazil's declaration to Comtrade and may diverge from [ComexStat](comexstat.md), which is revised: for corn to China in 2024, Comtrade has 2,285,068 t and ComexStat 2,227,000 t (+2.6%), probably because SECEX revised the data after the submission. For soybean and soybean meal in 2024 and 2025 and for corn in 2025, the difference was at most 0.06% (checked on 2026-09-25). The difference may also come from UN estimation: for chicken in 2024 (HS 020711 to 020714), Comtrade's weight exceeds ComexStat's by 0.88%, with FOB practically equal (+0.007%), because the net weight of 020714 is estimated by the UN. The `peso_liquido_estimado` column and the `MetaInfo` warning show when this happens (checked on 2026-09-26).

The internal license category is `zona_cinza`; see [Licenses](../licenses.md#un-comtrade). Technical references: [preview](https://uncomtrade.org/docs/what-is-data-preview/) and the [official SDK with countOnly](https://github.com/uncomtrade/comtradeapicall).
