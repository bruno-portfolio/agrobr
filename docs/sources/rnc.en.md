# RNC/SNPC — CultivarWeb/MAPA

CultivarWeb publishes exports of cultivars registered with RNC and protection records from SNPC. agrobr keeps these families separate, with their own identity, contract and provenance. The project's classification is `livre`; its basis and limits are described in the [license documentation](../licenses.en.md). Public access alone is not presented as a specific license for the CSVs.

## Overview

| Field | Value |
|---|---|
| Operator | MAPA |
| Portal | [CultivarWeb](https://sistemas.agricultura.gov.br/snpc/cultivarweb) |
| Resources | Registered-cultivar CSV export; protected-cultivar CSV export |
| Observed format | Comma-separated UTF-8 CSV, quoted fields where needed |
| Time coverage | Current export; no historical-revision selector |
| SDK cache | Raw CSV and manifest, 24-hour TTL from acquisition |

The **7 September 2026** capture contained 38,335 RNC rows and 5,424 SNPC rows. These are counts from that acquisition, not permanent sizes or a guarantee of all historical registrations. The observed RNC export contained only status `REGISTRADA`; it does not establish coverage of closed registrations. SNPC included six statuses covering definitive, provisional, cancelled, expired, relinquished and void protection.

## Delivered data

### Registered cultivars

Ten columns: `cultivar`, `nome_comum`, `nome_cientifico`, `grupo`, `situacao`, `nr_formulario`, `nr_registro`, `data_registro`, `data_validade`, `mantenedor`.

The registration number identifies the row within the export. Form numbers are not keys: repeated and blank values occur. Published blank cultivar/maintainer names are preserved, as are textual identifiers and any punctuation or leading zeros. Compound species or maintainer fields are not automatically split.

### Protected cultivars

Twelve columns: `cultivar`, `nome_cientifico`, `nome_comum`, `nr_processo`, `situacao`, `nr_certificado`, `inicio_protecao`, `termino_protecao`, `titular`, `representante_legal`, `melhoristas`, `termino_protecao_texto`.

The key is the process number, scoped to the export. Repeated certificates across processes are not deduplicated. Holder and breeder fields may contain several people; their compound text is retained, without inferring person counts from delimiters.

`termino_protecao_texto` preserves the end cell after removing outer whitespace. The date column separates valid dates from values without a determined date: blanks and `até a emissão do certificado definitivo` (“until the definitive certificate is issued”) become `NaT`, with their text retained alongside. The cited capture contained 5,251 dates, 171 conditions and two blanks. The condition occurs under several administrative statuses and does not automatically mean provisional protection.

All four date columns are civil `datetime64[ns]`, without a timezone, and allow published missing values. Other invalid non-empty date text raises an explicit error. Remaining columns contain text; the SDK removes outer whitespace and preserves legitimate blanks. In `nome_cientifico` and `nome_comum`, it also collapses repeated internal whitespace (including the no-break space) into one space: SNPC publishes soybean as `Glycine max (L.)  Merr.`, with 2 spaces, and RNC as `Glycine max (L.) Merr.`, so the scientific name matches across the 2 families. Other columns stay as published.

## Access and validation

A public session follows the search and export forms. A GET obtains a CSRF token before each POST, including retries; no personal credential is required. The client uses a timeout, retry and rotating User-Agent. Public metadata excludes tokens and cookies.

The complete received CSV is validated before filtering: full layout, row widths, Pydantic models, dates, key and contract. A layout/content error or a mismatch between the search total and record count aborts the acquisition. The SHA256 layout fingerprint describes the header structure; the CSV hash identifies the acquired bytes. Neither certifies geographic accuracy or legal status.

Existing text filters use case- and accent-insensitive literal substrings. RNC `nr_registro`/`nr_formulario` and SNPC `nr_processo`/`nr_certificado` use textual equality. All remove outer whitespace. Selection does not reduce population validation or the volume transferred for the current acquisition.

## Cache, metadata and coverage

The cache stores a raw CSV and manifest in a local package per family. Its 24-hour TTL uses the UTC receipt time, not last access or file mtime. Expired, incompatible or corrupt packages are ignored; writes atomically replace a package after validation. Cache hits parse and validate the body again, retaining the original acquisition metadata. `use_cache=False` neither reads nor writes the cache. Where it lives and how to clean it: [What agrobr writes to disk](../advanced/disco.md).

`MetaInfo` distinguishes cache/network, raw hash/size, original time, expiry, fetch/parsing/filter durations and selected source. `source_details` records the search and export, statistics before filtering and the local selection.

Coverage `count_matched` means only that the validated population equals the total reported by the preceding search. Without a verified total, the status is `unknown`. Search and export are separate requests; neither matching counts nor caching establishes a transactional snapshot or immutable historical revision.

## Usage

```python
from agrobr import rnc

registered = await rnc.registradas(especie="soja")
protected, meta = await rnc.protegidas(
    nr_processo="21806.000132/2019",
    use_cache=False,
    return_meta=True,
)
```

See signatures and types in the [RNC/SNPC API](../api/rnc.en.md). The `cultivares_registradas` and `cultivares_protegidas` datasets reuse these capabilities and contracts 1.0, without fallback to another source. The datasets reject `deterministic` context before I/O because this route does not select historical revisions.

## Limits

The delivery covers the fields of the two queried exports. It does not join registration to protection by name, calculate legal validity, reproduce past editions by date or automatically extend the catalog to other MAPA documents/services. Counts and statuses change with the current publication.

## Export of 2026-09-18

The CSVs of the 2026-09-18 export contain 38,325 RNC records and 5,424 SNPC records. The HTML search count that precedes each CSV matches it; this agreement establishes neither a transactional snapshot nor historical completeness.

In RNC, the published 22,946 blank form numbers, 4,256 blank cultivar names and 4,271 blank maintainers remain empty text; both dates are populated in that export. There are 924 repeated form-number groups with 15,142 occurrences. In SNPC, protection end has 5,267 dates, 155 conditions and two blanks; the 155 conditions and two blanks produce null dates while retaining the text. There are 102 certificate numbers repeated across distinct processes, with 204 occurrences. These secondary-identifier repetitions are not deduplicated.

The original acquisition in `fetched_at` retains timezone-aware UTC through cache and dataset calls. At both source and dataset level, `fetch_timestamp` equals acquisition time. Table dates are civil dates independent of these timestamps.
