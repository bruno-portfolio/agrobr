# SICAR (Rural Environmental Registry)

## About

The **National Rural Environmental Registry System (SICAR)** is the mandatory
electronic registry of every rural property in Brazil, under Law 12.651/2012
(Forest Code). Administered by the Brazilian Forest Service (SFB),
the system holds more than **7.4 million properties** registered across 27 states.

The CAR includes information about:

- Rural property identification
- Registration status (Active, Pending, Suspended, Cancelled)
- Total area in hectares
- Fiscal modules
- Property type (Rural, Settlement, Indigenous Land)
- Municipality and IBGE code

## Access via WFS

agrobr accesses the SICAR GeoServer WFS directly, with no need for
CAPTCHA or authentication. The OGC WFS protocol allows standardized queries
with server-side filters (CQL_FILTER) and transparent pagination.

**Endpoint:** `https://geoserver.car.gov.br/geoserver/sicar/wfs`

## Available fields

| Field | Type | Description |
|-------|------|-----------|
| cod_imovel | string | Unique property code (UF-IBGE-hash) |
| status | string | AT (Active), PE (Pending), SU (Suspended), CA (Cancelled) |
| data_criacao | datetime UTC | Instant of record creation |
| data_atualizacao | datetime UTC | Last update (nullable) |
| area_ha | float | Total area in hectares |
| condicao | string | Registration condition (nullable) |
| uf | string | State abbreviation |
| municipio | string | Municipality name |
| cod_municipio_ibge | int | Municipality IBGE code |
| modulos_fiscais | float | Number of fiscal modules |
| tipo | string | IRU (Rural), AST (Settlement), PCT (Indigenous Land) |
| cod_municipio | int | 7-digit IBGE code (same as `cod_municipio_ibge`); nullable |

## Notes

- **Tabular format 2.0:** `imoveis()` uses GeoJSON attributes only, without geometry or
  GeoPandas. Dates are UTC, including null columns and empty results. Official CSV clocks
  lacked a timezone and differed from the UTC instants in JSON and CQL cutoffs; no fixed
  offset is applied to convert old CSV captures
- **Incremental update:** `imoveis()`, `imoveis_geo()` and `imoveis_geo_stream()` accept
  `atualizado_apos` (CQL `data_atualizacao>'...'`, ISO date or datetime) to fetch only
  records updated after a given date. The column is requested in the 15 layers that provide it.
  The field does not exist in PE, PI, PR, RJ, RN, RO, RR, RS, SC, SE, SP or TO: the filter
  raises before network access in these states, and the column remains null in queries without
  that filter. This coverage comes from the `DescribeFeatureType` of all 27 layers on 2026-09-06
- **Current state:** creation (`>=`) and update (`>`) filters select records available at query
  time. They do not retrieve previous revisions or deletions. The `cadastro_rural` dataset also
  accepts municipality (name or code) and update filters, and rejects `deterministic`
- **Geometry available:** `imoveis_geo()` returns a `GeoDataFrame` with MultiPolygon polygons
  (EPSG:4326) via WFS GeoJSON. Requires `pip install agrobr[geo]`. The default result limit is
  5,000 features; `max_registros` above 10,000 or `None` uses pagination. A cut result warns
  (`validation_warnings`, `UserWarning` and `source_details["sicar"]["truncado"]`). All 27 layers declare
  SIRGAS 2000 (`DefaultCRS` EPSG:4674); agrobr requests `srsName=EPSG:4326`
  and rejects with `ParseError` any page with features that declares another CRS
- **Filter precision:** `atualizado_apos` accepts milliseconds with optional additional zeros.
  `.212000` is sent as `.212`; submillisecond values raise without rounding
- **Pagination:** large queries use 10,000-record pages sorted by `cod_imovel` in both
  tabular and geospatial paths. The source advertises `PagingIsTransactionSafe=FALSE`:
  sorting fixes the record order but does not guarantee a snapshot across pages during
  concurrent updates
- **Counts during tabular pagination:** changes in `numberMatched` produce a warning log and
  `MetaInfo.validation_warnings` (with `return_meta=True`). The largest observed total
  determines how many pages to request; the final number of unique feature IDs must match the last
  announced count. Repeated feature IDs or a final mismatch raise `ParseError` with instructions to
  repeat the query. There is no automatic retry or guarantee of snapshot completeness;
  warnings are also preserved in `cadastro_rural` and municipality summaries
- **Counts during geospatial pagination:** paginated `imoveis_geo()` (`max_registros=None` or above
  10,000) and `imoveis_geo_stream()` follow the same rule: pages follow the initial count, and the final
  number of unique feature IDs must match the last announced count (or `max_registros`, if smaller);
  otherwise `ParseError` with instructions to repeat the query. A change in `numberMatched` goes to the log
  and, in `imoveis_geo(return_meta=True)`, to `MetaInfo.validation_warnings`. In the stream, the check runs
  after the last page, so the error arrives after the batches already delivered
- **Repeated occurrences:** after the scan, `imoveis()`, municipality summaries, `imoveis_geo()`
  and `imoveis_geo_stream()` select one occurrence per `cod_imovel`. The comparison field is chosen for the whole group: update if
  available for every occurrence; otherwise creation if available for every occurrence; otherwise
  the highest numeric feature-ID suffix. Date ties use the same ID rule. The 12 states without
  updates are listed above. `cadastro_rural` keeps contract 2.1, its key and twelve columns.
  Warnings and `source_details["sicar"]` record feature counts, collapsed codes, discarded
  occurrences and criteria. The discard list contains up to 1,000 items and flags truncation.
  See the [selection rule and provenance fields](../contracts/cadastro_rural.en.md#multiple-occurrences-and-provenance)
- **No cache:** every call queries the CAR GeoServer; repeating the query downloads everything again
- **Extended timeout:** 180s read timeout for states with many records (BA, MG, MT)
- **SSL:** the CAR GeoServer uses a legacy cipher suite that rejects the standard TLS handshake.
  The client uses an `SSLContext` with `@SECLEVEL=1` while retaining certificate and hostname
  verification. Certificate trust failures do not activate an unverified fallback
- **EUDR relevance:** data essential for compliance with the EU Deforestation Regulation

## License

Open data from the Brazilian federal government. Available via the gov.br CKAN portal.
License: **CC-BY** — free use with attribution to the source.

## Links

- [Portal CAR](https://www.car.gov.br)
- [SICAR Consulta Publica](https://www.car.gov.br/publico/imoveis/index)
- [Dados Abertos SFB](https://www.gov.br/agricultura/pt-br/assuntos/servico-florestal-brasileiro)

## Counts and provenance

On 2026-09-18, the complete DF query had 21,006 features across three pages, including 513 null update timestamps. In an MT query, a zero fiscal-module value is preserved. In two pages of 10,000 features from GO and RS, occurrence selection by update and by creation yields 9,999 properties in each.

Matching counts do not guarantee a transactional snapshot. With several pages, `MetaInfo` carries each one in `source_details["resources"]` (SHA-256 and bytes) and, at the top, the hash of the `{query, resources}` manifest (`hash_kind` `resource_manifest_sha256`).
