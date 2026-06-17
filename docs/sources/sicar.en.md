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
| data_criacao | datetime | Registration creation date |
| data_atualizacao | datetime | Last update (nullable) |
| area_ha | float | Total area in hectares |
| condicao | string | Registration condition (nullable) |
| uf | string | State abbreviation |
| municipio | string | Municipality name |
| cod_municipio_ibge | int | Municipality IBGE code |
| modulos_fiscais | float | Number of fiscal modules |
| tipo | string | IRU (Rural), AST (Settlement), PCT (Indigenous Land) |

## Notes

- **Incremental update:** `imoveis()`, `imoveis_geo()` and `imoveis_geo_stream()` accept
  `atualizado_apos` (CQL `data_atualizacao>'...'`, ISO date or datetime) to fetch only
  records updated after a given date. The filter is applied server-side only: the
  `data_atualizacao` column is not returned by the WFS (it comes back empty in the result).
  Unavailable in SP, RS, PR, SC, RJ and TO — the `data_atualizacao` field does not exist
  in those state WFS layers
- **Geometry available:** `imoveis_geo()` returns a `GeoDataFrame` with MultiPolygon polygons
  (EPSG:4326) via WFS GeoJSON. Requires `pip install agrobr[geo]`. Max 5,000 features per request
- **Transparent pagination:** large queries are paginated automatically (10,000 records per page)
- **Extended timeout:** 180s read timeout for states with many records (BA, MG, MT)
- **SSL:** the CAR GeoServer uses a legacy cipher suite that rejects the standard TLS handshake.
  The client uses a custom SSLContext with `@SECLEVEL=1` to enable compatible ciphers.
- **EUDR relevance:** data essential for compliance with the EU Deforestation Regulation

## License

Open data from the Brazilian federal government. Available via the gov.br CKAN portal.
License: **CC-BY** — free use with attribution to the source.

## Links

- [Portal CAR](https://www.car.gov.br)
- [SICAR Consulta Publica](https://www.car.gov.br/publico/imoveis/index)
- [Dados Abertos SFB](https://www.gov.br/agricultura/pt-br/assuntos/servico-florestal-brasileiro)
