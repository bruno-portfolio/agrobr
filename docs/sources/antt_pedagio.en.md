# ANTT Pedagio

## About the source

The **ANTT** (National Land Transport Agency — Agencia Nacional de Transportes Terrestres) publishes open data on vehicle traffic at highway toll plazas on the [dados.antt.gov.br](https://dados.antt.gov.br) portal.

The data is a proxy for crop outflow: heavy commercial vehicles (3+ axles) correlate strongly with the transport of grains by road.

## Datasets

### Traffic Volume (primary)
- **Coverage:** 2010-2025 (16 annual CSVs)
- **Granularity:** Monthly, by plaza/concessionaire/vehicle category
- **Format:** CSV separated by `;`
- **Schema V1 (2010-2023):** With header, category as text ("Categoria 1"-"Categoria 9")
- **Schema V2 (2024+):** No header, axles as a number (2-18)

### Toll Plaza Registry (reference)
- **Format:** Single CSV (~200+ active plazas)
- **Columns:** concessionaire, plaza, highway, state, km, municipality, latitude/longitude, status
- **Use:** Automatic join with traffic data to enrich with state/highway/municipality

## License

CC-BY (Creative Commons Attribution). Open data with no authentication required.

## Category Mapping

| Category V1 | Axles | Type |
|-------------|-------|------|
| 1 | 2 | Passeio |
| 2 | 2 | Comercial |
| 3 | 3 | Passeio |
| 4 | 3 | Comercial |
| 5 | 4 | Passeio |
| 6 | 4 | Comercial |
| 7 | 5 | Comercial |
| 8 | 6 | Comercial |
| 9 | 2 | Moto |

## Technical notes

- `apenas_pesados=True` filters `n_eixos >= 3 AND tipo_veiculo == "Comercial"`
- Volume is aggregated by plaza/month/axle (automatic/manual collection type is summed)
- Default with no year filter = current year + previous (avoids downloading 16 CSVs)
- Encoding: Windows-1252 with automatic fallback chain
