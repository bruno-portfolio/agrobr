# ANTT Toll

## Source

ANTT publishes CSV resources and metadata in its [official open-data catalogue](https://dados.antt.gov.br/dataset/volume-trafego-praca-pedagio). Availability depends on year and frequency. Monthly resources exist from 2010; current daily resources cover 2024 onward.

## Traffic contract 3.0

The output preserves 13 columns and the published collection modality, tariff category and frequency. Its key is `data`, `concessionaria`, `praca`, `sentido`, `tipo_veiculo`, `categoria_eixo`, `tipo_cobranca`, `frequencia`. Collection modalities are not merged. `n_eixos` requires explicit textual evidence; historical category codes are not universal physical axle counts. Literal labels and spaces remain intact.

`frequencia="mensal"` and `"diaria"` select separate resource families. The client does not replace a missing frequency with another. The catalogue determines the selected year/revision; bytes, hashes and parsing statistics describe the actual acquisition.

## Known source anomalies

In the monthly CSVs, 2020, 2021 and 2023 carry `mes_ano` values on a day other than 1 (month end, fill series, typing errors; 56, 3,258 and 734 rows), assigned to their month; 2013, 2015 and 2023 carry fractional or negative volumes (4, 3 and 36 rows), which are dropped; ECOSUL December 2021 is published twice with identical rows and counts once. CONCEBRA 2021–2023 publishes daily rows labelled with day 1 of the month, which are summed into the month. Each case raises a warning and is reported in `source_details`.

## Plaza registry

The current registry can supply state, highway and municipality through an unambiguous literal match. It does not reconstruct historical geography. With a state/highway filter, a plaza without that link (15 pairs in GO, MG and PR in 2026) is dropped from the result with a warning. The official `municipal` header supplies `municipio`; the canonical column takes precedence if both exist. Registry contract: 1.0.1.

## Transport and budgets

Downloads stream to temporary disk files. CSV: 512 MiB; retained spool: 1 GiB; transferred bytes across attempts: 3 GiB. A session is reused within an acquisition, with no persistent CSV cache. Redirects are rejected; HTTP 200 WAF/maintenance HTML raises `SourceUnavailableError`. Files are closed on success, failure and cancellation. Parser row/memory limits are documented in the [API reference](../api/antt_pedagio.md).

Heavy vehicle counts may support transport studies, but do not identify the cargo carried.

## Licence and dictionary

CC-BY as declared in CKAN; no authentication is required. See [data licences](../licenses.md) and the [official dictionary](https://dados.antt.gov.br/dataset/5bf70ec3-b24e-4f73-99a0-78b200f5e915/resource/5cec4e90-24d4-4a43-84e1-121a422bbcd5/download/dicionario_dados_surod_volume-de-trafego-nas-pracas-de-pedagio.pdf).
