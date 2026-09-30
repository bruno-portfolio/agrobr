# BCB/SICOR — Rural Credit

Rural credit data from the Rural Credit Operations System (SICOR),
made available through the Central Bank's OData API.

## API

```python
from agrobr import bcb

# Working capital (custeio) credit for soybean, 2024/25 crop year
df = await bcb.credito_rural(produto="soja", safra="2024/25", finalidade="custeio")

# Filter by state
df = await bcb.credito_rural(produto="soja", safra="2024/25", uf="MT")

# Aggregate by state (sums municipalities)
df = await bcb.credito_rural(produto="soja", safra="2024/25", agregacao="uf")

# Aggregate by program
df = await bcb.credito_rural(produto="soja", safra="2024/25", agregacao="programa")

# Filter by program
df = await bcb.credito_rural(produto="soja", safra="2024/25", programa="Pronamp")

# Filter by insurance type
df = await bcb.credito_rural(produto="soja", safra="2024/25", tipo_seguro="Proagro tradicional")

# Record by record, with month, funding source, modality and activity
df = await bcb.credito_rural(produto="soja", safra="2024/25", uf="MT", agregacao="registro")
```

## Columns — `credito_rural`

| Column | Type | Description |
|---|---|---|
| `safra` | str | Crop year in the format "2024/25" (YYYY/YY) |
| `produto` | str | Requested product key, unaccented and lower-case (e.g. `algodao`, `cana`), identical for both sources |
| `uf` | str | State |
| `finalidade` | str | Requested purpose, lower-case for both sources (custeio, investimento, comercializacao) |
| `agregacao` | str | Output level: `uf` or `programa` |
| `programa` | str | SICOR program from the official table (PRONAMP, PRONAF, RenovAgro, etc.); null for state aggregation and when the code is null |
| `cd_programa` | str | Program code; null for state aggregation |
| `qtd_contratos` | int | Number of contracts |
| `valor` | float | Financed amount (R$) |
| `area_financiada` | float | Financed area (ha); null when no record in the group publishes an area, which always holds through OData (custeio publishes an empty `AreaCusteio`; investimento and comercializacao carry no area); only the BigQuery fallback fills it |
| `fonte` | str | `bcb_odata` or `bcb_bigquery` |

These are the 11 columns of contract 2.0 (`docs/contracts/credito_rural.en.md`), in the `uf` and `programa`
aggregations: there the insurance type only serves the `tipo_seguro` filter, and the issue year and month build the crop
year. With `agregacao="registro"`, the output is record by record, without aggregation, with 23 columns: the 11 plus
`ano_emissao`, `mes_emissao`, `regiao`, `cd_sub_programa`, `cd_fonte_recurso`, `fonte_recurso`, `cd_tipo_seguro`,
`tipo_seguro`, `cd_modalidade`, `modalidade`, `cd_atividade` and `atividade`, in the
[bcb.credito_rural_registro](../contracts/bcb_credito_rural_registro.en.md) 1.0 contract.

## SICOR Dimensions

The API returns dimension codes (`cdPrograma`, `cdTipoSeguro`, etc.). Program and insurance-type names follow the
official BCB domain tables (`https://www.bcb.gov.br/htms/sicor/Programa.csv` and `TipoGarantiaEmpreendimento.csv`):
the published program is the part of the official description before the first " - " (the
whole description when there is no such separator; stray source quotes removed); the insurance type is the official
description. Name filters are case-insensitive (`programa="pronamp"`). Unknown codes produce
`"Desconhecido ({code})"` with a log warning; a null code keeps a null name, without a warning. The official
description of `0152` records that it was Moderinfra until 2021-06-30; the published name is the current one.

| Code | Published program | Official description | Official validity |
|---|---|---|---|
| `0001` | PRONAF | PRONAF - PROGRAMA NACIONAL DE FORTALECIMENTO DA AGRICULTURA FAMILIAR | 01/11/2011 a 31/12/2099 |
| `0050` | PRONAMP | PRONAMP - PROGRAMA NACIONAL DE APOIO AO MÉDIO PRODUTOR RURAL | 01/11/2011 a 31/12/2099 |
| `0070` | FUNCAFÉ (PROGRAMA DE DEFESA DA ECONOMIA CAFEEIRA) | FUNCAFÉ (PROGRAMA DE DEFESA DA ECONOMIA CAFEEIRA) | 02/07/2012 a 31/12/2099 |
| `0100` | PRLC-BA (PROG RECUP LAVOURA CACAUEIRA BAIANA) ENCERRADO | PRLC-BA (PROG RECUP LAVOURA CACAUEIRA BAIANA) ENCERRADO | 01/11/2011 a 30/09/2022 |
| `0110` | PRODECER III | PRODECER III - PROG COOP NIPO-BRASILEIRA P DESENV DOS CERRADOS - ENCERRADO | 01/11/2011 a 30/09/2022 |
| `0151` | PROCAP-AGRO (PROGRAMA DE CAPITALIZAÇÃO DAS COOPERATIVAS DE PRODUÇÃO AGROPECUÁRIAS) | PROCAP-AGRO (PROGRAMA DE CAPITALIZAÇÃO DAS COOPERATIVAS DE PRODUÇÃO AGROPECUÁRIAS) | 01/11/2011 a 31/12/2099 |
| `0152` | PROIRRIGA | PROIRRIGA - antigo Moderinfra, alterado em 01/07/2021 | 01/11/2011 a 31/12/2099 |
| `0153` | MODERAGRO | MODERAGRO - PROGRAMA DE MODERNIZAÇÃO DA AGRICULTURA E CONSERVAÇÃO DE RECURSOS NATURAIS | 01/11/2011 a 30/06/2025 |
| `0154` | MODERFROTA | MODERFROTA - PROGRAMA DE MODERNIZAÇÃO DA FROTA DE TRATORES AGRÍCOLAS E IMPL ASSOC E COLHEITADEIRAS | 01/11/2011 a 31/12/2099 |
| `0155` | PRODECOOP | PRODECOOP - PROGRAMA DE DESENVOLVIMENTO COOPERATIVO PARA AGREGAÇÃO DE VALOR À PRODUÇÃO AGROPECUÁRIA | 01/11/2011 a 31/12/2099 |
| `0156` | ABC + Programa para a Adaptação à Mudança do Clima e Baixa Emissão de Carbono | ABC + Programa para a Adaptação à Mudança do Clima e Baixa Emissão de Carbono | 01/11/2011 a 30/06/2023 |
| `0157` | PSI-RURAL | PSI-RURAL - PROG SUSTENTAÇÃO  INVESTIMENTO ENCERRADO | 01/11/2011 a 31/05/2016 |
| `0158` | PROCAP-CRED (PROG CAPIT COOP CRÉDITO) ENCERRADO | PROCAP-CRED (PROG CAPIT COOP CRÉDITO) ENCERRADO | 01/11/2011 a 05/04/2016 |
| `0159` | MODERMAQ | MODERMAQ - PROG MOD PARQUE IND NACIONAL - ENCERRADO | 06/08/2004 a 30/06/2016 |
| `0160` | PRI | PRI - PROGRAMA DE REFORÇO DO INVESTIMENTO (CIRC 3.745) - ENCERRADO | 01/01/2013 a 31/12/2015 |
| `0161` | PRORENOVA-RURAL- PROG APOIO  RENOV IMPLANTAÇÃO NOVOS CANAVIAIS- ENCERRADO | PRORENOVA-RURAL- PROG APOIO  RENOV IMPLANTAÇÃO NOVOS CANAVIAIS- ENCERRADO | 18/06/2013 a 31/12/2018 |
| `0162` | INOVAGRO | INOVAGRO - Programa de Incentivo à Inovação Tecnológica na Produção Agropecuária | 01/07/2013 a 31/12/2099 |
| `0163` | PCA | PCA - Programa para Construção e Ampliação de Armazéns | 01/07/2013 a 31/12/2099 |
| `0164` | PRORENOVA-IND- PROG APOIO RENOV IMPLANT NOVOS CANAVIAIS | PRORENOVA-IND- PROG APOIO RENOV IMPLANT NOVOS CANAVIAIS - ENCERRADO | 18/06/2013 a 07/07/2017 |
| `0165` | PROAQÜICULTURA-PROG APOIO DESENVSETOR AQUÍCOLA | PROAQÜICULTURA-PROG APOIO DESENVSETOR AQUÍCOLA - ENCERRADO | 01/07/2013 a 30/09/2022 |
| `0180` | FNO-ABC (PROG FINANC AGRICULTURA BAIXO CARBONO) ENCERRADO | FNO-ABC (PROG FINANC AGRICULTURA BAIXO CARBONO) ENCERRADO | 01/01/2015 a 30/06/2015 |
| `0200` | PROCERA | PROCERA - PROG ESPECIAL DE CRÉDITO PARA A REFORMA AGRÁRIA - ENCERRADO | 01/01/1984 a 30/09/2022 |
| `0201` | PROGRAMA NACIONAL DE CRÉDITO FUNDIÁRIO (FTRA) | PROGRAMA NACIONAL DE CRÉDITO FUNDIÁRIO (FTRA) | 01/01/2013 a 31/12/2099 |
| `0222` | RenovAgro | RenovAgro - Programa de Financiamento a Sistemas de Produção Agropecuária Sustentáveis | 01/07/2023 a 31/12/2099 |
| `0240` | ANF | ANF - ATIVIDADE NÃO FINANCIADA ENQUADRADA NO PROAGRO | 01/01/1984 a 31/12/2099 |
| `0721` | Linha Crédito Rural instit Res. 4.028/2011 (Dívidas Composição e Renegoc PRONAF) | Linha Crédito Rural instit Res. 4.028/2011 (Dívidas Composição e Renegoc PRONAF) - ENCERRADO | 01/01/2013 a 15/10/2014 |
| `0722` | Linha Crédito Rural inst Res. 4.029/2011 (Reneg Crédito Fundiário) ENCERRADO | Linha Crédito Rural inst Res. 4.029/2011 (Reneg Crédito Fundiário) ENCERRADO | 01/01/2013 a 31/12/2013 |
| `0730` | Linha Crédito Rural inst Res. 4.083/2012 (Enchentes Reg Norte) ENCERRADO | Linha Crédito Rural inst Res. 4.083/2012 (Enchentes Reg Norte) ENCERRADO | 01/01/2013 a 31/12/2013 |
| `0735` | Linha de Crédito Rural instituida pela Res. 4.126/2012 (Produtores de Maçã) ENCERRADO | Linha de Crédito Rural instituida pela Res. 4.126/2012 (Produtores de Maçã) ENCERRADO | 01/01/2013 a 31/12/2013 |
| `0776` | Linha Crédito Rural Inst Res. 4.147/2012 e 4.260/2013 (Agricultores Familiares) ENCERRADO | Linha Crédito Rural Inst Res. 4.147/2012 e 4.260/2013 (Agricultores Familiares) ENCERRADO | 01/01/2013 a 31/12/2015 |
| `0777` | Linha Crédito Rural inst Res. 4.147/2012 e 4.260/2013 (Demais Agricultores) ENCERRADO | Linha Crédito Rural inst Res. 4.147/2012 e 4.260/2013 (Demais Agricultores) ENCERRADO | 26/10/2012 a 31/12/2015 |
| `0779` | Linha de Crédito Rural instituida pela Res. 4.161/2012 (Produtores de Arroz) ENCERRADO | Linha de Crédito Rural instituida pela Res. 4.161/2012 (Produtores de Arroz) ENCERRADO | 01/01/2013 a 31/12/2013 |
| `0783` | Linha Crédito Rural inst pelas Res 4.189 e 4.212/2013-PRONAF (Estiagem Area Sudene) ENCERRADO | Linha Crédito Rural inst pelas Res 4.189 e 4.212/2013-PRONAF (Estiagem Area Sudene) ENCERRADO | 01/01/2013 a 31/12/2014 |
| `0784` | Linha Credito Rural inst Res. 4.188 e 4.211/2013-Demais Produtores (Estiagem Area Sudene) ENCERRADO | Linha Credito Rural inst Res. 4.188 e 4.211/2013-Demais Produtores (Estiagem Area Sudene) ENCERRADO | 01/01/2013 a 31/12/2014 |
| `0785` | Linha Crédito Rural inst  Res. 4.220/2013 (Recursos BNDES-Estiagem Área da Sudene) ENCERRADO | Linha Crédito Rural inst  Res. 4.220/2013 (Recursos BNDES-Estiagem Área da Sudene) ENCERRADO | 02/05/2013 a 30/06/2014 |
| `0786` | Linha de Crédito Rural Instituída pela Res. 4.289/2013 (Renegociação Café Arábica) ENCERRADO | Linha de Crédito Rural Instituída pela Res. 4.289/2013 (Renegociação Café Arábica) ENCERRADO | 25/11/2013 a 31/07/2014 |
| `0790` | Linha de Crédito Rural inst Res 5.120/2024 (Linha emergencial Custeio Pecuário) | Linha de Crédito Rural inst Res 5.120/2024 (Linha emergencial Custeio Pecuário) | 07/02/2024 a 30/06/2024 |
| `0888` | Outras Linhas de Crédito Rural não Especificadas | Outras Linhas de Crédito Rural não Especificadas - ENCERRADO | 01/01/2014 a 31/01/2014 |
| `0901` | Eco Invest Brasil | Eco Invest Brasil - RES CMN Nº 5.130/2024 | 24/03/2026 a 01/01/2099 |
| `0999` | FINANCIAMENTO SEM VÍNCULO A PROGRAMA ESPECÍFICO | FINANCIAMENTO SEM VÍNCULO A PROGRAMA ESPECÍFICO | 02/07/2012 a 31/12/2099 |

| Code | Published insurance type (official description) |
|---|---|
| `0` | Não se aplica |
| `1` | Proagro tradicional |
| `2` | Proagro mais |
| `3` | Outro seguro |
| `9` | Sem adesão a seguro |

With `agregacao="registro"`, funding source, modality and activity carry the whole official description from
`FonteRecursos.csv` (37 codes), `Modalidade.csv` (64) and `Atividade.csv` (2). For funding
sources, the part before " - " would merge distinct codes (4 sources would become "POUPANÇA RURAL"). A code outside the
table gets a null name, without a guess or a warning. `cd_modalidade` comes out as the source publishes it (`"01"`), and
the name is resolved by the table's number (`"1"` = LAVOURA). The sub-programme comes out as the code only, as in 1.1.0.

## Purposes

- `custeio` — production financing
- `investimento` — purchase of machinery, infrastructure
- `comercializacao` — marketing financing
- `industrializacao` — agro-industrialisation financing, which SICOR publishes only in the total by state, without product

> **Note:** `credito_rural` (by product) accepts the first three. Agro-industrialisation is in the `RegiaoUF` entity, read by `bcb.credito_rural_total`, with the four purposes by state, programme and month.

## Products

Soybean, corn, coffee, cotton, rice, wheat, beans, sugarcane, cassava,
sorghum, oats, barley, among others. Use the agrobr canonical name.

## MetaInfo

```python
df, meta = await bcb.credito_rural(produto="soja", safra="2024/25", return_meta=True)
print(meta.source)          # "bcb_credito"
print(meta.schema_version)  # "2.0"
```

With `agregacao="registro"`, `schema_version` and `contract_version` are `"1.0"`, and `source_details["contract"]` is
`"bcb.credito_rural_registro"`.

`source_url` is the requested OData query, with `$filter` and `$select` and without `$top`. `raw_content_hash` is the
SHA-256 of the canonical `{query, resources}` manifest (`source_details["hash_kind"] = "resource_manifest_sha256"`), and
`raw_content_size` its size; `source_details["resources"]` lists each received page (URL, SHA-256, bytes, and time),
including the 12 of the per-month split when the volume goes over the Olinda limit. `fetched_at` is the most recent page's.
BCB revises closed months (RS, Jun 2023: 55 → 54 contracts). The manifest hash changes on every call, because it carries the
time of each page: it identifies the acquisition. To know whether the base changed between 2 queries, compare the `sha256` of
each item of `source_details["resources"]`. The BigQuery fallback does
not declare the base date.

No cache: every call queries the source (BCB's OData or, on the fallback, BigQuery). The same holds for SGS, PTAX, and
Focus.

## Status (Sep/2026)

The SICOR API was restructured (~2024). agrobr reads `CusteioRegiaoUFProduto`,
`InvestRegiaoUFProduto` and `ComercRegiaoUFProduto` (by product, in `credito_rural`) and
`RegiaoUF` (total by state and purpose, in `credito_rural_total`). The service also publishes
municipality by product (`CusteioMunicipioProduto`, `InvestMunicipioProduto`) and municipality
without product (`CusteioInvestimentoComercialIndustrialSemFiltros`), which agrobr does not
read.

Source notes, checked for 2022 and 2023:

- there is no total or Brazil row: the national total is the sum of the states;
- `RegiaoUF` equals the sum of the `SemFiltros` municipalities by state and purpose, and the
  by-product sum for the three purposes that have products;
- programme, sub-programme and funding-source codes may come with or without leading zeros
  depending on the entity ("1" × "0001"): normalise before comparing;
- for investment, the sub-programme differs between `RegiaoUF` and `SemFiltros`: never join the
  two by sub-programme;
- `RegiaoUF` and `SemFiltros` cover Jan 2013 to the last closed month; `200` with `value: []`
  only appears outside that coverage (Dec 2012 and the current month).

The client uses equality on `nomeProduto`, including SICOR's literal double
quotes, and on `nomeUF` for states. Crop years are also constrained server-side through
`AnoEmissao` and `MesEmissao`; both filters are checked again client-side so an
inconsistent source response cannot reach the parser. Program and insurance type
filters remain client-side.

Retry with exponential backoff (6 attempts, 120s read timeout).
The API returns HTTP 500 intermittently. Since v0.8.0, agrobr
uses Base dos Dados (BigQuery) as an automatic fallback when the OData
API fails. Install with `pip install agrobr[bigquery]`. The fallback only applies to
`agregacao="uf"` without `programa` or `tipo_seguro`: the table aggregates by municipality and
carries no programme, funding source, insurance type, modality or activity. In the other cases,
OData being down raises `SourceUnavailableError`, with the reason in the message.

## SGS — Time Series

SGS publishes observations by code at `api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados?formato=json`, without authentication. Observation responses contain date and value (some series, such as TR, also carry `dataFim`, the end of the period), without a frequency/unit catalogue or a global count.

The [official series 1 catalogue](https://dadosabertos.bcb.gov.br/dataset/1-taxa-de-cambio---livre---dolar-americano-venda---diario) states a ten-year daily-query limit effective March 26, 2025. agrobr partitions ranges into blocks ending on calendar-year boundaries and validates their union. Series 1's `/ultimos/N` route allows up to 20; use dates with `ultimos` in the [SGS API](../api/bcb.en.md#sgs) for larger tails.

```python
from agrobr import bcb

df, meta = await bcb.sgs(
    1, inicio="01/01/2010", fim="31/12/2024", return_meta=True,
)
ipca = await bcb.sgs("ipca", inicio="01/01/2024", fim="31/12/2024")
```

The example query returns 3,767 daily observations for 2010–2024 across two blocks. The source may also return monthly and quarterly references before the requested daily bound, and repeat a monthly reference across disjoint daily windows. The implementation preserves published reference dates, diagnoses bounds, and reconciles only identical values across blocks. It does not infer frequency or fill dates.

[Contract 2.1](../contracts/bcb_sgs.en.md) retains `data`, `valor`, `codigo`, and `nome_serie`, including empty output, and adds `data_fim` when the series publishes `dataFim`. The 17 aliases remain available; other integer codes can be queried without inventing names. IPCA values can be negative. Check units and frequency in the particular series' catalogue; a historical exchange-rate code does not imply BRL throughout its history.

Metadata records every final response, hash, status, UTC acquisition, and references outside the requested bounds. Obtaining every planned block does not establish series completeness because the endpoint provides no independent total. The official missing-values 404 does not establish that a code exists. The [verified ODbL license](../licenses.en.md#bcb-sgs) belongs to the series 1 catalogue and was not generalized to arbitrary codes.

---

## PTAX — Currencies, Quotes and Bulletins

The [official daily bulletin dataset](https://dadosabertos.bcb.gov.br/dataset/taxas-de-cambio-todos-os-boletins-diarios) provides quotes, parities, and currency metadata. `bcb.ptax` now selects a currency and `fechamento`, `todos`, `abertura`, or `intermediario`. USD closing remains the default. `bcb.ptax_moedas` exposes the current `Moedas` catalogue.

The `Moedas` catalogue lists AUD, CAD, CHF, DKK, EUR, GBP, JPY, NOK, SEK, and USD. Those entries describe this OData service, without establishing the full historical currency universe. The [portal's general currency table](https://ptax.bcb.gov.br/ptax_internet/consultarTabelaMoedas.do?method=consultaTabelaMoedas) contains additional codes and exclusion dates; it is a separate family.

```python
from agrobr import bcb

moedas = await bcb.ptax_moedas()
df, meta = await bcb.ptax(
    moeda="EUR", boletim="todos",
    inicio="03/09/2026", fim="06/09/2026", return_meta=True,
)
```

The [quote contract 2.0 and catalogue contract 1.0](../contracts/bcb_ptax.en.md) retain eight and three columns respectively. Quotes preserve four measures, currency, bulletin text, timestamp, and civil date. There is no monetary conversion or aggregation across bulletins.

**Route distinctions:** generic USD closing matched the legacy dollar endpoint in recent periods and in June 1994. The day route calls the closing bulletin `Fechamento PTAX`; the period route calls it `Fechamento`, with matching quote values and timestamps. The selector recognizes both while output retains the original label. In the same interval, the specialized closing endpoint returned one closing although the generic route returned two; the specialized opening/intermediate endpoint returned only the last intermediate. They are not used to replace the generic quote routes.

Published fractional timestamps remain distinct: an intermediate and closing can share the same second while differing in microseconds. `data_hora` stays naive, with ns dtype; `data` is its civil date. UTC acquisition is recorded separately. Quotes refer to the domestic monetary unit applicable at the historical date; the SDK does not label all historical values BRL. Type A parities express selected currency/USD, type B USD/selected currency, with catalogue context retained.

In known queries, a weekend and unsupported ZZZ/ARS selections returned the same empty envelope. Therefore currency selection is validated against the acquired catalogue before quotes are requested. Data acquisition uses ordered pages and validates every record before bulletin filtering. Known queries returned no independent count; terminal empty pages leave coverage unknown. Count/nextLink annotations are validated if present, without an atomic revision guarantee.

The dataset and its [currency](https://dadosabertos.bcb.gov.br/dataset/taxas-de-cambio-todos-os-boletins-diarios/resource/9d07b9dc-c2bc-47ca-af92-10b18bcd0d69), [day](https://dadosabertos.bcb.gov.br/dataset/taxas-de-cambio-todos-os-boletins-diarios/resource/db9b40bf-9b8f-47c4-a82d-3a3afab52e90), and [period](https://dadosabertos.bcb.gov.br/dataset/taxas-de-cambio-todos-os-boletins-diarios/resource/0439af6a-d9be-4bf7-bf1a-60583e5f4c1c) resources state ODbL; see [license details](../licenses.en.md#bcb-ptax). The broader all-currencies CSV, exclusions, and revision history are outside this API. See [parameters, errors, dtypes, and provenance](../api/bcb.en.md#ptax).

---

## Focus — Market Expectations

The [official Market Expectations dataset](https://dadosabertos.bcb.gov.br/dataset/expectativas-mercado) publishes aggregated forecast statistics from survey participants. Its catalogue describes daily calculation and publication on the first working day of the week. The module queries statistical history through OData, using annual or monthly entities.

| Frequency | Entity |
|-----------|--------|
| anual, default | `ExpectativasMercadoAnuais` |
| mensal | `ExpectativaMercadoMensais` |

```python
from agrobr import bcb

df, meta = await bcb.focus(
    "IPCA", periodicidade="mensal", inicio="2026-08-28",
    top=100, max_registros=30, return_meta=True,
)
```

The API retains ten columns and adds `periodicidade` and `indicador_detalhe`, with [contract 2.0](../contracts/bcb_focus.en.md). Survey date and textual forecast horizon are separate dimensions. Annual detail retains, for example, Exportações, Importações, and Saldo for Balança comercial. Bases 0/1 on the same date/reference remain separate. The function provides no generic indicator catalogue or guarantee that an indicator exists in both entities.

In known queries, with reference/base/detail tie-breakers, one page of six records matched two pages of three for each entity, and no independent count or nextLink came back: `$count=true` was ignored, `$inlinecount` was refused, and `/$count` returned 403. Metadata distinguishes local limits and observed termination from completeness. Concurrent revisions remain possible; this is not an atomic snapshot.

The catalogue omits periods without statistics. Empty output does not validate the indicator; lowercase `ipca` was empty where `IPCA` had records. The [API](../api/bcb.en.md#focus) documents selection, pagination, dtypes, errors, and statistical warnings without filling periods or inferring units.

The catalogue and its [monthly](https://dadosabertos.bcb.gov.br/dataset/expectativas-mercado/resource/c26059cc-2b28-41c5-a258-88a7c2b664d0) and [annual](https://dadosabertos.bcb.gov.br/dataset/expectativas-mercado/resource/57d46ecb-4d27-45e9-8145-c173e0b94ff5) resources declare ODbL; see [licenses](../licenses.en.md#bcb-focus). Other entities, including quarterly, Selic, and Top5, remain outside this selector. Institutional microdata is not part of this output.

---

## Source

- SICOR API: `https://olinda.bcb.gov.br/olinda/servico/SICOR/versao/v2/odata`
- SGS API: `https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados`
- PTAX API: `https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/`
- Focus API: `https://olinda.bcb.gov.br/olinda/servico/Expectativas/versao/v1/odata/`
- Update frequency: monthly (SICOR), several times a day (PTAX), series-dependent (SGS); Focus: daily calculation and weekly publication
- History: 2013+ (SICOR), variable (SGS)
- Contracts: SGS 3.0; Focus, PTAX quotes and rural credit 2.0; PTAX currencies 1.0; see each API

## Products and empty responses

SICOR filtering uses exact equality with the published name, including literal double quotes. This excludes corn silage from corn queries and wheat silage or buckwheat from wheat queries. Aliases such as `cafe`, `feijao`, `algodao`, `cana`, and `mandioca` are mapped to source spelling for the filter, while `produto` publishes the requested key, unaccented and lower-case. `cafe_arabica` and `cafe_conilon` were removed: this SICOR dataset does not distinguish these types; use `cafe`.

`custeio` uses agricultural products. `investimento` uses investment items such as BOVINOS, CAFÉ, CANA-DE-AÇUCAR, BANANA, and tractors; soybean and corn may have no records. Valid queries without records return an empty DataFrame matching rural-credit contract 2.0 and an explanatory warning, not `ParseError`.
