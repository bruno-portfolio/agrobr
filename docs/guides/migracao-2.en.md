# Migrating to agrobr 2.0

agrobr 2.0 makes explicit errors and guarantees that the 1.x series handled
permissively. Use this guide to update existing integrations. Each dataset's
contract version remains independent from the library version, as described in
the [SemVer policy](../contracts/semver.md).

**How to test the migration:** a loose mock (`patch(...)`, `AsyncMock()`) accepts any argument and hides the call that
2.0 rejects ([§78](#78-argument-outside-the-signature-raises-typeerror)). Create mocks with `patch(..., autospec=True)`,
or check each call with `inspect.signature(function).bind(*args, **kwargs)`: both raise `TypeError` for an argument
outside the signature, as 2.0 does. In functions that keep `**kwargs` (such as those in
[§78](#78-argument-outside-the-signature-raises-typeerror)), the signature accepts any name, and only the call itself
rejects an unknown one: there, neither `autospec` nor `bind` flags `zarc.zoneamento(cultura=...)`,
`mapbiomas.cobertura(estado=...)` or `desmatamento.deter(data_inicio=...)`. Check those calls against the table in
[§85](#85-parameter-names-one-vocabulary-across-the-api).

## Summary: what breaks

In order of risk. Each line points to the section with the details.

**Silent changes (the same call returns something else)**

1. `bcb.credito_rural` without `agregacao` returns the per-state aggregate (11 columns), no longer the SICOR records
   (21 columns): for the 1.1.0 cut, pass `agregacao="registro"` ([§4](#4-bcbcredito_rural-moves-to-contract-20)).
2. `comtrade.comercio` with `partner=None`, and `datasets.comercio_internacional` with `parceiro=None`, return the
   World aggregate, no longer all published partners: for the 1.1.0 cut, pass `"all"` or `"todos"`
   ([Comtrade](#comtrade-world-coverage-and-contracts-20)). `trade_mirror` requires an explicit partner: `None`,
   World, `"all"` and `"todos"` raise `InvalidParameterError`.
3. The rural credit `safra` comes as "2023/24", not "2023/2024": convert stored data with
   `agrobr.normalize.dates.normalizar_safra` ([§76](#76-rural-credit-the-crop-year-comes-as-yyyyyy)).
4. Datasets return the columns in contract order, and `queimadas` `data` comes as `datetime64`: read columns by name, not by
   position ([§74](#74-datasets-columns-in-contract-order-and-dates-as-datetime64)).
5. Renamed values: `nome_serie` comes as `"ipa_agricola"` (was `"ipa_agropecuario"`), and `especie` as `"galinhas"` (was
   `"galinhas_poedeiras"`). The old names are still accepted as input, with a `FutureWarning`, but filters on the output must
   change ([§31](#31-sgs-ipa_agricola-instead-of-ipa_agropecuario), [§26](#26-ppm-galinhas-replaces-galinhas_poedeiras)).
6. `conab.safras(produto, safra=X)` without `levantamento` reads X from the most recent publication, with CONAB's revision: for
   the original figure, pass `levantamento` ([§27](#27-conab-past-crop-years-come-from-the-most-recent-revision)).
7. `ibge.censo_agro_municipal_1985` returns one row per PDF cell, with `valor` only in confirmed cells: use `valor_lido` and
   filter by `status` ([§81](#81-municipal-1985-census-cell-by-cell-each-with-its-status-contract-20)).
8. agrobr's logs no longer follow the application's structlog configuration: they go through the standard library's
   `logging`, as JSON, and are controlled with `logging.getLogger("agrobr")` ([§73](#73-logs-off-standard-output)).
9. `municipio` matches the full name or the IBGE code, no longer a substring of the name, in SICAR, MapBiomas, PSR, ZARC
   and ANP: the same call can return another municipality, more rows or fewer ([§86](#86-municipality-full-name-or-ibge-code)).
10. Renamed output columns: `estado` → `uf` in CONAB progress and MapBiomas; Portuguese columns in
    `posicionamento_fundos`, `oferta_demanda_global` and `comercio_internacional`; a normalized `produto` in
    `conab.brasil_total`. Code that only exports the frame gets another schema, without an error ([§87](#87-renamed-output-columns)).
11. Date columns that were text come as `datetime64` (DERAL, IMEA, the INMET catalogue, ANTAQ and ANTT plazas): a text
    filter in `dd/mm/yyyy` with a day of 12 or less matches another date, and DERAL's filter by sheet name (`"Atual"`,
    `"02-10-23"`) can come back empty, with no warning ([§89](#89-output-dtypes)).
12. Integers by nature come as `Int64`, text in the pandas default dtype and the empty result with the full result's
    dtypes; 6 contracts from 1.1.0 move to 2.0 because of it ([§89](#89-output-dtypes)).
13. `AGROBR_CACHE_DIR` now works, and `true`/`yes` now disable the ANEC and Acervo Fundiário caches
    ([§91](#91-environment-variables)).
14. IBAMA keeps the CSV for 1 hour: within the hour, the same call returns the previous collection
    ([§94](#94-heavy-downloads-and-cache)).
15. `ibge produtos --pesquisa PAM` queries PAM, no longer LSPA ([§92](#92-cli-formato-in-every-command-and-snapshot-use-removed)).
16. The dataset and contract catalog, `normalize` and ANEC return copies: mutating the returned object no longer changes
    the next call ([§95](#95-returns-are-copies)).
17. `mapbiomas.cobertura`, `mapbiomas.transicao` and `datasets.uso_do_solo` without `colecao` read collection 11, no longer
    10: the same call returns other figures. For the 1.1.0 cut, pass `colecao=10`
    ([§63](#63-mapbiomas-a-class-outside-the-legend-comes-out-null-with-a-warning)).
18. From CONAB, `area_colhida` comes out null in `conab.safras` and `estimativa_safra`, and `data_publicacao` is the bulletin
    date, no longer the day of the query ([§3](#3-conabsafras-moves-to-contract-20)).
19. `acervo_fundiario.sigef(uf)` returns the same parcels in a different order (public, then private) and with the new
    `natureza` column; `source_details` becomes per file ([§29](#29-land-registry-public-and-private-sigef-snci-per-state)).
20. `antt_pedagio.fluxo_pedagio` comes with `sentido` in upper case (`CRESCENTE`, `DECRESCENTE`): filtering and `groupby`
    by the published text change, and volumes do not ([§24](#24-antt-preserves-collection-modality-and-frequency)).
21. `anp_diesel.vendas_diesel` comes with `produto` `DIESEL S10` (was `DIESEL S-10`) and `regiao` by the canonical name
    (`Centro-Oeste`, was `REGIÃO CENTRO-OESTE`): a filter by the old text comes back empty ([§97](#97-other-per-source-changes)).
22. `agrobr conab levantamentos` lists every survey, not just the first 10 ([§92](#92-cli-formato-in-every-command-and-snapshot-use-removed)).
23. `MetaInfo.license` and `datasets.info()["licenses"]` change class: IMEA and Notícias Agrícolas become `zona_cinza`,
    Comtrade `restrito` and Acervo Fundiário `livre`; with CEPEA and Notícias Agrícolas together, `nc`. Review license
    filters ([§11](#11-new-license-and-fallback-warnings)).

**Now raises**

24. HTTP error statuses raise `SourceUnavailableError`, not `httpx.HTTPStatusError`: change the `except`
   ([§75](#75-http-error-statuses-sourceunavailableerror-not-httpxhttpstatuserror)).
25. An unknown or misspelled argument, which 1.1.0 silently dropped, raises before the network: `TypeError` in the 77
    functions that lost `**kwargs`, and `TypeError` or `InvalidParameterError` in the 33 that keep it: fix the name
    ([§78](#78-argument-outside-the-signature-raises-typeerror)).
26. An impossible argument (unknown state, inverted period) raises `InvalidParameterError` before the network, where 1.1.0
    returned empty data or a source error; in 2.0, the rule applies to every source
    ([§66](#66-impossible-arguments-rejected-before-the-network), [§90](#90-errors-the-class-tells-the-cause)).
27. `as_polars=True` without Polars raises `ImportError` instead of returning pandas: install `agrobr[polars]`
    ([§8](#8-as_polarstrue-requires-polars)). The extra now requires `polars>=0.20.3`: with an older Polars pinned, the
    install reports a conflict ([§19](#19-dependencies-failures-and-structured-output)).
28. The old parameter names (`data_inicial`, `start`, `commodity`, `cultura`, `estado`, `cod_municipio`,
    `max_features` and others) raise `TypeError` (`InvalidParameterError` in `zarc.zoneamento`, `mapbiomas.cobertura` and
    `mapbiomas.transicao`): use the new name ([§85](#85-parameter-names-one-vocabulary-across-the-api)).
29. `as_polars`, `return_meta` and the options after them become keyword-only; in IBGE, the secondary filters too
    ([§88](#88-flags-and-secondary-filters-by-keyword-only)).
30. A layout failure in every source of a dataset raises `ParseError`, no longer `SourceUnavailableError`
    ([§90](#90-errors-the-class-tells-the-cause)).
31. In the CLI, `--output`, `--json` and `snapshot use` exit with code 2: use `--formato` ([§92](#92-cli-formato-in-every-command-and-snapshot-use-removed)).
32. `precos_diesel` with a period starting after today raises `InvalidParameterError` before the network, where 1.1.0
    returned an empty result for a state or Brazil ([§97](#97-other-per-source-changes)).
33. `embrapa_solos.mapa_solos(ordem=...)` matches the whole class: a name fragment (`"latos"`) raises
    `InvalidParameterError` with the list of classes ([§97](#97-other-per-source-changes)).
34. `AGROBR_HTTP_RATE_LIMIT_<SOURCE>` and a `rate_limit_*` argument reject negative values, `inf` and `nan` with
    `ValidationError` ([§91](#91-environment-variables)).
35. `cadastro_rural`, `desmatamento`, `exportacao`, `importacao`, `uso_do_solo` and `zoneamento_agricola` inside
    `datasets.deterministic(...)` raise `InvalidParameterError` before the network, where 1.1.0 returned current data: move
    those calls outside the block ([§71](#71-deterministic-mode-a-warning-where-the-mode-does-not-apply)).
36. `normalize.municipio_para_ibge(nome)` without `uf` raises `InvalidParameterError` when the name belongs to more than one
    municipality (521 municipalities share 240 names), where 1.1.0 silently returned the code of the first one on the list:
    pass `uf` ([§90](#90-errors-the-class-tells-the-cause)).
37. `abiove.exportacao(ano, produto="total")` raises `InvalidParameterError`, where 1.1.0 returned empty: for the total
    of all products, use `agregacao="mensal"` ([§39](#39-abiove-latest-edition-and-edicao)).

**No longer raises**

38. `datasets.estimativa_safra` with no observations in every source returns the contract's empty frame with
    `UserWarning`, no longer `SourceUnavailableError`: check `df.empty` ([estimativa_safra](#estimativa_safra-contract-31-and-temporal-selection)).
39. `cftc.cot` with no report in the period returns a typed empty frame, no longer `SourceUnavailableError`; counts come as
    `Int64` ([§99](#99-cftc-a-period-without-reports-returns-a-typed-empty-frame)).

**Removed API**

40. `agrobr.configure()`, the `quality`, `sla`, `export`, `plugins` and `validators.semantic` modules and
    `load_baseline_fingerprint` are gone, and `cache.get_policy` now applies to CEPEA only: remove the uses
    ([§9](#9-experimental-modules-were-removed), [§10](#10-agrobrconfigure-was-removed), [§50](#50-dead-code-cleanup),
    [§83](#83-cache-the-cepea-policy-only-and-load_baseline_fingerprint-is-gone)).
    The `uf=` argument of `anda.entregas` and `datasets.fertilizante` is also gone: it was an explicit parameter in 1.1.0,
    and passing it raises `TypeError`, because the PDFs only carry the national total ([§50](#50-dead-code-cleanup)).
41. The `agrobr.conab.custo_producao` and `agrobr.conab.serie_historica` subpackages are gone: import the functions
    from `agrobr.conab` ([§93](#93-conab-one-path-per-function)).
42. `b3.oi_historico` is renamed `b3.posicoes_abertas_historico`, also in `sync.b3`; the `futuros_agricolas`
    `tipo="oi_historico"` stays ([§98](#98-b3-oi_historico-is-renamed-posicoes_abertas_historico)).
43. The `_moeda` parameter of `cepea.indicador` is removed: passing it raises `TypeError` ([§6](#6-cepea-rejects-invalid-parameters)).

To stay on the 1.x series while you migrate: `pip install "agrobr<2"`.

## BCB PTAX: currencies, bulletins, and explicit contracts

`bcb.ptax` adds `moeda="USD"`, `boletim="fechamento"`, and `top=1000`. The default remains the dollar closing; use `boletim="todos"` for opening, intermediate, and closing bulletins, or select `abertura`/`intermediario`. New `bcb.ptax_moedas()` exposes the current OData catalogue in three columns (`moeda`, `nome`, `tipo_moeda`), with contract 1.0. Quote acquisition now depends on that catalogue for currency validation, with explicit resources and failures; it is not a historical validity table.

Quotes move to contract **2.0**, parser **2**, retaining `cotacao_compra`, `cotacao_venda`, `data_hora`, `data` as the prefix and adding `moeda`, `paridade_compra`, `paridade_venda`, `tipo_boletim`. Measures use finite nullable float64; data_hora/data use timezone-naive datetime64[ns], including empty output. `data` no longer holds Python date objects; use `.dt.date` in the application if needed. Update schemas and keys: identity uses currency, full timestamp, and published type within the selected route. Do not discard fractional seconds.

Day/period routes may label the same closing `Fechamento PTAX` and `Fechamento`. The selector recognizes both; output preserves the difference. In todos, new, empty, or null types remain with diagnostics; specific selectors raise when classification is impossible. Account for label variation when joining different routes. Do not add bulletins together or use the last row as a daily average.

Dates now undergo strict validation. A single date combined with interval bounds raises; start-only fills the end with today's date in Brasília, and end-only fills the start with end minus 30 days. Previously ignored partial bounds are now honored. Currencies require three ASCII letters and normalize only case; symbols absent from the current catalogue raise before requesting quotes. Empty results, HTTP failures, and envelopes without value are distinct outcomes.

`top` controls pages without limiting output rows. Acquisition validates every page before bulletin filtering. Retain metadata: catalogue and quote coverage are separate, and without an independent count remain unknown even after an empty page. Top-level hash/size represent a resource manifest. Historical quotes use the monetary unit applicable at that date; do not label the entire series BRL or assign UTC to published clock time.

Use `BCB_PTAX_V2` and `BCB_PTAX_MOEDAS_V1` from `agrobr.contracts.bcb_ptax`, or registry keys `bcb_ptax`/`bcb_ptax_moedas`. No quote V1 contract was previously registered. See the [API](../api/bcb.en.md#ptax) and [contracts](../contracts/bcb_ptax.en.md).

## BCB Focus: annual/monthly horizons, identity, and contract 2.0

`bcb.focus` adds the keyword-only `periodicidade="anual"|"mensal"`; the default remains annual with indicator `"PIB Agropecuária"`. Output retains the previous ten columns and adds `periodicidade` and `indicador_detalhe`. Update column selections and storage keys: previously discarded annual detail separates Exportações, Importações, and Saldo, for example. The full key includes periodicity, indicator, detail, survey date, reference, and calculation base. Do not sum different bases or details.

Schema/contract move to **2.0**, parser to **2**; the newly registered contract is `bcb_focus`, constant `BCB_FOCUS_V2`. Civil dates use `datetime64[ns]`, statistics `float64`, and counts/bases `Int64`, including empty output. Use `.isna()` for missing values; published empty detail text remains distinct from null. `data_referencia` retains YYYY or MM/YYYY text: future projections are valid and do not represent realized observations.

Indicators require exact names without aliases or case normalization. Boolean/fractional counts, invalid dates, malformed JSON, missing fields, and duplicate identities now raise explicit errors. Finite inconsistent statistics remain with warnings. Order now includes textual reference, base, and annual detail tie-breakers; locally limited results within the same survey date may differ from the previous order.

`top` is page size; `max_registros` cuts only after validating the full page. Short pages no longer automatically end acquisition: the client advances by the number received until an empty page, local limit, or reconciled count. Retain `source_details.coverage`: successful acquisition does not establish the source's total. Top-level hash/size identify a manifest, with individual hashes for bodies. See the [API](../api/bcb.en.md#focus) and [contract](../contracts/bcb_focus.en.md).

## BCB SGS: historical blocks and contract 3.0

`bcb.sgs` retains its 17 aliases and four columns, while moving to schema **3.0** and parser **2**; the range becomes
`inicio`/`fim` ([§85](#85-parameter-names-one-vocabulary-across-the-api)). Series that publish `dataFim` (e.g. TR) gain the optional `data_fim` column after the four; 1.1 dropped that field. Civil dates always use `datetime64[ns]`, including on pandas 3, which previously could infer us. Values use float64, codes `Int64`, and unknown names are null. Use `.isna()` for missing values. The new registered contract is `bcb_sgs`, constant `BCB_SGS_V3`; `BCB_SGS_V2` keeps schema 2.1, and no SGS V1 contract was previously registered.

Long ranges are partitioned by calendar years, reconciled, and sorted before applying `ultimos`. Without dates, the default range uses a single reference date, today in Brasília; start-only queries fill the end, while end-only queries retain an omitted start. For more than 20 observations in series 1, provide both dates with `ultimos`, since the native latest-values route rejects 21.

Boolean codes/counts, numeric strings as codes, malformed dates, and inverted ranges now fail before network access. Invalid observations no longer silently become null. Duplicates within a body and conflicting values across blocks raise errors. Monthly/quarterly references before the starting day remain present with a warning and provenance; do not automatically interpret them as daily observations.

The official missing-values 404 now returns typed empty output with a warning, without establishing that the code exists. Failed blocks do not yield partial output. Retain metadata: coverage distinguishes acquired blocks from series completeness, which remains unknown; top hash/size identify a manifest, with individual body hashes. See the [SGS API](../api/bcb.en.md#sgs) and [contract](../contracts/bcb_sgs.en.md).

## Comtrade: World, coverage and contracts 2.0

`comtrade.comercio` moves to contract **2.1** (`comercio_bilateral`), `trade_mirror` to **2.0** and `datasets.comercio_internacional` to **3.0**. None/world/mundo/"0" now sends the explicit World aggregate. Use `partner="all"` or `"todos"` to retain the old query that omitted the parameter and returned all published partners. Do not add the aggregate to its components. In `datasets.comercio_internacional`, the filters and columns are in Portuguese: `parceiro=None` sends World ([§85](#85-parameter-names-one-vocabulary-across-the-api), [§87](#87-renamed-output-columns)).

Bilateral output retains 22 columns and adds `classificacao` and `classificacao_original`; the dataset also preserves all 24 when empty. In contract 2.1, the 3 UN estimation flags come as nullable flags (27 columns): see [section 58](#58-comtrade-un-estimation-flags). Its key is `periodo, reporter_code, partner_code, hs_code, fluxo_code, classificacao`. Mirror output grows from 18 to 24 columns, adding revision/flag per leg and two numeric country codes. Descriptive ISO may be null. Integers use `Int64`, measures `float64`, and nullable flags `boolean`. Use `.isna()` for missing values.

HS accepts 2/4/6 ASCII digits, including textual lists; odd codes and unknown arguments now fail. Monthly periods distinguish YYYYMM from YYYY, which expands a full year. Failed blocks and incompatible responses are no longer ignored. Mirror rejects `fluxo`, World, identical countries and incompatible HS revisions in the same cell.

The client compares an independent count with disjoint partitions. Default `require_complete=False` (`exigir_completo` in the dataset) returns partial output with a warning; use True when the application requires proven coverage. Preserve source details for limits and attempts. The top hash and size identify a resource manifest; each response body has its own hash. Snapshot only fills an omitted year.

Use `COMERCIO_BILATERAL_V2` and `TRADE_MIRROR_V2` from `agrobr.contracts.comtrade`, or the registry. V1 constants remain historical. The internal license is `restrito`, with a first-call warning and the UN policy's explicit exceptions; see [Licenses](../licenses.md#un-comtrade). See [API, metadata and limits](../api/comtrade.md).

## Lista Suja: CSV, publication context, and contract 2.0

`lista_suja.empregadores()` now uses CSV without an optional dependency and verifies the advertised TXT to obtain the edition. `formato="pdf"` retains PDF access through `agrobr[pdf]`; explicit formats are exclusive. Auto mode may use PDF after an eligible CSV availability failure, recording its cause in metadata. Invalid bodies and incompatible discovery raise errors.

Contract **2.0** retains the original eight columns and adds `id_registro`, `data_decisao`, `data_atualizacao`, and `data_inclusao_texto`. Years and counts now use `Int64`, including nulls and empty results; missing text changes from empty strings to nulls. Use `.isna()` for missing values. Civil dates use `datetime64[ns]`. An inclusion containing an interval and a new date retains `data_inclusao=NaT` and the entire cell in `data_inclusao_texto`; extracting one date requires an explicit application rule.

Pass `id_registro` as exact text. The key is local to the export; retain `MetaInfo.raw_content_hash` as well, since documents may occur in multiple rows and update dates may have revisions. Periodic publication and registry update dates are distinct in `source_details.publication`. Divergent TXT raises an error; missing or unavailable TXT leaves the date null with diagnostics. This API does not select historical editions.

Use `get_contract("lista_suja_empregadores")` or `LISTA_SUJA_EMPREGADORES_V2` from `agrobr.contracts.lista_suja`. Invalid filters and unknown arguments now raise `InvalidParameterError` before network access. See [columns, provenance, and limitations](../sources/lista_suja.md).

## defensivos: situation, composition, and acquisition cache

The `formulados`, `autorizacoes`, and `tecnicos` APIs retain their previous columns and move to schema **1.1**. Formulated products and authorizations add textual `situacao`; formulated and technical products add `composicao_texto`, preserving the original cell. The technical parser also fixes ingredient and group extraction for nested parentheses and multiple components. The new `composicao(tipo="formulados"|"tecnicos")` uses contract **1.0**, with one row per component position, retaining repeated ingredients. Concentrations and units are available in that table; ambiguous values remain null with diagnostics.

Pass `nr_registro` as exact text, retaining leading zeros and accents, such as `"00301"` or `"14017/Pré-Mistura"`. Empty, numeric, and boolean filters and unknown arguments now raise `InvalidParameterError` before network access. `situacao="TRUE"` compares published text without inferring registration validity; technical products do not accept this filter.

The cache now uses versioned ZIPs by family, including composition, types, and provenance. Old files are preserved, but the first call with the new parser requires a fresh acquisition. `use_cache=False` skips reading and writing. On cache hits, `MetaInfo.fetched_at` and the hash identify the original acquisition; `fetch_timestamp` identifies the current query. Current CSVs cannot reconstruct a historical registration database by date. See [API and contracts](../api/defensivos.md).

## cadastro_rural: SICAR filters and temporal context

The dataset now forwards `atualizado_apos` to SICAR and accepts `as_polars`, both keyword-only, like `return_meta`. `municipio` accepts the full name or the IBGE code ([§86](#86-municipality-full-name-or-ibge-code)) and stays in 2nd position: the first 7 positional arguments (`uf` to `criado_apos`) remain valid. The tabular source and dataset move to contract **2.1**, retaining the eleven columns (plus the optional `cod_municipio`) and `[cod_imovel]` primary key while requiring `datetime64[ns, UTC]` dates, including nulls and empty results. Unknown filters passed to the `cadastro_rural` dataset now raise before network access.

```python
from agrobr import datasets

df, meta = await datasets.cadastro_rural(
    "DF", municipio=5300108,
    atualizado_apos="2026-09-01", return_meta=True,
)
```

`criado_apos` includes the cutoff (`>=`); `atualizado_apos` uses a strict cutoff (`>`). The update filter is rejected in the twelve states whose WFS layer lacks the field. In the other layers, the projection now requests the update date omitted by the previous implementation.

Tabular collection uses GeoJSON attributes without geometry or a GeoPandas dependency. This avoids timezone-free CSV clocks that do not match the instants used by CQL. `atualizado_apos` accepts `Z`/offset and normalizes to UTC; timezone-free inputs are interpreted as UTC. Returned timestamps can be reused through `.isoformat()`. The filter supports millisecond precision: additional zeros are normalized (`.212000` becomes `.212`), while submillisecond values are rejected without rounding. Do not apply `tz_localize("UTC")` to old CSV dates without knowing their timezone; retrieve instants through the new transport when needed.

Use `get_contract("cadastro_rural")` or `SICAR_IMOVEIS_V2` from `agrobr.contracts.sicar`. `SICAR_IMOVEIS_V1` from `agrobr.contracts.datasets` remains a historical contract rather than a UTC alias.

Move `cadastro_rural` calls outside `datasets.deterministic(...)` blocks: the dataset now rejects this context with `InvalidParameterError`, including when explicit filters are supplied. Previously, the snapshot date became a creation-after filter, selecting a different set of records. An incremental query against the current registry does not recover its state at a past date. See [filters and limits](../contracts/cadastro_rural.md).

## clima: public archives and monthly contract 3.1

`datasets.clima` now tries the INMET observation API, public ZIP archives, and, for state mode, NASA POWER, in that order. Select `fonte="inmet_historico"` for archives, `fonte="inmet"` for the token API, or `fonte="nasa_power"` for a representative point in the state. An explicit source uses only that route. Archives cover 2000 onward; earlier years exclude them from fallback.

```python
df, meta = await datasets.clima(
    estacao="A001", inicio="2000-12-30", fim="2001-01-02",
    fonte="inmet_historico", return_meta=True,
)
monthly = await datasets.clima("GO", 2001, fonte="inmet_historico")
```

Monthly contract **3.1** preserves the `[mes, uf]` key and aggregation formulas. All three temperatures and precipitation now permit nulls: periods without measurements are not assigned zero. Four optional location and basis columns are added, `lat`, `lon`, `agregacao_espacial`, and `base_tempo`; the coverage columns (`estacoes_chuva`, `estacoes_chuva_parciais`, `dias`, `data_inicio` and `data_fim`) are in [section 17](#17-historical-units-cache-and-missing-measurements). NASA coordinates are retained; aggregated INMET rows have null coordinates. Daily boundaries use UTC for INMET and LST for NASA; the NASA requested point is not the mean of the state's stations.

Station mode validates daily output against `clima_estacao` **1.0** and hourly output against the new `clima_estacao_horaria` **1.0**, keyed by `[data, hora_utc, estacao]`. Station `agregacao="mensal"` now raises an error; previously it could return unaggregated hours. Conflicting state/year and station arguments, dates ignored in state mode, and unknown arguments also fail before network access. `as_polars=True` converts after contract validation.

The current contract is `agrobr.contracts.clima.CLIMA_V3`; `agrobr.contracts.datasets.CLIMA_V2` remains historical. The `fonte` column still identifies the institution (`inmet` or `nasa_power`), while `meta.selected_source` distinguishes the `inmet_historico` route. `meta.source_details` preserves resources, hashes, archive members, metadata per edition, and per-variable coverage. The API's current operating-station catalog differs from archive membership; use an explicit route to control station selection.

Within `datasets.deterministic(...)`, climate uses the context year only when `ano` is omitted. The context does not truncate observations at that date, force offline execution, or freeze archive revisions. Metadata states these limits; preserve data and hashes to reproduce the queried edition. See the [contract and coverage limits](../contracts/clima.md).

## ANA: provenance for queries with multiple pages

With `return_meta=True`, `raw_content_hash` and `raw_content_size` are now populated when ANA returns more than one page. The hash identifies the UTF-8 serialization of `{"query": ..., "resources": ...}`, using sorted keys, `ensure_ascii=False` and `(',', ':')` separators. `raw_content_size` measures that serialization. The total size of the original bodies is `source_details["resource_bytes"]`.

`source_details` exposes `hash_kind="resource_manifest_sha256"`, `manifest_encoding="canonical_json_utf8"`, `manifest_fields=["query", "resources"]`, `query` and `resources`. The logical query contains `fonte`, `recurso`, `where`, `bbox`, `max_registros` and `formato`; `resources` preserves acquisition order and contains `pagina` (starting at 1), `sha256` and `bytes`.

A single page keeps the body hash and size; zero pages keep a null hash and size zero. Data, columns, types, geometries, CRS, requests and `source_url` in the tabular/geographic APIs remain unchanged. The `MetaInfo` hash manifest does not store bodies; use raw collection to preserve them on disk.

## Contract constant imports

Update direct imports whose names changed in 2.0:

Named historical imports remain available with `DeprecationWarning`. They retain their stated schema version and are excluded from `__all__` and the active registry. Current contracts own their columns; modifying a historical contract does not modify the current one. The three aliases below follow the same warning and export policy but still point to the current contract, as they did previously.

| Module under `agrobr.contracts` | Previous name | Current name |
|---|---|---|
| `datasets` → `clima` | `CLIMA_V1` / `CLIMA_V2` | `CLIMA_V3` |
| `ibge` | `IBGE_PAM_V1` | `IBGE_PAM_V2` |
| `datasets` → `antt_pedagio` | `ANTT_PEDAGIO_FLUXO_V1` / `ANTT_PEDAGIO_FLUXO_V2` | `ANTT_PEDAGIO_FLUXO_V3` |
| `conab` → `conab_custos` | `CONAB_CUSTO_PRODUCAO_V1` / `CONAB_CUSTO_PRODUCAO_V2` | `CONAB_CUSTOS_V3` |
| `datasets` | `CREDITO_RURAL_V1_1` | `CREDITO_RURAL_V2` |
| `datasets` | `EXPORTACAO_V1` | `EXPORTACAO_V1_1` |
| `datasets` | `IMPORTACAO_V1` | `IMPORTACAO_V1_2` |
| `datasets` | `MAPBIOMAS_COBERTURA_V1` | `MAPBIOMAS_COBERTURA_V2` |
| `datasets` | `MAPBIOMAS_TRANSICAO_V1` | `MAPBIOMAS_TRANSICAO_V2` |
| `datasets` | `PRECO_ATACADO_V1` | `PRECO_ATACADO_V2` |
| `datasets` | `MAPA_PSR_APOLICES_V1` | `MAPA_PSR_APOLICES_V2` |

`preco_atacado` and `mapa_psr_apolices` move to 2.0: in wholesale prices, `categoria` is null for products
outside agrobr's table; in policies, `seguradora` joins the key. Code validating the category handles the null,
and joins on the old policy key add the insurer. `PRECO_ATACADO_V1` and `MAPA_PSR_APOLICES_V1` return the
1.0 contracts shipped in 1.1.0, with `DeprecationWarning`; they do not describe the current output.

Canonical names `IBGE_LSPA_V2`, `IBGE_CENSO_AGRO_LEGADO_V2`, and
`CONAB_SAFRA_V2` also identify the current contracts (`ibge.lspa` 2.0, `ibge.censo_agro_legado` 2.1, `conab.safras` 2.0). Their three previous `_V1` names
remain import-compatible aliases to the same current contract; they do not
provide the old schema. For dataset lookup, prefer `get_contract(name)` and
read `contract.version`.

Contracts whose version changes under 2.0's naming and dtype rules (`abate_trimestral`, `antt_pedagio_pracas`,
`condicao_lavouras`, `movimentacao_portuaria`, `oferta_demanda_global`, `posicionamento_fundos`, `comercio_internacional`
and `bcb_sgs`) get a new constant, and the old one stays importable, without a warning, with the previous schema (2 exceptions:
`POSICIONAMENTO_FUNDOS_V1` moved to 1.1, with the optional `swap_spread` and `other_spread` columns, and
`COMERCIO_BILATERAL_V1`, the old `comercio_internacional` constant, is historical and emits `DeprecationWarning`): see
[section 89](#89-output-dtypes).

## estimativa_safra: contract 3.1 and temporal selection

The dataset now uses `ESTIMATIVA_SAFRA_V3_1`, in `agrobr.contracts.estimativa_safra`. The CONAB source continues to use `CONAB_SAFRA_V2`; its alias does not become the new dataset contract. Use `get_contract("estimativa_safra")`, version `"3.1"`, to obtain the correct contract.

The previous ten columns remain, with two nullable additions, `ano_lspa` and `mes_lspa`, and two unit columns, `unidade_producao` (`mil_ton`) and `unidade_area` (`mil_ha`), the same from both sources. The key changes from `[safra, produto, uf, levantamento]` to `[fonte, safra, produto, uf, levantamento, ano_lspa, mes_lspa]`. Update database keys, deduplication and schema checks: this identity change requires a major version. Old LSPA rows do not contain enough information to reconstruct the month from the crop year alone; recover it from archived provenance or issue a new explicit query.

The 3 positional arguments `produto, safra, uf` remain valid; `return_meta` becomes keyword-only ([§88](#88-flags-and-secondary-filters-by-keyword-only)), like the new `fonte`, `levantamento`, `mes` and `as_polars`. Use `levantamento` for CONAB and `mes` for LSPA; incompatible combinations fail before network access, and unavailable explicit references are not replaced with another source or period.

```python
from agrobr import datasets

conab_mt = await datasets.estimativa_safra(
    "soja", safra="2024/25", uf="MT", levantamento=1
)
lspa_mt = await datasets.estimativa_safra(
    "soja", safra="2024/25", uf="MT", mes="01"
)
```

The second call queries January in calendar year 2025. CONAB survey 1 is a different reference; it does not represent January LSPA. Specify the same state when comparing sources, because `uf=None` returns states from CONAB and Brazil from LSPA. See the [complete contract](../contracts/estimativa_safra.md).

**Cut without observations.** When every queried source answers with no data for the cut (for example, a crop year the CONAB sheet does not publish yet, or the crop year after the most recent one in the CONAB surveys, with the LSPA not yet covering its year), the dataset returns the contract's empty frame, with `UserWarning` and the warning in `meta.validation_warnings`, instead of `SourceUnavailableError`. Code that used the exception to detect "no data" should check `df.empty`. With one source empty and the other down, it remains `SourceUnavailableError`; with the other failing on layout, `ParseError`.

## 1. ANDA accepts only the total product

Previously, a specific product was accepted even though the ANDA PDF only
contained total fertilizer deliveries:

```python
df = await anda.entregas(2025, produto="ureia")
```

Now, use the only value represented by the source:

```python
df = await anda.entregas(2025, produto="total")
```

What to do: remove product-specific filters from ANDA queries and do not treat
the published total as the volume of a fertilizer category.

## 2. Every stable column must be present

In the 1.x series, an adapter could omit a nullable `stable` column:

```python
df = pd.DataFrame(
    {
        "data": pd.to_datetime(["2026-09-04"]),
        "produto": ["soja"],
        "valor": [135.0],
        "unidade": ["BRL/sc60kg"],
        "fonte": ["cepea"],
    }
)
contracts.get_contract("preco_diario").validate(df)
```

In 2.0, the column must exist; `nullable` permits a null value, not an absent
column:

```python
df["praca"] = pd.NA
contracts.get_contract("preco_diario").validate(df)
```

What to do: when producing DataFrames for validation, create every `stable`
column; fill only columns marked `nullable` by the contract with `pd.NA`.

## 3. `conab.safras` moves to contract 2.0

Code written for 1.x could assume that survey number and publication date were
always present:

```python
surveys = df["levantamento"].astype("int64")
dates = pd.to_datetime(df["data_publicacao"])
```

Under contract 2.0, both fields are null on rows from the historical series (§27); in the `estimativa_safra` dataset
(contract 3.1), also on IBGE LSPA fallback rows:

```python
surveys = df["levantamento"].astype("Int64")
dates = pd.to_datetime(df["data_publicacao"], errors="coerce")
without_survey = df[df["levantamento"].isna()]
```

What to do: treat both fields as optional and use nullable pandas dtypes.

`area_colhida` comes out null from CONAB: the bulletin publishes a single area, which 1.x
copied into both columns. Use `area_plantada`. `data_publicacao` is now the bulletin
date; in 1.x it was the day of the query.

## 4. `bcb.credito_rural` moves to contract 2.0

In 1.1.0, the default call (`agregacao="municipio"`) did not aggregate: it returned the SICOR records, one per
month, state, programme, sub-programme, funding source, insurance type, activity and modality, with 21 columns for
operating costs (20 for investment and marketing, which have no area). The 1.x contract's `volume` column never came out:

```python
df = await bcb.credito_rural("soja", agregacao="municipio")
total = df["volume"].sum()
```

In 2.0, `"municipio"` raises `InvalidParameterError`, and state-level aggregation is the default; the program option
also groups by programme:

```python
by_state = await bcb.credito_rural("soja", agregacao="uf")
by_program = await bcb.credito_rural("soja", agregacao="programa")

columns = [
    "programa",
    "cd_programa",
    "qtd_contratos",
    "valor",
    "area_financiada",
    "fonte",
]
result = by_program[columns]
```

The `uf` and `programa` aggregations have 11 columns. Of the 21 of the default 1.1.0 call, 12 are not in them:
`regiao`, `mes_emissao`, `ano_emissao`, `cd_sub_programa`, `cd_fonte_recurso`, `fonte_recurso`, `cd_tipo_seguro`,
`tipo_seguro`, `cd_modalidade`, `modalidade`, `cd_atividade` and `atividade`. 1.1.0 code that groups by month, funding
source, modality or activity hits `KeyError` there. The 1.1.0 cut is `agregacao="registro"`:

```python
records = await bcb.credito_rural("soja", safra="2024/25", agregacao="registro")
by_month_and_source = records.groupby(["mes_emissao", "fonte_recurso"])["valor"].sum()
```

`registro` returns the 21 columns of 1.1.0, plus `agregacao` and `fonte`, in the
[bcb.credito_rural_registro](../contracts/bcb_credito_rural_registro.en.md) 1.0 contract, also in the `credito_rural`
dataset. Differences from 1.1.0:

- the crop year comes out as `"2024/25"` (section 76);
- programme and insurance-type names come from BCB's official tables (section 30), and funding source, modality and
  activity names from the whole official description. 1.1.0 had hand-typed dictionaries: funding source `0403`, in 39
  of the 277 records of MT soybean operating costs in 2024/25, came out as `Desconhecido (0403)`;
- a funding source, modality or activity code outside the table leaves the name null, instead of `Desconhecido (…)`;
- `area_financiada` comes out for every purpose, null for investment and marketing;
- no BigQuery fallback: with OData down, it raises `SourceUnavailableError`. The fallback only applies to
  `agregacao="uf"` without `programa` or `tipo_seguro`, because the Base dos Dados table carries no programme or
  insurance.

What to do: replace `agregacao="municipio"` with `"registro"` (the same cut, record by record) or with `"uf"` or
`"programa"` (aggregated), remove uses of `volume`, and update column selections. For municipal BigQuery data, use the
optional `agrobr[bigquery]` integration outside this contract.

## 5. DERAL preserves the crop season in the product

In the 1.x series, first and second crop seasons could be merged:

```python
corn = df[df["produto"] == "milho"]
```

Now, labels identified by the source remain separate:

```python
corn = df[df["produto"].isin(["milho", "milho_1", "milho_2"])]
beans = df[df["produto"].isin(["feijao", "feijao_1", "feijao_2"])]
```

What to do: include the `_1` and `_2` suffixes in filters, groupings, and keys;
an unsuffixed name remains possible when the spreadsheet does not identify the
crop season.

## 6. CEPEA rejects invalid parameters

Previously, an unknown product or market and some invalid date ranges could
produce an empty DataFrame:

```python
df = await cepea.indicador("banana", praca="marte")
if df.empty:
    print("No data")
```

Now, `indicador()`, `ultimo()`, and `pracas()` raise
`InvalidParameterError`:

```python
from agrobr import InvalidParameterError

try:
    df = await cepea.indicador("soja", inicio="2026-01-01")
except InvalidParameterError as exc:
    print(f"Invalid parameter: {exc}")
```

What to do: validate or normalize user input and handle
`InvalidParameterError` separately from empty results and source failures.

**`_moeda` is removed.** The reserved `_moeda` parameter of `indicador()` performed no conversion and is gone. Passing it, by name or as the fifth positional argument, raises `TypeError`: remove the argument. Published units remain in the `unidade` column.

## 7. Parameter errors stop the dataset fallback chain

In the 1.x series, applications could treat invalid input as a source failure:

```python
try:
    df = await datasets.preco_diario("banana")
except SourceUnavailableError:
    try_again_later()
```

In 2.0, invalid input does not try fallback sources:

```python
from agrobr import InvalidParameterError, SourceUnavailableError

try:
    df = await datasets.preco_diario("soja")
except InvalidParameterError:
    correct_input()
except SourceUnavailableError:
    try_again_later()
```

What to do: catch `InvalidParameterError` before availability errors and ask
for corrected input instead of retrying the request.

## 8. `as_polars=True` requires Polars

Previously, requesting Polars without the dependency installed could return
pandas:

```python
df = await cepea.indicador("soja", as_polars=True)
# df could be pandas.DataFrame
```

Now, install the extra before requesting that format:

```bash
pip install "agrobr[polars]>=2.0"
```

```python
df = await cepea.indicador("soja", as_polars=True)
# df is polars.DataFrame; without the extra, the call raises ImportError
```

What to do: install `agrobr[polars]` or keep `as_polars=False` and use pandas. The extra requires `polars>=0.20.3`.

## 9. Experimental modules were removed

Direct imports discovered by exploring the package no longer exist:

```python
from agrobr.export import export_csv
from agrobr.quality import quick_check
from agrobr.validators import validate_safra
```

Use maintained public APIs or native DataFrame features:

```python
from agrobr import contracts

contracts.get_contract("estimativa_safra").validate(df)
df.to_csv("crop-estimate.csv", index=False, encoding="utf-8")
```

What to do: remove uses of `quality`, `sla`, `export`, `plugins`,
`validators.semantic`, and `validators.validate_safra`. There is no direct
replacement for the experimental plugin system or semantic validator; add
application-specific rules where needed.

## 10. `agrobr.configure()` was removed

The deprecated call never changed actual fetch, cache, or fallback behavior:

```python
import agrobr

agrobr.configure(cache_enabled=False, log_level="DEBUG")
```

In 2.0, remove the call. For the supported deterministic mode, use the dataset
context manager:

```python
from agrobr import datasets

async with datasets.deterministic("2026-09-04"):
    df = await datasets.preco_diario("soja")
```

What to do: use the documented `AGROBR_*` environment variables for each
effective setting. Deterministic mode currently covers only `preco_diario`;
see the [snapshots guide](snapshots.md).

## 11. New license and fallback warnings

Previously, a query could switch sources without a dedicated signal to the
caller:

```python
df = await datasets.preco_diario("soja")
```

Now, applications can make fallback handling explicit in their policy:

```python
import warnings

from agrobr import datasets
from agrobr.exceptions import SourceFallbackWarning

warnings.filterwarnings("error", category=SourceFallbackWarning)
df = await datasets.preco_diario("soja")
```

The first CEPEA call also emits a notice about the CC BY-NC 4.0 data license.
What to do: keep the notice visible, review the [source
licenses](../licenses.md), and explicitly decide whether a fallback should
be accepted, logged, or converted to an error.

### License classifications in 2.0

Applications filtering on `MetaInfo.license` must update their policy for these changes:

| Source or source combination | Before | Now |
|---|---|---|
| IMEA — public series | `restrito` | `zona_cinza` |
| Notícias Agrícolas — publisher | `restrito` | `zona_cinza` |
| UN Comtrade | `zona_cinza` | `restrito` |
| Acervo Fundiário/INCRA | `nc` | `livre` |
| CEPEA and Notícias Agrícolas in `data_sources` | `restrito` | `nc` |

B3 remains `zona_cinza`. IMEA's non-public files still require the written authorization specified in its terms. CEPEA-origin data retains CC BY-NC 4.0, including during fallback; when both sources appear in metadata, `nc` takes precedence. Comtrade retains the redistribution exceptions in its policy. Acervo no longer emits the commercial-use-prohibition warning.

The new categories also appear in `datasets.info()["licenses"]` and source descriptions; Comtrade's `datasets.info("comercio_internacional")["license"]` and `source_details["license"]["classification"]` from new queries become `restrito`. Previously saved metadata files are not rewritten. `MetaInfo.from_dict()` recomputes `license` from the installed table but retains older classifications inside `source_details`. When reading older files, check their provenance and the [current license table](../licenses.md) before applying a filter.

`livre` permits commercial use within the stated scope but may require attribution, preservation of notices, identification of changes and ND/SA conditions. `zona_cinza` neither grants permission nor establishes a general prohibition. Reclassification does not change data acquisition or returned values.

MapBiomas Alerta remains `livre`, but its data, including API results, is under CC BY-SA 3.0 BR: preserve attribution, a license link, identification of changes and the SA conditions for adaptations. Third-party images and reports require their own terms. A filter based only on `license == "livre"` does not check these obligations; see the [MapBiomas Alerta scope](../licenses.md#mapbiomas-alerta).

## 12. Production costs preserve the published sheet

Remove `tecnologia=` from `conab.custo_producao`, `conab.custo_producao_total` and `datasets.custo_producao`. The active contract is **3.0**, with **25 columns**. Use `agrobr.contracts.conab_custos.CONAB_CUSTOS_V3` or `get_contract("custo_producao")`. Select unambiguous `planilha` and `aba`; source context, literal row labels and physical row numbers are retained.

There is no artificial primary key. Preserve repeated occurrences, subtotal/total distinctions, nullable season tokens and separate price-reference dates. Negative revenue is valid; missing quantities and unit prices are not derived from other costs. See the [complete contract](../contracts/custo_producao.md).

## 13. SICOR products and valid empty results

Replace `cafe_arabica` and `cafe_conilon` with `cafe` in rural-credit queries. SICOR does not distinguish these types. Corn and wheat now use exact matching, excluding silage and buckwheat from aggregates; previously computed totals may change.

Valid filters without records return empty DataFrames with contract columns and types. For `investimento`, the product is an investment item, not necessarily the financed crop. Do not interpret missing records as zero credit without assessing source coverage.

## 14. Types and provenance

Contracts reject fractional integers, text columns containing non-text values, and numeric/textual booleans. Use nullable types (`Int64`, `Float64`, `boolean`) to preserve missing values. `required_columns` includes all stable columns, even nullable ones, and `MetaInfo.schema_version` matches the dataset contract version.

Install `agrobr[polars]` for conversion: the extra includes `pyarrow`. Conversion errors no longer incorrectly report missing Polars when another dependency is absent.

### MetaInfo: timezone-aware UTC timestamps

The four temporal fields in `MetaInfo` (`fetched_at`, `timestamp`, `cache_expires_at`, and `fetch_timestamp`) now always use timezone-aware UTC, including subsequent assignments. Naive input values are interpreted as UTC; aware values with another offset are converted while preserving the instant. For example, `2024-06-15T23:30:00-03:00` becomes `2024-06-16T02:30:00+00:00`. Optional fields retain `None`.

`from_dict()` still accepts legacy ISO strings without an offset. Pass `datetime` objects to the constructor and assignments; a raw string raises `AttributeError`, without implicit conversion. `to_dict()` now includes `+00:00` in all populated timestamps; update string comparisons and consumer schemas. Compare or subtract these fields using `datetime.now(UTC)`, importing `UTC` from `datetime`; `utcnow()` still returns a naive datetime and cannot be used for this direct comparison. Published civil dates in dataset columns retain their own contracts.

### MetaInfo: two source identity conventions

`selected_source` and `attempted_sources` may identify the dataset adapter or the route reported by the source. `comercio_internacional`, `desmatamento`, `empregadores_lista_suja`, `unidades_conservacao`, `unidades_conservacao_federais`, `uso_do_solo`, `cultivares_registradas`, and `cultivares_protegidas` preserve internal provenance when provided, including a single attempt. Other datasets use the adapter name (`DatasetSource.name`): for a simple `cadastro_rural` query, the fields are `"sicar"` and `["sicar"]`, although the source API identifies `sicar_wfs`.

Under the base rule, more than one internal attempt or `selected_source="cache"` makes the dataset incorporate source provenance. Attempts combine previous adapters and internal routes in order, without duplicates; selection uses the reported route or the adapter if that route is absent. `from_cache` still indicates acquisition reuse and is propagated separately: a value of `True` alone does not change published identifiers. These conventions remain distinct in 2.0; check the identifiers used by each dataset when persisting or comparing provenance.

## 15. CEPEA and historical limits

Review older Paraná soybean, chilled chicken, anhydrous ethanol, and refined-sugar series: previous selection could use another table. Refined sugar now uses its dedicated page and `BRL/kg`. Orange indicators are published on the citrus page and remain available.

Live hog preserves the State column as market location; affected legacy rows are quarantined, outside normal queries and with their originals retained. For milk, `data` is the first day of the reference month, `praca` is the state or BRASIL, and the unit is `BRL/L`; the spot table is excluded. CEPEA history accumulates in cache: requesting a year does not make the source publish a year of data.

## 16. Snapshots and period parameters

Snapshots require `pyarrow` or `fastparquet` and accept only CEPEA, CONAB, and IBGE. Zero files removes the newly created directory and raises `SnapshotError`, exported from `agrobr`; the CLI exits with code 1. Partial snapshots record errors by source in the manifest. `load_from_snapshot()` rejects `pyarrow` before 14.0.1 (CVE-2023-47248) with `ImportError`, before reading the Parquet file; upgrade with `pip install "pyarrow>=14.0.1"`.

PPM rejects future years; PRODES requires an integer year within its layer range. Slaughter, quarterly milk, and GDP accept formats such as `2025-T4`, normalized to `202504`. ZARC accepts aliases and source names, but availability depends on season. Legacy annual fire archives are supported; fallback downloads may contain hundreds of MB. HTTP 400/404 when requesting a token indicates an unpublished file and returns an empty result after date validation. At download time, HTTP 404 returns empty data, but HTTP 400 raises `SourceUnavailableError`. Token/download operations are serialized within the process; historical queries fetch days sequentially.

## 17. Historical units, cache, and missing measurements

`ibge.pam` and the `producao_anual` 2.2 contract retain published numbers and add `unidade_producao`, `unidade_rendimento`, `unidade_valor_producao`, and `condicao_produto`. Group or explicitly convert before comparing periods. Oranges change from thousand fruits to tonnes in 2001; coffee changes from in-husk to processed in 2002; pre-1994 currencies are not BRL.

Monthly `clima` contract 3.1 preserves missing precipitation and temperatures. In 1.1.0, INMET state rainfall was the sum across stations; it is now the mean of the totals of stations with valid rainfall on every day of the month. A station with an incomplete month is left out and counted in `estacoes_chuva_parciais` (`estacoes_chuva` counts those included); with no complete station, as in the current month, `precip_acum_mm` is null, with a `UserWarning` and the same message in `MetaInfo.validation_warnings`. In MT, February 2026, 12 of the 34 stations with rainfall had only 6 to 25 of the 28 days: with them in the mean, the monthly value would be 254.1 mm; with the 22 complete ones only, it is 305.7 mm. Partial totals are not extrapolated: `dias`, `data_inicio` and `data_fim` give each month's daily coverage, for INMET and for the NASA POWER monthly output (schema 1.2). Daily station mode retains `clima_estacao` 1.0; hourly mode uses `clima_estacao_horaria` 1.0.

Cache migrations automatically preserve affected records in `indicadores_quarentena`, in the same DuckDB file, before removing them from the active area. This includes live hog and missing locations from migrations 5/6, CEPEA series from 7, and legacy Notícias Agrícolas refined sugar with incorrect units, handled in 8. Preservation does not require a manual backup. See the audit and recovery procedure below.

`datasets.preco_diario` raises `SourceUnavailableError` if source and cache fail; genuinely empty filtered results remain valid. Fallback warnings also cover the internal CEPEA → Notícias Agrícolas switch and are not swallowed when promoted to exceptions. Missing CONAB publication dates remain null, never the fetch date. Browsers close when leaving the page context; repeated sync calls do not reuse resources from a closed loop.

## 18. Automatic preservation of existing caches

agrobr 2.0 requires DuckDB 1.5.2 or newer. Versions 1.5.0 and 1.5.1 were excluded
because of indexed-table alteration failures, including legacy databases.
Migration 2 adds only missing columns: existing `hit_count` and `stale` do not
receive `ALTER`, preserving their values in partially migrated databases.

The first cache access upgrades its schema to version 11, even when querying an
unaffected product. The entire pending sequence is one transaction: copy to
quarantine, verify content, remove or correct active rows, and record versions.
Column additions (migrations 2, 10 and 11) are idempotent and run before that
transaction, because DuckDB cannot alter a table already modified in the same
transaction.
Write, permission, or version-recording failures raise `CacheMigrationError`
(exported from `agrobr`) and roll back the upgrade; they are not treated as an
empty cache. Resolve the cause before retrying.

The `indicadores_quarentena` table lives in the same `agrobr.duckdb` file configured
by `AGROBR_CACHE_DIR` (default: `~/.agrobr/cache`). It retains all original
fields and types, including ID, location, decimal value, unit, source,
methodology, collection timestamp, and parser version. It adds
`quarantine_migration`, `quarantined_at`, and `quarantine_reason`. Normal and
offline queries read only `indicadores`; quarantine is not a fallback and does
not expire automatically.

Rules are conservative when legacy metadata cannot distinguish correct and
incorrect observations:

- Migration 5: records with null locations, from any source.
- Migration 6: CEPEA live hog before parser 2 or Notícias Agrícolas before parser 3.
- Migration 7: CEPEA before parser 2 for `soja_parana`, `frango_resfriado`,
  `etanol_anidro`, `acucar_refinado`, `leite`, `laranja_industria`, and
  `laranja_in_natura`.
- Migration 8: CEPEA or Notícias Agrícolas refined sugar labeled `BRL/sc50kg`
  instead of `BRL/kg`, regardless of parser version. Legacy NA already used
  version 2, so that number alone cannot identify correct units.
- Migration 9: Notícias Agrícolas milk leaves the active area because its parser
  stored the closing date, whereas CEPEA uses the reference month. CEPEA wheat
  before parser 2 labeled `BRL/sc60kg` becomes `BRL/ton`; cotton from that same
  legacy parser labeled `BRL/@` becomes `cBRL/lb`. Published numbers stay the
  same; only these confirmed incorrect labels change, with the original rows
  also retained in quarantine.
- Migration 10: adds `valor_usd` and `peso_medio_kg` to `indicadores` and, when it
  already exists, to `indicadores_quarentena`; nothing is quarantined, and earlier rows
  stay null in those columns until the next collection.
- Migration 11: adds `anomalies` to the same tables and stores the weekly-average marker
  in the cache. Earlier rows receive `["media_semanal"]` for Notícias Agrícolas hydrous
  and anhydrous ethanol, which publishes only weekly averages, and an empty list
  otherwise; nothing is quarantined.

Only pending migrations run. Newly parsed observations use CEPEA 2 and Notícias
Agrícolas 3. Migration 9's label correction does not convert values between
units. Other quarantined records may require comparison against the source
before recovery.

Allow additional space for copies of affected rows, indexes, the transaction,
and WAL; requirements depend on the database, with no guaranteed fixed size
multiplier. Quarantine preserves migration history but does not protect against
physical file loss or corruption: maintain external backups as well.

### Audit and recover in a copy

Normally close every database connection and process before copying the file so
checkpointing completes. Do not copy only the main file while the database is in
use or has a pending transaction. Use explicit paths; this example assumes the
closed source file is in the current directory.

```python
import shutil
from pathlib import Path

import duckdb

source = Path("agrobr.duckdb")
copy = Path("agrobr-audit.duckdb")
if copy.exists():
    raise FileExistsError(copy)
shutil.copy2(source, copy)

with duckdb.connect(str(copy), read_only=True) as conn:
    summary = conn.execute("""
        SELECT quarantine_migration, produto, fonte, unidade, COUNT(*) AS records
        FROM indicadores_quarentena
        GROUP BY ALL ORDER BY ALL
    """).fetchall()
    print(summary)
```

To rebuild the active area **only in that audit copy**, originals can be
reinserted without changing types, keys, the ID sequence, or schema version.
Run this only after assessing the records; it does not correct their values or
labels and must not be applied to the production cache:

```python
with duckdb.connect(str(copy)) as conn:
    conn.execute("BEGIN TRANSACTION")
    try:
        conn.execute("""
            INSERT INTO indicadores BY NAME
            SELECT * EXCLUDE (quarantine_migration, quarantined_at, quarantine_reason)
            FROM indicadores_quarentena
        """)
        conn.execute("COMMIT")
    except duckdb.Error:
        conn.execute("ROLLBACK")
        raise
```

If observations with the same keys already exist, insertion fails without
overwriting them. In that case, query the originals in quarantine directly and
decide which IDs to reconstruct in the copy. Quarantine remains available after
reinsertion.

Installations that already ran older implementations of migrations 5/6/7 may
have missing records. Migration 8 preserves what still exists but does not
recreate previously deleted data. Reinstalling an earlier library version does
not restore data. Without an earlier backup, the lost count may remain unknown;
CEPEA's recent window does not guarantee full recovery.

### Cache and fallback policy

In `cepea.indicador(..., return_meta=True)` and `datasets.preco_diario`,
`selected_source="cache"` identifies local retrieval. `data_sources` lists the
actual providers of returned rows, also identified by the `fonte` column. A warm
or offline query has `attempted_sources=["cache"]` and is not a new fallback
attempt.

When collection fails and cache replaces it, `attempted_sources` retains those
attempts and ends with `"cache"`. The dataset emits `SourceFallbackWarning`,
including this internal path, and honors promotion to an exception. The direct
CEPEA API emits `StaleDataWarning` for cache used after failure. These warnings
are not provider authorization policies: to reject previously fetched Notícias
Agrícolas records, inspect `data_sources` or the `fonte` column, even offline.

Unreadable database: in 1.1.0, with a damaged `agrobr.duckdb` (power outage, full disk or antivirus in the middle
of a write), `cepea.indicador` went on without cache on every call, with no `UserWarning` (only a `logger.warning`),
until someone deleted the file.
In 2.0, a database that DuckDB flags as unreadable (incomplete read, checksum or invalid file) is moved to
`agrobr.duckdb.corrompido-<YYYYMMDDHHMM>`, with the WAL, agrobr warns once with both paths (`UserWarning`), and
the next query creates a new database. The migration quarantine goes with the moved file. A file in use by
another process and a full disk move nothing. See [what agrobr writes to disk](../advanced/disco.md).

## 19. Dependencies, failures, and structured output

Security floors also move to HTTPX 0.28.1, httpcore 1.0.9, lxml 6.1.0, requests 2.33.0, and GeoPandas 1.1.4 in the geo extra, and `certifi` 2026.7.22 (TLS certificate authorities) becomes a direct core dependency. See the rationale in the [dependency policy](dependencies.md).

Upgrade dependencies with the package: minimum pandas 2.2.2, Typer 0.26.0, pdfplumber 0.11.10 for PDF, pyogrio 0.8.0 for geo, and polars 0.20.3 for polars (the datasets' `as_polars=True` uses the `String` type, which Polars only has from that version on). These floors exclude combinations that failed during import, CLI execution, or numeric PDF extraction. SIDRA now uses asynchronous HTTP directly; sidrapy is no longer a dependency.

Chunked NASA queries fail entirely when any chunk cannot be fetched. Invalid aggregation is rejected before network access. A start after today (Brasília calendar) in `clima_ponto` and a year after the current one in `clima_uf` also raise `InvalidParameterError` before network access; in 1.1.0, the response with no day became `ParseError`. Missing measurements must not be interpreted as complete coverage.

CLI JSON/CSV occupies stdout only; progress and errors use stderr. Empty results produce `[]` or a CSV header. Empty `snapshot list --formato json` produces `[]`. `doctor` exposes cache errors, honors health configurations, and exits with code 1 for local errors or an outage of any checked source. This diagnoses collection health and does not, by itself, mean the installation is defective. Warnings such as missing credentials are not classified as source outages.

Empty results use the requested variant's contract, including PRODES/DETER, futures, rural insurance, and state-level MapBiomas. Numeric contracts reject booleans, complex numbers, and infinities; numeric dates are not silently interpreted as 1970 timestamps.

New snapshots are published after the manifest is complete and verified using SHA-256 on read. Cancelling creation releases the name for retry. Legacy snapshots remain readable; see [integrity and abrupt interruption limitations](snapshots.md).

## 20. LSPA preserves month, variable, and unit

The `lspa` contract moves to 2.0. Use key `[ano, mes, produto, localidade, variavel]`; `mes` is always present and annual queries preserve published months. `variavel_cod` identifies the SIDRA variable and `unidade` specifies each row's measure. Do not treat a month label as the product or variable. The `estimativa_safra` dataset continues selecting the latest month and consolidating sub-crops in its own units.

## 21. Legacy Census uses official geography and headers

The `censo_agropecuario_legado` contract moves to 2.1. `nivel="uf"` returns actual state totals; without `uf`, it queries all 27 directories. For national activity categories, use `nivel="brasil"` without a state filter. Include `uf` in municipal keys to distinguish identical names; absent historical codes remain null.

Update `categoria` and `variavel` filters according to the [contract](../contracts/censo_agropecuario_legado.md): they reflect actual labels, including the header hierarchy. Read `unidade` per row; counts, area, production, and money are distinct measures, with the official cell's numeric scale applied and decimal precision preserved.

In 1.1.0, `censo_agro_legado("maquinas", uf="PA")` silently returned Table 6 (personnel) as tractors: IBGE's `Para/Tab_7Mn.zip` is a copy of `Tab_6Mn.zip`, and the personnel of Baixo Amazonas (124,592 people) came out as `total_tratores`. Discard Pará machinery series stored with 1.x. In 2.0, `uf="PA"` raises `SourceUnavailableError`, and the query without `uf` returns the other 26 states, with the warning in `MetaInfo` and a `UserWarning`: Pará's municipal machinery table is not on the FTP.

## 22. Coffee, sugarcane and the CONAB catalog

Remove `cana_industria` from historical-series queries: industrial tables were not interpreted and the product is no longer advertised in version 2.0. This API has no industrial replacement.

For coffee, contract `serie_historica_safra` 1.1 adds optional columns `area_em_producao_mil_ha` and `area_formacao_mil_ha`. Total planted area is their sum when both exist; yield still refers to producing area. Production and yield are correctly converted from thousand 60 kg bags and bags/ha to thousand tonnes and kg/ha. Review coffee series persisted using the previous parser. A zero published in the spreadsheet now comes out as `0.0`: it used to become null, and a state with no production in that season had no row. Series persisted with the previous parser have fewer rows and nulls where the source publishes zero. A season that is zero in every state (not surveyed) is still left out.

For coffee (`cafe`, `cafe_arabica`, `cafe_conilon`), ES, RJ and SP came out with `regiao="NORTE"`, because the Minas Gerais sub-region "Norte, Jequitinhonha e Mucuri" was read as a macro-region. They now come out as `SUDESTE`, and the region only changes on the exact macro-region label. Redo regional aggregations built from coffee series persisted with 1.x.

For `cana`, the Área sheet of the agricultural spreadsheet is harvested area (official title "Série Histórica de Área Colhida"). It moves to the new optional column `area_colhida_mil_ha` of contract 1.1, and `area_plantada_mil_ha` stays null for this product. In 1.x, this value came out as planted area: switch the column where you read sugarcane `area_plantada_mil_ha`. For total area, use `cana_area_total`.

## 23. Prices and production values follow each row's unit

In `preco_diario`, `valor` uses `unidade`: cotton in `cBRL/lb` requires division by 100 for display in BRL/lb. In PEVS, production value uses currency, whereas production quantity uses the product's physical unit. Preserve the historical units supplied by SIDRA when combining periods.

## 24. ANTT preserves collection modality and frequency

The active `antt_pedagio_fluxo` contract is **3.0**, with 13 columns. Use `ANTT_PEDAGIO_FLUXO_V3` from `agrobr.contracts.antt_pedagio`, or `get_contract("antt_pedagio_fluxo")`. The key is `data`, `concessionaria`, `praca`, `sentido`, `tipo_veiculo`, `categoria_eixo`, `tipo_cobranca`, `frequencia`. Keep manual and automatic collections separate.

Only explicit textual axle counts populate `n_eixos`; standalone numbers and tariff categories stay null. Literal category labels and spaces are preserved. Explicit commercial ranges can satisfy the heavy-vehicle filter without inventing an exact count.

Choose `frequencia="mensal"` or `"diaria"`; neither silently replaces the other. Date selection still validates the entire downloaded annual CSV. CSV/spool/transfer budgets are 512 MiB / 1 GiB / 3 GiB. The default parser limit is 500,000 selected rows; exceeding a budget raises an error.

The plaza registry contract moves to 2.0 (`ANTT_PEDAGIO_PRACAS_V2`), with typed `km_m`, `ano_do_pnv_snv` and `data_da_inativacao` ([§89](#89-output-dtypes)); the traffic range is `inicio`/`fim`. Traffic metadata preserves all resources, hashes, selected frequency, EOF statistics and coverage. `raw_content_size` is the size of the manifest, the same object as `raw_content_hash`, which identifies the query and acquisition; catalogue and CSV bytes received across attempts go in `source_details["received_bytes"]` ([§57](#57-metainfo-source_url-manifests-and-cache)). See the [API reference](../api/antt_pedagio.md).

With `uf`/`rodovia`, a plaza without a unique registry link is dropped from the result with a warning (previously the whole query failed). A CSV without a header now raises `ParseError`. The legacy functions `parser.parse_trafego`, `parse_trafego_v1`, `parse_trafego_v2`, `join_fluxo_pracas`, `heavy_vehicle_mask`, `client.download_csv` and the constants `CATEGORIA_MAP`, `EIXOS_TIPO_MAP`, `COLUNAS_FLUXO`, `COLUNAS_V2` and `ANO_INICIO_V2` were removed: use `fluxo_pedagio()` or, for a local file, `parser.parse_trafego_file()`.


**Flow text and the `municipal` column.** `sentido` comes in upper case: `Crescente` and `CRESCENTE` become `CRESCENTE` (likewise `DECRESCENTE`), and `fluxo_pedagio` labels come without the outer spaces ANTT publishes. Code that filtered or grouped by the published text changes the value; volumes do not change (in the official 2023 monthly CSV, the total, the per-direction volume and the enriched volume match). Concessionaire and plaza names from the registry also come without outer spaces, and the link to state, highway and municipality is kept. In `pracas_pedagio`, the `municipal` column stays in this version as a copy of `municipio`, but it emits `FutureWarning` and a `MetaInfo` warning and goes away in the next major version: use `municipio`.

## 25. INCRA: WFS 2.0, 22 columns and the empty-date placeholder

`incra.quilombolas()` and `incra.quilombolas_geo()` now read the layer through WFS 2.0.0/JSON and return **22 columns** (contract `incra_quilombolas` 2.0): the previous 10, in the same order, followed by `feature_id`, `regional`, `processo`, `data_publicacao_2`, `responsavel`, `esfera`, `data_cadastro`, `codigo_sipra`, `descricao`, `data_decreto`, `tipo_levantamento` and `escala`. Select columns by name.

- `data_publicacao`, `data_titulo`, `data_publicacao_2` and `data_decreto` are `datetime64[ns]`, and `data_cadastro` is `datetime64[ns, UTC]`. The source's `0001-01-01` placeholder ("no date") becomes `NaT` without a warning; any other date outside 1900–2099 (a typo in the source) becomes `NaT`, with a `UserWarning` and a warning in `meta.validation_warnings`. `incra.vinculos_quilombolas` follows the same rule in the `perimetro_*` columns.
- `codigo` and `familias` are `Int64`; `area_ha` is `float64` in published hectares; the other texts use the pandas default dtype ([§89](#89-output-dtypes)).
- `bbox` uses EPSG:4326, and `quilombolas_geo()` returns coordinates in that CRS **without topology repair** (the 1.x `make_valid` is gone).
- Invalid parameters raise `InvalidParameterError` before any request; truncation by `max_registros` emits a `UserWarning`.

New functions: `incra.andamento_quilombola()` (the "Andamento dos processos" PDF table) and `incra.vinculos_quilombolas()` (NUP links), both with `agrobr[pdf]`. See the [source page](../sources/incra.md).

## 26. PPM: `galinhas` replaces `galinhas_poedeiras`

Category 32793 of table 3939 is "Galináceos - galinhas" and, per the PPM technical notes, includes laying and breeder hens. Use `ibge.ppm("galinhas")` or `datasets.pecuaria_municipal("galinhas")`. `galinhas_poedeiras` is still accepted as an alias, with a `FutureWarning`, and the numbers do not change; the `especie` column now reads `"galinhas"` in both cases. Filters on `especie == "galinhas_poedeiras"` must switch to `"galinhas"`.

## 27. CONAB: past crop years come from the most recent revision

`conab.safras(produto, safra=X)` without `levantamento` now reads X from the most recent publication carrying it. For the crop year before the current one, that is the latest survey of the next crop year, which CONAB revises (sesame MT 2024/25: 401.2 thousand ha in the 12th survey of 2024/25, 695 in the 12th survey of 2025/26). Previously the crop year's own latest survey was used. Two or more crop years behind the most recent edition, the revision only exists in the historical series, and the figure now comes from it (soybean 2022/23: 159,154.3 thousand t, against 154,609.5 in the 12th survey of 2023/24); on those rows, `levantamento` and `data_publicacao` are null. A crop year before the start of the product's series (soybean: 1976/77) comes back empty, as in `conab.serie_historica`; previously it raised `SourceUnavailableError`. `datasets.estimativa_safra`, `datasets.producao_anual` (CONAB route) and `conab.brasil_total(safra=X)` follow the same rule. For an older crop year, `brasil_total` reads about 35 series, one per product row, and recomputes the subtotals and BRASIL. `conab.balanco(safra=X)` now also reads the most recent publication whose Suprimento sheet carries X (wheat 2024/25: 7,873.4 thousand t in Sep 2026, against 7,536.1 in Sep 2025); previously the crop year's own edition was used. `conab.balanco()` without `produto` now includes soybean, which comes from its own "Suprimento - Soja" sheet and used to be left out. For the original figure, pass `levantamento`, which now also exists in `balanco` and `brasil_total`: agrobr warns when a more recent publication exists. In these cases `levantamento` and `data_publicacao` describe the publication used, and `MetaInfo.source_details["publicacao"]` identifies it.

In the CONAB fallback of `datasets.producao_anual`, a call without `ano` now returns the previous calendar year (an already harvested crop season), no longer the estimate of the season in progress labeled with the following year.

## 28. ComexStat: aliases cover the whole product and each period's codes

The aliases of `comexstat.exportacao`/`importacao` (and the products of `datasets.exportacao`/`importacao`) now sum every NCM code of the product, with the codes in force in each year. Before, several pointed to a single code, some already discontinued, and came back empty or partial without warning. The full table (included / not included) is in the [ComexStat API](../api/comexstat.md).

| Alias | Before | Now |
|-------|--------|-----|
| `soja`, `soja_grao` | `12019000` (empty until 2011) | `12019000` + `12010090` |
| `soja_semeadura` | `12011000` (empty until 2011) | `12011000` + `12010010` |
| `farelo_soja` | `23040010` | `2304` (includes `23040090`, 78 % of 2025 exports) |
| `milho` | `10059010` | `1005` |
| `arroz` | `10063021` | `1006` |
| `trigo` | `10019900` (empty until 2011) | `1001` |
| `algodao` | `520100` | `5201` + `5203` |
| `cafe` | `09011110` | `09011` + `09012` |
| `cafe_arabica`, `cafe_conilon` | `09011110`, `09011190` | removed: `InvalidParameterError` (the NCM does not separate species) |
| `acucar` | `17011400` (empty until 2011) | `1701` |
| `etanol` | `22071000` (exports empty or residual since 2012) | `2207` |
| `carne_bovina` | `02023000` | `0201` + `0202` |
| `carne_frango` | `02071400` (since 2024, only the old code's residue) | `02071` |
| `carne_suina` | `02032900` | `0203` |
| `ureia` | `31021010` | `310210` |
| `kcl` | `31042090` | `310420` |
| `dap` | `31053000` (empty until 2018) | `310530` |
| `npk` | `3105` (included MAP and DAP) | `31052000` |
| `ssp`, `tsp` | empty before 2017 | `InvalidParameterError` before 2017 |
| `defensivos`, `agrotoxicos` | `3808` | `3808` without the 27 codes put up exclusively for household sanitation use |

To reproduce an old selection, pass the NCM code or prefix as `produto` (e.g. `comexstat.exportacao("10059010")` for grain maize, `"3105"` for the whole compound-fertiliser heading). The datasets consolidate each product's codes and no longer return the `ncm` column. In `MetaInfo.source_details["query"]`, `ncm_prefixo` (text) became `ncm_prefixos` (list), plus `ncm_excluidos`.

## 29. Land Registry: public and private SIGEF, SNCI per state

`acervo_fundiario.sigef` and `snci` (and the `_geo` variants) no longer refuse states through a fixed list: a state without a file on the server raises `SourceUnavailableError` (HTTP 404). The constants `acervo_fundiario.models.SIGEF_UFS_DISPONIVEIS` and `SNCI_UFS_DISPONIVEIS` were removed. SNCI is published per state, and the list changes over time: on 2026-10-01, AC, DF and RR had no file (RR's existed on 2026-09-22). With `bbox`, `sigef` and `snci` (table) filter by geometry with any pyogrio and GDAL version: in 1.1.0, with pyogrio before 0.10 (GDAL 3.8), they came back empty, with no warning.

SIGEF no longer reads `Sigef Brasil_{UF}.zip`; it reads the 2 files INCRA publishes it in, `Sigef Público_{UF}.zip` and `Sigef Privado_{UF}.zip`, which partition the state. What changes for code calling `sigef(uf)` as in 1.1.0:

- the same parcels come with the new `natureza` column (`"publico"` or `"privado"`), public first and then private, each part in file order: code that relied on the single file's order needs to sort (by `codigo_parcela`, for example);
- `natureza="publico"` or `"privado"` downloads only that file;
- `MetaInfo.source_url` is the encoded URL of the first file read (`Sigef%20P%C3%BAblico_GO.zip`); `source_details` becomes `source_details["arquivos"]["publico"|"privado"]` (with `url`, `etag`, `last_modified`, `sha256`…); `attempted_sources` lists the files read; `schema_version` is `1.1`;
- the cache moves to `acervo_fundiario/sigef_publico/` and `sigef_privado/`; the 1.1.0 `acervo_fundiario/sigef/` folder is no longer read and can be deleted.

## 30. Rural credit (SICOR): program and insurance type from the official BCB table

`bcb.credito_rural` and the `credito_rural` dataset publish the program name and filter with `programa=`/`tipo_seguro=`
using the names from the BCB domain tables (`Programa.csv` and `TipoGarantiaEmpreendimento.csv`): program = part of the
official description before the first " - "; insurance type = official description. Filters are case-insensitive, so
`programa="Pronaf"` and `programa="Pronamp"` still work. Published values and what each filter selects change:

| Code | Before | Now |
|---|---|---|
| program `0001` | Pronaf | PRONAF |
| program `0050` | Pronamp | PRONAMP |
| program `0070` | Funcafe | FUNCAFÉ (PROGRAMA DE DEFESA DA ECONOMIA CAFEEIRA) |
| program `0100` | Moderfrota | PRLC-BA (PROG RECUP LAVOURA CACAUEIRA BAIANA) ENCERRADO — Moderfrota is `0154` |
| program `0110` | Inovagro | PRODECER III — Inovagro is `0162` |
| program `0152` | RenovAgro | PROIRRIGA — RenovAgro is `0222` |
| program `0156` | Moderagro/Moderfrota | ABC + Programa para a Adaptação à Mudança do Clima e Baixa Emissão de Carbono |
| program `0200` | Proirriga | PROCERA |
| program `0999` | Sem programa especifico | FINANCIAMENTO SEM VÍNCULO A PROGRAMA ESPECÍFICO |
| program `0153`, `0162`, `0222` and other official codes | Desconhecido (code) | name from the official table (MODERAGRO, INOVAGRO, RenovAgro…) |
| insurance type `1` | Proagro | Proagro tradicional |
| insurance type `2` | Sem seguro | Proagro mais |
| insurance type `3` | Seguro privado | Outro seguro |
| insurance type `9` | Nao se aplica | Sem adesão a seguro |
| insurance type `0` | Desconhecido (0) | Não se aplica |

Codes `0002`, `0102`, `0104`, `0106`, `0108`, `0112`, `0114` and `0150` do not exist in the official table and were
dropped. Filtering `tipo_seguro="Sem seguro"` used to return contracts with Proagro Mais: use `"Sem adesão a seguro"`.
The full table is in [sources/BCB](../sources/bcb.en.md#sicor-dimensions).

## 31. SGS: `ipa_agricola` instead of `ipa_agropecuario`

SGS series 7460 is the IPA-DI by origin for **agricultural products** (without livestock), as its official name
says. The alias becomes `ipa_agricola`; `ipa_agropecuario` is still accepted, emits a `FutureWarning` and
returns `nome_serie="ipa_agricola"`. Code filtering `nome_serie == "ipa_agropecuario"` after the query should
switch to `"ipa_agricola"`. The numeric code `7460` does not change.

## 32. Embrapa Solos: 85 columns and laboratory values as text

`embrapa_solos.perfis()` now reads WFS 2.0 as JSON and returns **85 columns** (`embrapa_solos_perfis` 3.0 contract): the 19 from 1.x, in the same order, followed by the other published attributes, `uf_original` and `feature_id`. Each row is a horizon or layer (34,464 in the layer); `codigo_pon` identifies the sampling point. `mapa_solos()` gains `ordem3`, `subordem3`, `gdegrupo3` and `feature_id` (19 columns).

- The 9 laboratory values (`areia_total`, `silte`, `argila`, `ph_h2o`, `carbono_organico`, `ctc`, `saturacao_bases`, `aluminio`, `fosforo`) are no longer `float`: they are the published text, including `NULL`. 1.x converted them with `errors="coerce"`, silently turning `<1`, `<0.5`, `0,19` and the marks in `fosforo` into nulls. Convert in your application, e.g. `pd.to_numeric(df["argila"].replace("NULL", pd.NA))`; for `fosforo`, handle censored values and the comma first.
- `ano` is `Int64` and `data_colet` is `datetime64[ns]`; `NULL` becomes missing in those 2 columns, and a year or date outside the format, or an impossible date (`2024-02-30`), raises `ParseError`. Under the [date rule](normalizacao.en.md#source-dates), a well-formed `data_colet` with a year outside 1900–2099 becomes `NaT`, with a `UserWarning` and a warning in `meta.validation_warnings` (on 2026-10-07, 73 of the layer's 34,464 records, such as `0982-11-01` and `1892-07-14`).
- `max_registros` (default 50,000; 5,000 profiles and 3,000 polygons in the `_geo` functions) caps the prefix read in `fid` order. `uf` and `ordem` filter that prefix locally, and a cut that leaves the selection partial raises a `UserWarning`; `max_registros=None` scans the whole layer. `tamanho_pagina` sets the page size.
- **Cost:** each page waits 2 s (agrobr's pace for the source), and `uf` and `ordem` do not reduce the pages, because they filter
  after the read. `perfis()` reads pages of 250 (the whole layer is ~138 pages, over 4 min), and `perfis_geo()` pages of 100 up
  to 5,000 rows (50 pages, over 1.5 min). Examples: 310 s for `perfis(uf="GO")` and 113 s for
  `perfis_geo(uf="DF")`. Raise `tamanho_pagina` (up to 1,000; 100 in the `_geo` functions) or
  lower `max_registros`.
- `bbox` and the `_geo` functions use EPSG:4326. See the [source page](../sources/embrapa_solos.en.md).

## 33. Rural credit (SICOR): `produto` and `finalidade` as requested, validated crop year

`bcb.credito_rural` and the `credito_rural` dataset publish in `produto` the requested key, unaccented and lower-case,
like `producao_anual` and `estimativa_safra`; the filter still uses the official SICOR spelling. `finalidade` is also
lower-case through the BigQuery fallback, which used to publish `CUSTEIO`. If you filtered on the old spelling, switch:

| Requested | Before | Now |
|---|---|---|
| `algodao` or `algodão` | algodão | algodao |
| `cafe` or `café` | café | cafe |
| `cana` | cana-de-açucar | cana |
| `feijao` or `feijão` | feijão | feijao |
| `mandioca` | mandioca (aipim, macaxeira) | mandioca |
| investment item, e.g. `CANA-DE-AÇUCAR` | cana-de-açucar | cana-de-acucar |

Soybean, corn, rice, wheat and sorghum are unchanged. The crop year accepts only `YYYY`, `YYYY/YY` and `YYYY/YYYY` with
consecutive years; `"2023/25"`, `"2024/2023"`, `"23/24"` and free text raise `InvalidParameterError` before any request
(they used to be silently read as another crop year, return empty, or raise `ValueError` in the client).

## 34. FUNAI: 19 columns and the update date read day first

`funai.terras_indigenas()` now reads WFS 2.0 as JSON and returns **19 columns** (`funai.terras_indigenas` 2.0 contract): the 9 from 1.x, in the same order, followed by `feature_id`, `gid`, `reestudo_ti`, `cr`, `faixa_fronteira`, `undadm_codigo`, `undadm_nome`, `undadm_sigla`, `dominio_uniao` and `epsg`.

- `data_atualizacao` stays `datetime64[ns]`, now read day first, as FUNAI publishes it (`dd/mm/yyyy`), and null for some lands. 1.x converted it with `pd.to_datetime(errors="coerce")` without `dayfirst`, swapping day and month whenever the day was 12 or less. An unreadable date becomes `NaT`, with a warning in `meta.validation_warnings`.
- `uf` is the published text: lands in more than one state come as "AM, RR", and the `uf` filter matches any of them.
- `max_registros` (default 10,000; 1,000 in the `_geo` functions) and `tamanho_pagina` are new parameters. `uf` and `fase` filter the prefix read locally, and a cut that leaves the selection partial raises a `UserWarning`. Geo output and `bbox` use EPSG:4326. See the [source page](../sources/funai.en.md).
- **Cost:** each page waits 2 s (agrobr's pace for the source), and `uf` and `fase` do not reduce the pages. `terras_indigenas()` reads pages of 250, and `terras_indigenas_geo()` pages of 10 TIs, up to 1,000 (up to 100 pages). Example: 141 s for `terras_indigenas_geo(uf="AC")`. Raise `tamanho_pagina` (up to 1,000; 100 in the `_geo` functions).

## 35. ZARC: nine crops of the 2017/2018 to 2023/2024 seasons

The annual tables from 2017/2018 to 2023/2024 publish nine labels the catalogue did not recognise: `zarc.zoneamento` and
the `zoneamento_agricola` dataset rejected the `cultura=` filter before any request and, without a filter, published an
improvised name. `zarc.culturas()` now has 107 crops and the filter accepts the official label or the key:

| Official label | Before (no filter) | Now |
|---|---|---|
| Milho | milho | milho |
| Arroz Irrigado | arroz_irrigado | arroz_irrigado |
| Feijão 1ª Safra | feijao_1a_safra | feijao_1 |
| Trigo Irrigado | trigo_irrigado | trigo_irrigado |
| Mamona Semi-árido Sequeiro | mamona_semi-arido_sequeiro | mamona_semiarido_sequeiro |
| Cevada Grãos Irrigada | cevada_graos_irrigada | cevada_graos_irrigada |
| Cevada Grãos Sequeiro | cevada_graos_sequeiro | cevada_graos_sequeiro |
| Aveia Sequeiro | aveia_sequeiro | aveia_sequeiro |
| Aveia Irrigada | aveia_irrigada | aveia_irrigada |

If you filtered `feijao_1a_safra` or `mamona_semi-arido_sequeiro` after querying, switch to the new keys. A catalogue
crop missing from the requested season still raises `InvalidParameterError`, now listing the seasons in which it
appears (e.g. `milho` from 2017/2018 to 2023/2024; `milho_1` from 2024/2025 to 2026/2027). For the 11 crop labels ZARC
renamed in the 2024/2025 season, the error also points to the equivalent key in the queried table; the table of pairs
and the key for joining seasons (`cultura_codigo` and `manejo`) are on the [source page](../sources/zarc.md). In 2.0, the
filter is called `produto` ([§85](#85-parameter-names-one-vocabulary-across-the-api)); the output column stays `cultura`.

## 36. IBAMA: current file, edition in `MetaInfo` and `_geo` `bbox` by polygon

`ibama.embargos()` and `ibama.embargos_geo()` now read the current CSV of the "Fiscalização - termo de embargo" dataset
(daily update). 1.x read a ZIP the source stopped updating on May 3, 2026; results saved from 1.x are a snapshot of that
date (on September 23, 2026 they lacked 2,454 terms, 525 disembargos and 237 cancellations).

- The download goes from ~47 MB (ZIP) to ~208 MB (uncompressed CSV; the source publishes no ZIP).
- `meta.source_details["ultima_atualizacao_relatorio"]` carries the edition read (Brasília time).
- `embargos_geo(bbox=...)` filters by polygon intersection with the box; before, it filtered by the term's reference
  point and returned polygons outside the box. `embargos(bbox=...)` still uses the point. See the [source page](../sources/ibama.md).

## 37. Agricultural Census: `Total` row published

`ibge.censo_agro()` and `datasets.censo_agropecuario` now publish the `categoria = "Total"` row when the source
publishes it. 1.x dropped that row in every theme. Establishment counts do not add up across categories, so the
official total could not be rebuilt: irrigation, Brasília 2017, 2,726 establishments in the Total and 3,224 adding up
the methods.

- To add up categories, filter `categoria != "Total"`; for the official total, use the `Total` row.
- `censo_agropecuario` contract 1.2 and parser 3 of the current SIDRA census. See the
  [contract](../contracts/censo_agropecuario.md).

## 38. B3: contract month on options and the `vencimento` filter

`b3.posicoes_abertas_historico(..., vencimento="V26")` and `datasets.futuros_agricolas(..., tipo="oi_historico", vencimento=...)` now
return the future **and the options** of the contract month. 1.x compared the raw code and returned only the future:
anyone adding up `posicoes_abertas` by expiry now also adds up the options.

- To keep the 1.x behavior, pass `tipo="futuro"` to `b3.posicoes_abertas_historico`; in `datasets.futuros_agricolas`,
  where `tipo` selects the query, filter the result's `tipo` column (`df[df["tipo"] == "futuro"]`). An option's published
  code (MYOA, e.g. `VVJK`) matches only that series.
- Options' `vencimento_mes` and `vencimento_ano` are no longer null. They are the **contract** month and year, not the
  expiration's, which can fall in the previous month: arabica and conillon coffee options and soybean cross and FOB options
  (e.g. `ICFH27C035000` expires on 2027-02-12). Contract `b3.posicoes_abertas` 1.1, with both fields non-null.
- A ticker or `XprtnCd` outside the pattern raises `ParseError` (in 1.x the value stayed null). A `vencimento` filter in
  any other format raises `InvalidParameterError` before the network. See the [source page](../sources/b3.md).

## 39. ABIOVE: latest edition and `edicao`

`abiove.exportacao(ano, mes=...)` and the ABIOVE fallback of `datasets.exportacao` now read the latest edition of the
workbook that publishes `ano`. In 1.x, `ano` and `mes` chose the file (`exp_{ano}{mes}.xlsx`, or the year's own last
edition): a past year's number stayed stuck in the December edition, without ABIOVE's revisions (in September 2026, 19 of
the 96 cells of 2025; meal, Dec/2025: 2,020,365.023 t in 1.x and 1,990,304.323 t in the current edition), and a month
without its own edition (e.g. `ano=2026, mes=1`) raised `SourceUnavailableError`.

- `mes` only filters the data month. Outside 1-12 it raises `InvalidParameterError` before the network; a month not yet
  published returns an empty DataFrame.
- For the original number, pass `edicao="YYYY-MM"` (e.g. `edicao="2025-12"`); editions of `ano` and `ano + 1` are valid.
- `MetaInfo.source_details["edicao"]` carries the file and month of the edition read, and `raw_content_hash` the
  workbook SHA-256. See the [API](../api/abiove.md).
- `datasets.exportacao` does not accept `mes`. In 1.1.0, the argument reached the ABIOVE fallback; in 2.0, it raises
  `TypeError` (section 78). For a month, use `abiove.exportacao(ano, mes=...)` or filter the result's `mes` column.
- `abiove.exportacao(ano, produto="total")` raises `InvalidParameterError`: for the total of all products, use
  `agregacao="mensal"` (with or without `produto="total"`). In the monthly sum with `produto="grao"` (or another), the
  `produto` column carries the filtered product; in 1.1.0, it carried `"total"`.

## 40. Comtrade: aliases with the same meaning as ComexStat

The aliases of `comtrade.comercio`, `comtrade.trade_mirror` and `datasets.comercio_internacional` now sum only the product's
HS codes, with the same meaning as in ComexStat (section 28). The full table (included / not included) is in the
[Comtrade API](../api/comtrade.md).

| Alias | Before | Now |
|-------|--------|-----|
| `suco_laranja` | `2009` (juices of all fruits: +US$ 357 million, +11.4%, in 2025) | `200911`, `200912`, `200919` |
| `carne_frango` | `0207` (all poultry: +US$ 212 million, +2.5%, in 2025) | `020711` to `020714`; before 1996, `InvalidParameterError`, and in 1996 for Brazil, which reported in HS 1992 (H0) |
| `cafe` | `0901` (with husks and substitutes) | `090111`, `090112`, `090121`, `090122` |
| `algodao` | `5201` | `5201` + `5203` |
| `soja` | `1201` (with seed) | `120190` (since 2012) + `120100` (up to 2011) |
| `complexo_soja` | `1201` + `1507` + `2304` | `soja` + `1507` + `2304` |

To reproduce an old selection, pass the HS code as `produto` (e.g. `comtrade.comercio("2009")`). `comtrade.produtos()`
returns the new map. `celulose` (`4703`) and `tabaco` (`2401`) do not change; the docs now say what is left out.

## 41. PSR: `seguradora` in the policy key and records published twice

`mapa_psr.apolices` and `datasets.seguro_rural(tipo="apolices")` now use the `mapa_psr_apolices` 2.0 contract, with
`seguradora` in the primary key (non-null). In 1.1.0 the dataset raised `ContractViolationError` for 2007, 2008, 2009, 2011 and
2012, because MAPA publishes the same policy number under two insurers; these years are now delivered in full. Anyone joining
policies on the old key must add `seguradora`.

A record published twice and identical in every column (1 case, in 2009) is returned once, with a warning and the count in
`source_details["duplicatas_colapsadas"]`. Like the dataset, the source now raises `ContractViolationError` when the key repeats
with different values.

## 42. PSR: geocode published as "-" is now null

In `mapa_psr.apolices`, `mapa_psr.sinistros` and `datasets.seguro_rural`, `cd_ibge` is null when MAPA publishes "-" instead
of the geocode (1,516 policies between 2006 and 2025) or leaves the cell empty. In 1.1.0 these came out as the strings "-" and
"". Filters and joins on `cd_ibge` must handle the null.

For the whole municipality, pass the code or the name in `municipio`: the filter compares the code, and policies labelled
with a district name come in ([§86](#86-municipality-full-name-or-ibge-code)). In `mapa_psr.apolices` and `mapa_psr.sinistros`, `as_polars` and
`return_meta` are keyword-only ([§88](#88-flags-and-secondary-filters-by-keyword-only)).

## 43. CONAB: winter cereals in the Oct 2019 to Jan 2022 editions

In these editions, the wheat, oat, canola, rye, barley and triticale sheets carry the year in their name ("Trigo 2021"). In 1.1.0, `conab.safras` (and `datasets.estimativa_safra` with `fonte="conab"`) looked for the sheet without the year and raised an error for all six cereals. The sheet is now chosen by its header: the most recent one that publishes the requested crop year wins. When the edition does not yet publish the crop year's own winter crop (surveys 1 to 4 of 2019/20, 3 and 4 of 2020/21 and 2 to 4 of 2021/22), the query returns empty, as in other years' surveys that do not publish the year, instead of raising an error.

## 44. CONAB progress: the "N estados" row is no longer `BR`

In `conab.progresso_safra` and `datasets.progresso_safra` (contract 2.0), the last row of each spreadsheet block, "7 estados",
"12 estados" and so on, is returned with `uf = "MEDIA_ESTADOS"` (the `estado` column becomes `uf`; [§87](#87-renamed-output-columns)). In 1.1.0 it was returned as `BR`, but it is CONAB's own
average of the monitored states (88% to 99.9% of the area, depending on the crop), not Brazil, and it cannot be reproduced from
the survey areas. The new `n_estados` and `cobertura_area_pct` columns carry the number of states and the coverage read from
the note "(Esses N estados correspondem a X% da área cultivada)", not recomputed (0.98 = 98%), and are null on state rows.

Filtering `uf="BR"` raises `InvalidParameterError` with the published coverage when the spreadsheet has no "Brasil" row
(none of the checked bulletins has one). Use `uf="MEDIA_ESTADOS"` and take the coverage into account. The progress
`parser_version` goes from 1 to 3.

## 45. CEPEA: wheat in both locations and the daily change in `meta`

`cepea.indicador("trigo")` without `praca` returns Paraná and Rio Grande do Sul. In 1.1.0, collection from CEPEA returned only
Paraná (the Notícias Agrícolas fallback already returned both). `cepea.ultimo("trigo")` without `praca` returns the most recent
date across the two locations and, on a tie, Paraná, because the order is by location. For the 1.1.0 series, pass
`praca="parana"`. `datasets.preco_diario("trigo")` stays on Paraná and, on a date without Paraná, uses Rio Grande do Sul,
labeled in the `praca` column, by the [tie-break for regional products](../contracts/preco_diario.en.md).

In a CEPEA `Indicador`, `meta["variacao"]` is now the daily change ("Var./Dia"). In 1.1.0 the parser stored there the last
change column of the table, which on daily pages is the monthly one. The monthly change goes to `meta["variacao_mes"]` and the
weekly one to `meta["variacao_semana"]`. For ethanol, whose pages are weekly, the weekly change leaves `variacao` and is only in
`variacao_semana`. As in 1.1.0, the changes come only in an `Indicador` freshly collected from the source: a cache read, warm
or `offline`, does not carry them.

## 46. CONAB costs: items by the published subtotal and category by section

In 1.1.0, `custo_producao` added a valued group together with its sub-items to the operating costs, read a section header
printed with zeros and the "Gestão da propriedade familiar" block after H as items, and classified `categoria` by the exact
label, which changes over the years. In 2.0.0 (parser 5, contract 3.0 kept):

- the items of each section add up to the published subtotal whenever the workbook itself does. The reading that closes
  wins (group, sub-items or both; management aggregate or its components), and the row that leaves stays in
  `meta.source_details["parser"]["notes"]` with its published value;
- a Roman section header printed with zeros opens the section, and a row after the section's subtotal or total becomes the
  `memo_after_total` note;
- a subtotal or formula total that does not close in the workbook itself issues a warning (`UserWarning` and
  `meta.validation_warnings`) with the published numbers; no row is recomputed;
- `categoria` comes from the published section (IV and V: `custos_fixos`; II, III and VI: `outros`) and, in the operating
  costs, from the normalized label. Labor, pesticides, seedlings and own machinery no longer fall into `outros` depending on
  the year's spelling.

Code that summed the `valor_ha` of a section's items now gets the published subtotal. Code that filtered
`categoria == "outros"` to find labor or pesticides should use `mao_de_obra` and `insumos`. Details and the 116 rejected
sheets are in the [contract](../contracts/custo_producao.md).

## 47. IMEA: a record published more than once is returned once

`imea.cotacoes` now collapses a record that IMEA publishes more than once, identical in every column: it is returned once, with
a warning and the count in `source_details["duplicatas_colapsadas"]` (`linhas` and `indicadores`). In 1.1.0 every copy was
returned: on September 25, 2026, 69 extra soybean rows (`R$/sc`). A key (indicator, location, date, crop year and unit)
repeated with different values is still returned in full, now with a warning and the count in
`source_details["chaves_repetidas"]`. Both keys are present in the `source_details` of every query.

## 48. MetaInfo: hash, size and locator of the body that supplied the data

The sources' `MetaInfo` now identifies the acquired body, following the rule in the
[contracts](../contracts/index.en.md#metainfo):

- `raw_content_hash` and `raw_content_size` are no longer null and zero when the response comes from a single body: ABIOVE,
  ANDA, INMET, Acervo Fundiário, ANA, ANTT (plazas), B3, CFTC, CONAB, DERAL, IBAMA, IBGE, ICMBio (`ucs_geo`), IMEA, PSR,
  Queimadas, SICAR, UNICA and Rio Verde.
- For CEPEA, `raw_content_hash` drops the `sha256:` format followed by 16 hex digits, a 64-bit prefix, and becomes the full
  SHA-256, with 64 digits and no prefix. Code that compared against the old format must change. `fetch_timestamp` is now
  filled on collection.
- For ANEC, `raw_content_hash` is no longer the fingerprint of the PDF structure (an MD5 with 16 hex digits) and becomes the
  full SHA-256 of the PDF, with its size in `raw_content_size`. The fingerprint moves to `source_details["layout_fingerprint"]`.
  Code that compared against the old format must change.
- `source_url` is now the resource that supplied the data:
  - for CFTC, the query with its filters; before, the endpoint without filters;
  - for B3, the CSV download with the token as `[REDACTED]`. Before, it was the ticket generator, which now goes in
    `source_details["ticket_url"]`;
  - for IBGE, the SIDRA query when there is only one. Before, it was the SIDRA page, which now goes in
    `source_details["pagina"]`. IBGE's `fetch_timestamp` is now filled.

## 49. USDA PSD: new gateway and labels from the official catalogs

In 1.1.0, `usda.psd` and `oferta_demanda_global` queried the old FAS OpenData, which returns 500, and mislabeled 6 of
the 9 attributes, soybean meal and the EU. In 2.0.0 (parser 2; `oferta_demanda_global` contract 2.0, section 87):

- the host is the `https://api.fas.usda.gov/api/psd` gateway, with the key in the `X-Api-Key` header (the same
  api.data.gov `AGROBR_USDA_API_KEY`);
- `attribute`, `unit`, `country` and the commodity name outside the registry come from the official catalogs stored in
  the package, and `attribute_br` uses the right IDs:

  | Label | ID in 1.1.0 | ID in 2.0.0 |
  |---|---|---|
  | `producao` | 125 (Domestic Consumption) | 28 (Production) |
  | `estoque_inicial` | 28 (Production) | 20 (Beginning Stocks) |
  | `consumo_domestico` | 57 (Imports) | 125; 126 for sugar; 142 for cotton |
  | `importacao` | 130 (Feed Dom. Consumption) | 57 (Imports) |
  | `estoque_final` | 84 (TY Imp. from U.S.) | 176 (Ending Stocks) |
  | `oferta_total` | 176 (Ending Stocks) | 86 (Total Supply) |

  `distribuicao_total` (178) and, for cotton, `perdas` (150) are added;
- `farelo_soja` moves from `4233000` (Oil, Cottonseed) to `0813100` (Meal, Soybean), and `ue`/`eu` from `E2` (EU-15) to
  `E4` (European Union);
- new columns: `attribute_id`, `unit_id`, `last_update_year` and `last_update_month`. The last two are the series' last
  update, not the queried edition, and `last_update_month` is null when the PSD publishes `00`;
- a commodity, country or attribute outside the catalogs raises `InvalidParameterError` before the network. Before, a
  7-digit code or a country of up to 3 letters went straight to the server, which answers `[]` without an error;
- `pivot=True` names each column by its `attribute_br` or, without one, by the official name. A repeated label in the
  same series raises `ParseError`, and a pivot failure no longer returns the long format silently;
- in a query by a direct code outside the registry, `commodity` moves from the code itself to the official catalog name
  (e.g. `0430000` → `Barley`); a code outside the catalog still comes back unchanged from `models.commodity_name`;
- public names in `agrobr.usda`: `models.PSD_COLUMNS_MAP` is removed, since it mapped the old OpenData PascalCase, which
  the gateway does not use; `client.fetch_psd_country`, `fetch_psd_world` and `fetch_psd_all_countries` now return
  `RespostaPSD(url, corpo, dados, status)` instead of the list of records, which is now in `.dados`.

Code that filtered by `attribute_br` now gets the right attribute. Code that used the `PSD_ATTRIBUTES` IDs or `4233000`
as soybean meal must switch to the ones in the table. Details on the [source](../sources/usda.md) and in the
[contract](../contracts/oferta_demanda_global.md). In `datasets.oferta_demanda_global` (contract 2.0), the filters and
columns are in Portuguese; the `usda.psd` source keeps the English names ([§85](#85-parameter-names-one-vocabulary-across-the-api), [§87](#87-renamed-output-columns)).

## 50. Dead code cleanup

This cleanup removes code with no effect and changes no data. Errors for invalid input change, and public names with no production use are removed:

- `utils.validate_bbox`, used by SFB, Acervo Fundiário, ANA, IBAMA, ICMBio and MapBiomas Alerta, raises
  `InvalidParameterError` instead of `ValueError`. Since it is a subclass of `ValueError`, code that catches `ValueError`
  still catches it, and code that catches `AgrobrError` now does too. In IBAMA, an area published with a dot raises
  `ParseError`, instead of "12.5" becoming 125.
- Public names with no production use are removed: `utils.concat_csv_pages` (no replacement);
  `alt.sicar.parser.parse_imoveis_csv` (use `alt.sicar.imoveis`); `anec.models.normalize_produto` (use
  `resolve_produto`, which rejects unknown products); `bcb.parser.parse_credito_rural` no longer resolves
  `fonte_recurso`, `modalidade` and `atividade`, which the default 1.1.0 call published and the 2.0 `uf` and `programa`
  aggregations do not (the `cd_*` codes remain). The 3 names come back in `agregacao="registro"` (section 4), and
  `resolve_fonte_recurso`, `resolve_modalidade`, `resolve_atividade`, `SICOR_FONTES_RECURSO`, `SICOR_MODALIDADES` and
  `SICOR_ATIVIDADES` stay in `bcb.models`, with the names from the official tables and `None` for a code outside the
  table (1.1.0 returned `Desconhecido (<code>)`); `defensivos.parser.parse_formulados_csv` and `parse_tecnicos_csv` (use `parse_*_bundle`, which
  returns the tables and the details) and `defensivos.models.FORMULADOS_COLS_DROP`/`TECNICOS_COLS_DROP`;
  `lista_suja.models.DOWNLOAD_URL` (use `constants.URLS[Fonte.LISTA_SUJA]["download"]`), `RENAME_MAP` and
  `PDF_HEADER_ROW_MARKER`; `rnc.client.fetch_registradas`/`fetch_protegidas` (use `fetch_*_bundle`, with `.content` and
  `.resource.url`) and `rnc.parser.parse_registradas_csv`/`parse_protegidas_csv` (use `parse_*_bundle(...).frame`);
  `alt.mapa_psr.client.download_csv` and `fetch_periodo` (use `open_periodo`). When
  `bcb.bigquery_client.fetch_credito_rural_bigquery` is called directly, an invalid harvest raises `ValueError` instead
  of silently dropping out of the filter.
- More public names with no production use are removed: `ibge.ftp_client.extract_xls_from_zip` (no
  replacement); in `desmatamento.parser`, the v1 CSV/GeoJSON parser (`parse_prodes_csv`, `parse_deter_csv`,
  `parse_deter_geojson` and `parse_prodes_geojson`; use `desmatamento.prodes`, `deter`, `prodes_geo` and `deter_geo`),
  and the 12 v1 column constants in `desmatamento.models` (`PRODES_COLUNAS_WFS*`, `DETER_COLUNAS_WFS*`,
  `COLUNAS_SAIDA_PRODES*`, `COLUNAS_SAIDA_DETER*`, `PRODES_GEOM_COLUMN` and `MAX_FEATURES_GEO`);
  `b3.models.parse_numero_br` (use `normalize.numeric.safe_float`); `anda.models.ANDA_UFS` (no replacement);
  `deral.models.DERAL_PRODUTOS` and `normalize_condicao` (the published crops remain in `DERAL_PRODUTOS_PUBLICADOS`).
  `normalize.encoding.ENCODING_CHAIN` loses `utf-16` and `ascii`, which never ran after `iso-8859-1`. `anda.entregas`
  and `datasets.fertilizante` lose the `uf` argument: the published PDFs only carry the national total (`uf="BR"`),
  and passing `uf` raises `TypeError`. In ABIOVE, a `produto` outside the list raises `InvalidParameterError` (a
  subclass of `ValueError`) before the network, and an `agregacao` other than `"detalhado"` and `"mensal"` raises
  `InvalidParameterError` instead of becoming `"detalhado"`. Layouts with no publication are no longer read: state
  tables in ANDA, crop-named sheets in DERAL and, in ABIOVE, products in columns, tables with a named header and the
  product taken from the sheet name (the workbook raises `ParseError`).

## 51. ANEC: four-product bulletins and unnamed column

Editions up to W2/2026 publish four products per period (soybean, soybean meal, maize and wheat); DDGS and sorghum start
in W3/2026. In 1.1.0, W1 and W2/2026 came out wrong: soybean, soybean meal and maize from both weeks were labeled
`last_week`, the monthly table was read by position (in January 2025, wheat with 6 t and sorghum with the total,
6,614,352 t) and, in W1, current-week wheat (144,290 t) and São Francisco do Sul soybean meal (65,000 t) were lost. In
2.0, both editions are read by name and match the bulletin's TOTAL row.

A column without a product name now raises `ParseError`. In W14/2026, the first current-week column has data and no
label: 1.1.0 dropped those values, and 2.0 rejects the edition, because agrobr does not guess the product.

The sum of the ports in each weekly column is checked against the published TOTAL row. A difference above rounding
becomes a `UserWarning` and an entry in `meta.validation_warnings`, without changing values. See the
[source](../sources/anec.md).

## 52. Source dates: the same rule on pandas 2 and 3

In 1.1.0, IBAMA, Acervo Fundiário, CFTC, INMET, MapBiomas Alerta and Queimadas converted dates with
`pd.to_datetime(errors="coerce")`, and the result depended on the pandas version: a date outside the `datetime64[ns]`
range (before 1677 or after 2262) became `NaT` on pandas 2 and came out as published on pandas 3. In IBAMA's current CSV,
the embargoes dated 1667 and 2925 change value with the environment.

In 2.0, the 6 sources follow a single rule, on both versions:

- an unreadable value, or one with a year outside 1900–2099, becomes `NaT`, including dates from 1677 to 1899 and from
  2100 to 2262, which 1.1.0 published on both versions;
- for IBAMA, an embargo or disembargo date on a day after the file's own edition (`ULTIMA_ATUALIZACAO_RELATORIO`) also
  becomes `NaT`: in the 23/09/2026 edition, the terms dated 2063, 2080 and 2090;
- a query that discards values raises a `UserWarning` and puts the same message in `meta.validation_warnings`, with the
  source, the column and the count.

**Every date column of the public outputs, from sources and datasets, comes out as `datetime64[ns]`** (`Datetime("ns")` in
polars). On pandas 3, 1.1.0 returned `s`, `ms`, `us` or `ns` depending on the source and the input, and joining 2 datasets on
the date in polars failed on the unit.

INMET drops the observation without a date and CFTC rejects the response with `ParseError`, as they already did with an
unreadable date. The published text of a discarded date remains in the source's raw body. See
[Normalization](normalizacao.en.md#source-dates).

## 53. MetaInfo: `fetch_timestamp` is the acquisition time, in datasets too

In 1.1.0, every dataset's `fetch_timestamp` was the time it built the `MetaInfo`, not the time the data was acquired. On a
cache hit, the dataset claimed an acquisition that never happened: `datasets.preco_diario` read from CEPEA's DuckDB and
`datasets.clima` from INMET's ZIP came with the call time.

In 2.0, `fetch_timestamp` is the UTC time of the acquisition of the body the top level of `MetaInfo` describes, and datasets
pass on the source's value (rule in [contracts](../contracts/index.md#metainfo)):

- a body received now: the time of that acquisition;
- a body read from cache (INMET, Acervo Fundiário, ANEC, ZARC, and Agrofit): the time of the original acquisition, equal to
  `fetched_at`; before, these sources published the call time;
- several bodies (BCB Focus, PTAX, and SGS, Comtrade, PRODES/DETER, Embrapa Solos, FUNAI, and INCRA): the most recent
  acquisition, equal to `fetched_at`; before, the build time;
- records from CEPEA's DuckDB cache: the original collection (the latest `parsed_at` among the returned rows), equal to
  `fetched_at`, at the source and in `datasets.preco_diario`.

Code that measured freshness by a dataset's `fetch_timestamp` now reads the acquisition. The build time remains in
`timestamp`.

`datasets.preco_diario` no longer has a `cache` source (see Changed): when collection fails, `cepea.indicador` reads the DuckDB
cache and returns `fetched_at` and `fetch_timestamp` with the records' original collection; before, the dataset's `cache`
source published the call time.

## 54. B3: provenance of `ajustes`, `historico`, and `posicoes_abertas_historico`

In 1.1.0, `b3.ajustes` came with a null `raw_content_hash` and a zero `raw_content_size`, and `b3.historico` without each
day's bodies. In 2.0:

- `b3.ajustes` and `datasets.futuros_agricolas(tipo="ajustes")`: SHA-256, size, and acquisition time of the received
  `PRyymmdd.zip`, at the top level. B3 rebuilds the outer ZIP on every request, so the top-level hash changes between 2
  downloads of the same session. The stable identity is in `source_details["zip_interno"]` and `source_details["xml"]`
  (name, SHA-256, and bytes);
- `b3.historico`: `source_details["corpos"]` lists each received day. With a single day, the top level is that body's; with
  several, the top level is null and zero, and `fetched_at`/`fetch_timestamp` are the most recent acquisition, by the rule
  in section 53.
- `b3.posicoes_abertas_historico`: `source_details["corpos"]` lists each day with a file, with the download URL (token as
  `[REDACTED]`) and `ticket_url`, under the same rule as `historico`. Days without a file are not listed.

Code that compared the hash of 2 downloads of the same session should compare the XML's.

## 55. `as_polars`: each column's type comes from the contract

In 1.1.0, `as_polars=True` converted the DataFrame by its data. A column filled with `pd.NA` came out with polars' `Null`
type, and the same column was `String` in one query and `Null` in another: `pl.concat` of `producao_anual("soja")` and
`producao_anual("cafe")` failed on `condicao_produto`, and that of `exportacao` from ComexStat and from the ABIOVE
fallback failed on `uf`.

In 2.0, all datasets type by [contract](../contracts/index.md#global-guarantees): `int` → `Int64`, `float` → `Float64`,
`str` → `String`, and `bool` → `Boolean`, even when the column is all null; an all-null date column is `Datetime("ns")`. A
column outside the contract keeps the type of its data. That is why the extra requires `polars>=0.20.3`, the first version
with the `String` type.

## 56. Embrapa Solos: double-encoded text repaired

In 1.1.0, profiles came with the text as Embrapa publishes it, part of it double-encoded (UTF-8 read as Latin-1):
"BrasÃ­lia", "SÃ£o Carlos", "AptidÃ£o". Filters and joins by municipality name came back empty.

In 2.0, text that round-trips through Latin-1 → UTF-8 and carries the signature ("Ã" or "Â" followed by a character between
U+0080 and U+00BF) comes repaired ("Brasília", "São Carlos"), with the per-column count in `MetaInfo.validation_warnings`.
Legitimate text with "Ã" stays as is. Code that already handled the double encoding on its own should drop that step, so it is
not applied twice. Text with the signature that the source publishes beyond repair (cut in the middle of a UTF-8 sequence, with "�" or "€") stays as published and gets its own count in the same warning. Embrapa's `parser_version` becomes 3.

## 57. MetaInfo: `source_url`, manifests, and cache

- `bcb.focus`: `source_url` is no longer the query's last page (empty, with `$skip`) but the query itself, without `$top`
  and `$skip`. Pages remain in `source_details["resources"]`.
- `antt_pedagio.fluxo_pedagio`: `raw_content_size` no longer adds up every received byte; it is the size of the manifest,
  the same object as `raw_content_hash`. Total bytes received, including retries and failures, are in
  `source_details["received_bytes"]`, and the bytes of the CSVs that went into the data, in
  `source_details["data_file_bytes"]`.
- `bcb.sgs` and `bcb.ptax`/`bcb.ptax_moedas`: `source_url` becomes the query instead of the last resource. For SGS, the
  whole requested range (before, the last date block); for PTAX, the query without `$top` and `$skip`.
- `conab.custo_producao` and `custo_producao_total`: `raw_content_size` becomes the manifest size, as for ANTT. Total
  bytes received are in `source_details["received_bytes"]`, and the workbook's, in `source_details["data_file_bytes"]`.
- `bcb.credito_rural`, `bcb.credito_rural_total`, and `datasets.credito_rural`: `source_url` is no longer the OData entity
  but the query, with `$filter` and `$select` and without `$top`; `raw_content_hash` and `raw_content_size` become the
  `{query, resources}` manifest's, with each page in `source_details["resources"]`.
- `ibge.*` (PAM, LSPA, PPM, slaughter, censuses, PEVS, milk, and agricultural GDP): `cache_expires_at` becomes null. In
  1.1.0, PAM, LSPA, PPM, and slaughter stamped an expiry without any cache; every call queries IBGE.
- `conab.safras`: `cache_expires_at` becomes null. In 1.1.0, it stamped 24 h with no cache at all; each call downloads
  the CONAB publication.

## 58. Comtrade: UN estimation flags

`comtrade.comercio` (contract `comercio_bilateral` 2.1) and `datasets.comercio_internacional` (contract 3.0) gain `peso_liquido_estimado`,
`peso_bruto_estimado`, and `quantidade_estimada`, nullable UN flags. `True` means the published measure is a UN estimate,
not the country's declaration. When the result has an estimated net weight, `meta.validation_warnings` names the HS and
period, and `peso_liquido_kg`, `volume_ton`, and `trade_mirror`'s `ratio_peso` use the estimated value. `trade_mirror` lists
those cells per leg in `source_details["peso_estimado"]`. Code comparing with ComexStat can filter or flag those rows.

## 59. CONAB: `safras` and `balanco` reject an unknown product before the network

`conab.safras` with a product outside `CONAB_PRODUTOS` raised `ParseError` ("Produto não suportado") after 4 requests. In
2.0 it raises `InvalidParameterError` before the network, listing the valid ones. Code that caught `ParseError` for this
case should catch `InvalidParameterError`.

`conab.balanco` with a product outside the 6 of the Suprimento sheet (`soja`, `milho`, `arroz`, `feijao`, `trigo`, and
`algodao`, with or without accents) downloaded the workbook and returned an empty frame. In 2.0, it also raises
`InvalidParameterError` before the network.

## 60. `exportacao` and `importacao`: the contract columns, from both sources

In 1.1.0, `exportacao` came out with different columns depending on the source: from ComexStat, with `volume_ton`, outside the
contract; from the ABIOVE fallback, also with `receita_usd_mil` and in another order. In pandas `concat` worked; in polars, only
with `how="diagonal"`.

In 2.0, `exportacao` (contract 1.1) and `importacao` (1.2) come out with the contract columns, in its order. `volume_ton`
(t = `kg_liquido` / 1000) becomes an optional contract column, and `receita_usd_mil` leaves the dataset (it is `valor_fob_usd`
in thousands and remains in `agrobr.abiove`).

## 61. ANDA: an unknown `agregacao` is rejected before the network

`anda.entregas` with an `agregacao` other than `"detalhado"` and `"mensal"` (for example, `"semanal"`) downloaded the PDF and
silently returned the detailed table. In 2.0, it raises `InvalidParameterError` (a subclass of `ValueError`) before the
network, as ABIOVE does (section 50). The `fertilizante` dataset has no such argument and does not change.

## 62. Queimadas: negative FRP and repeated hotspots no longer bring down the month

`datasets.queimadas` rejected the whole month with `ContractViolationError` when INPE published a hotspot with negative FRP or
the same hotspot twice (7 of the 20 months read from 2023 to 2026, among them Aug 2024, the example in the docs). In 2.0,
`queimadas.focos` and the dataset deliver the month: an equal copy comes out once; a repeated key that differs only in FRP comes
out as 1 row, with a null `frp`; one that differs in another column is dropped from the result; and a negative FRP comes out
null. Each case comes with a warning and the count in `meta.validation_warnings` and `source_details`. Code that read the
negative FRP or both rows of the repeated hotspot from the source now gets them this way. See the
[contract](../contracts/queimadas.md#negative-frp-and-repeated-hotspot).

## 63. MapBiomas: a class outside the legend comes out null, with a warning

In 1.1.0, `mapbiomas.cobertura`, `mapbiomas.transicao` and the state `uso_do_solo` returned `Classe {id}` when the
workbook carried a code outside the known legend. In 2.0, the label (`classe`, `classe_de` or `classe_para`) comes out
null, the published `classe_id` stays, and the query emits a `UserWarning` and puts the same message in
`meta.validation_warnings`, listing the codes. The municipal cut follows the same rule. The `mapbiomas_cobertura` and
`mapbiomas_transicao` contracts therefore move to 2.0 (`MAPBIOMAS_COBERTURA_V2` and `MAPBIOMAS_TRANSICAO_V2`): a
required column that becomes nullable is a major change. Code that filtered `classe.str.startswith("Classe ")` now
filters `classe.isna()`.

**Collection 11 by default.** Without `colecao`, `mapbiomas.cobertura`, `mapbiomas.transicao` and `datasets.uso_do_solo`
read collection 11 (1985–2025), no longer 10: the same call returns other figures, and new classes appear, such as
"Savana Alagada (beta)". For the 1.1.0 cut, pass `colecao=10`.

## 64. CEPEA: no network and no cache raise an error instead of an empty table

In 1.1.0, `cepea.indicador` with no network and nothing cached for the period returned the empty table, with no error
or warning, and the `MetaInfo` said `from_cache=True`; `cepea.ultimo` raised `ParseError` (a layout error). In 2.0, both
raise `SourceUnavailableError`, with `attempted_sources` (the sources tried and the cache). A page layout error is still
`ParseError`. With cache, the answer comes from it with a `StaleDataWarning`, as before. With `offline=True` and an
empty cache, `indicador` still returns the empty table, now with `from_cache=False` and a null `cache_expires_at`. Code
that treated the empty table as a network failure now catches `SourceUnavailableError`. `cepea.ultimo(produto, offline=True)` with no data in the cache raises the same `SourceUnavailableError`, with the reason "offline sem dado no cache" (before, `ParseError`).

## 65. IBGE: `uf` with a level that does not filter is rejected

In 1.1.0, `ibge.pam`, `ibge.ppm`, `ibge.censo_agro`, `ibge.censo_agro_historico`, `ibge.silvicultura`,
`ibge.extracao_vegetal` and `datasets.producao_anual` with `uf` and `nivel="brasil"` (or `"regiao"` in the historical
census) returned the country or region aggregate, without the filter and without a warning. In 2.0, they raise
`InvalidParameterError` before the network. With `uf`, use `nivel="uf"` or `"municipio"`; for the aggregate, drop `uf`.

## 66. Impossible arguments rejected before the network

In 1.1.0, these queries returned an empty frame (or the data without the filter) without an error, or raised an
unavailability error after the network. In 2.0, they raise `InvalidParameterError`:
- `conab.serie_historica` with `ano_inicio > ano_fim` or an unknown state, before the network;
- `desmatamento.prodes` (and the dataset) with a year after the current one, before the network; a valid year without
  features still returns empty, now with a warning;
- `conab.ceasa_precos` and `preco_atacado` with a product or CEASA outside what CONAB/PROHORT publishes, listing the valid
  ones (after the network call, against the response: a published product outside the 48 in `ceasa_produtos()` filters);
- `ibge.abate` and `ibge.leite_trimestral` with an unknown state (before, `SourceUnavailableError`), before the network;
- `cftc.cot` with `inicio > fim`, `conab.custo_sociobiodiversidade` with a year after the current one, and
  `antaq.movimentacao` with an unknown state, before the network or the download;
- `datasets.futuros_agricolas` with `data` and `tipo="historico"`, or with `inicio`, `fim`, or `vencimento` and `tipo="ajustes"`/`"posicoes"`;
- `b3.historico` and `futuros_agricolas(tipo="historico")` with an option code in `vencimento` (e.g. `VVJK`): settlements only carry futures, and 1.1.0 returned an empty result. Use the contract month code (e.g. `V26`);
- an unknown state (e.g. `uf="XX"`), before the network:
  - `bcb.credito_rural` and `datasets.credito_rural`: 1.1.0 filtered the state after the query and returned an empty result;
  - `conab.safras` and the `estimativa_safra` and `producao_anual` datasets: empty through CONAB's local filter (in
    `producao_anual`, 1.1.0 first passed the unknown code to IBGE);
  - `queimadas.focos` and `comexstat.exportacao`/`importacao`: empty through the local filter, after the download;
  - `datasets.clima`: in 1.1.0, INMET and NASA POWER rejected the state, and the dataset raised `SourceUnavailableError`.

An unknown state was already an error in 1.1.0, with `ValueError`, in ANTT (tolls), ANP (diesel), Desmatamento, Embrapa
Solos, FUNAI, INCRA, NASA POWER and `inmet.clima_uf` (the latter after downloading the station catalogue). In 2.0, they
raise `InvalidParameterError`, a subclass of `ValueError`, before the network: code catching `ValueError` still catches it.

Code that treated the empty result (or the slaughter and milk `SourceUnavailableError`) as "no data" should catch
`InvalidParameterError`.

## 67. USDA: without `market_year`, the previous year until the May WASDE

In 1.1.0, `usda.psd` without `market_year` and `datasets.oferta_demanda_global` without the year (`ano_comercial` in 2.0) asked for the current calendar year,
which the PSD only publishes from the May WASDE on: from January to April the result came back empty, with no warning.
In 2.0, when the current year comes back empty, the query asks for the previous one, and
`source_details["market_year"]` says which was used (`market_year_tentados` lists the requests). Code that needs a
fixed year passes the year (`market_year` in the source, `ano_comercial` in the dataset); with it, there is no fallback.

## 68. CLI: JSON dates in ISO 8601

In 1.1.0, `--formato json` wrote dates as milliseconds since 1970 (`1790035200000`), the `DataFrame.to_json` default.
In 2.0, every command writes dates in ISO 8601 (`"2026-09-22T00:00:00.000"`). Code that read the number reads the text:
`pd.to_datetime(...)`, `datetime.fromisoformat(...)` or, in `jq`, `.data[:10]`.

## 69. MapBiomas Alerta: `max_registros` instead of `limit`, code order and `tipo_data`

In 1.1.0, `alertas(limit=...)` and `alertas_geo(limit=...)` set the page size, and the query silently stopped at 50 pages
(5,000 alerts by default). The API's default order repeated and dropped alerts across pages, and `bbox` went in the wrong
order and came back empty. In 2.0:
- `limit` is gone. `max_registros` (default 5000) is the row cap, with a warning when it cuts, and `max_registros=None`
  brings the whole collection. `limit=` raises `TypeError`;
- pagination follows the code order and is checked against `totalCount`: a repeated code or an incomplete collection raises
  `ParseError`;
- `bbox` now filters (before, every query with a box came back empty);
- `tipo_data="publicacao"` filters by the publication date. The default stays on detection, with a warning when the period
  ends less than 293 days before today;
- a start after the end, or a date outside `YYYY-MM-DD`/`DD/MM/YYYY`, raises `InvalidParameterError` before the network.

Code that passed `limit=100` to get 100 alerts per page drops the argument (or uses `max_registros=None` for the whole
collection).

## 70. CEPEA: older periods come from the historical series

In 1.1.0, `cepea.indicador` and `datasets.preco_diario` only had the page's recent window (about 15 trading days) and
what the cache had accumulated: an earlier period came back empty or incomplete, with no warning. In 2.0, that period
comes from CEPEA's historical series. The first query that needs it downloads the product's whole series (a spreadsheet of
up to ~0.6 MB per indicator) and stores it in the cache; the default without `inicio` (365 days) goes through it too. An
unavailable series warns in `validation_warnings`, and a period left without data raises `SourceUnavailableError`. To
use the cache only, pass `offline=True`. Oranges have no series.

## 71. Deterministic mode: a warning where the mode does not apply

In 1.1.0, every dataset accepted `datasets.deterministic(...)`, but only `preco_diario` honored it (local cache, no
network, date cutoff). Datasets that go to the network silently queried the current source, with `meta.snapshot` filled
in, as if the data were that date's. In 2.0, those datasets warn in `validation_warnings` and with a `UserWarning` that
the data is current. Six of them now reject the context with `InvalidParameterError`, before the network, because the
source cannot return the data as it stood on that date: `cadastro_rural`, `desmatamento`, `exportacao`, `importacao`,
`uso_do_solo` and `zoneamento_agricola`. In 1.1.0 they returned current data: move those calls outside the
`deterministic(...)` block. Deterministic
`preco_diario` without the product in the local cache raises `SourceUnavailableError` instead of returning an empty frame:
populate the cache (or copy `~/.agrobr/cache/`) before reproducing on another machine. A period with no data, with the
product in the cache, still comes back empty.

## 72. 1995 census: `informantes` in the crop themes

In `ibge.censo_agro` and `datasets.censo_agropecuario`, variable 151 of the 1995 temporary and permanent crops (SIDRA
tables 492 and 504) is published as `informantes` instead of `estabelecimentos`: SIDRA labels it "Número de informantes".
In 2017, `estabelecimentos` still comes from the number of agricultural establishments (variables 10084 and 9504). Code
that filtered `variavel == "estabelecimentos"` in the 1995 crop themes filters `"informantes"` instead.

## 73. Logs off standard output

In 1.1.0, used as a library, agrobr printed structlog's logs (debug and info) to standard output, and `logging.basicConfig`
did not control them: `python export.py > data.csv` came out with logs at the top of the CSV. In 2.0, agrobr's logs go through
the standard library's `logging`, as JSON, in the logger of the module that emits them (`agrobr.cepea.parsers.v1`, for
example). Without configuration, only warnings and errors come out, on standard error; `logging.basicConfig(level=logging.DEBUG)`
turns them all on, and `logging.getLogger("agrobr")` controls agrobr only. `import agrobr` configures neither structlog nor
`logging`.

**Applications that use structlog.** The application's structlog configuration, made before or after the import, applies only
to its own logs; agrobr's stay in `logging`, as JSON. In 1.1.0, it applied to agrobr's logs too: to see them, configure
`logging` (`logging.basicConfig` or a handler on the `"agrobr"` logger).

**CLI.** `--verbose` shows INFO logs on standard error; without it, only warnings and errors. CLI logs come out readable,
in structlog's console format, without colour and one per line, as in 1.1.0; JSON applies to library use.

## 74. Datasets: columns in contract order and dates as `datetime64`

In 1.1.0, 8 datasets (`balanco`, `censo_agropecuario_legado`, `comparacao_anual_anec`, `pib_agro`, `producao_anual`,
`queimadas`, `serie_historica_safra` and `clima`) returned columns in the source order when there was data, and in the
contract order when empty. In 2.0, every dataset returns the contract columns in its order, with extra columns at the end,
in the source order, and `MetaInfo.columns` follows. If you read by position (`df.iloc[:, 3]`, `df.columns[0]`), read by
name. The `data` column of `queimadas` (and of the `queimadas.focos` and `focos_geo` sources) comes as `datetime64[ns]`
(`Datetime("ns")` in polars), no longer as `datetime.date` in an `object` column: `df["data"].dt.date` returns the old
value.

## 75. HTTP error statuses: `SourceUnavailableError`, not `httpx.HTTPStatusError`

In 1.1.0, a 403 or a 404 from the source came out of several sources as a raw `httpx.HTTPStatusError`, and out of
others as `SourceUnavailableError`. In 2.0, every HTTP error status comes out as `SourceUnavailableError`, with the
source and the status in the message: "HTTP 403" or "HTTP 404", in most sources followed by the reason
("HTTP 403: a fonte recusou o pedido (bloqueio de WAF ou permissão)", "HTTP 404: o recurso não existe na URL").
If you caught `httpx.HTTPStatusError` from these sources, catch `SourceUnavailableError` instead and read the status
from the message (`"HTTP 404" in str(error)`): ABIOVE, ANDA, ANEC, ANP (diesel), ANTT (tolls), B3, BCB (SGS, PTAX and Focus), CFTC, ComexStat, Defensivos, DERAL, Desmatamento, Embrapa Solos, FUNAI, IBAMA, IBGE (legacy census, over FTP), ICMBio, IMEA, INCRA, INMET (historical archive and API), Lista Suja, MapBiomas, MAPA PSR, NASA POWER, Queimadas, RNC, SFB, SICAR, UNICA, USDA and ZARC. In 1.1.0, they raised `httpx.HTTPStatusError` on 403, on 404 or on
both. SIDRA queries (IBGE) raised `ValueError` with the body of the error page, and now raise the same
`SourceUnavailableError`.

- **USDA, PSD 404:** the gateway answers 404 for a year without data (Brazilian soybeans in 1950, for example). The empty
  table stays, but now with a warning in `validation_warnings` and a `UserWarning`, because the same 404 comes out if the
  API URL changes.
- **Datasets:** the final `SourceUnavailableError` carries `attempted_sources` with the sources tried, in order, and
  `__cause__` with the error of the last one. In `errors`, a source that failed on an HTTP status now has the type
  `"unavailable"` (in 1.1.0, `"network"`).
- **`NetworkError`** is still exported, but nothing raises it in 2.0.

## 76. Rural credit: the crop year comes as "YYYY/YY"

In 1.1.0, `bcb.credito_rural` and `datasets.credito_rural` published the crop year as "2023/2024"; the new
`bcb.credito_rural_total` was born in the same format. In 2.0, the 3 publish "2023/24", the format of the other datasets
(`estimativa_safra`, `balanco`, `progresso_safra`, `serie_historica_safra`), and a `merge` on `safra` matches again. The
input still accepts "2023/24", "2023/2024" and "2024". To convert what was saved with 1.1.0:
`df["safra"] = df["safra"].map(agrobr.normalize.dates.normalizar_safra)`.

## 77. Hostile input: expansion limit, `semana_url` and snapshot name

- **Expansion limit:** ZIP and XLSX files that expand beyond the source limit raise `ResourceLimitError` before decompressing.
  The limits sit above the largest file each source publishes (the table is in
  [Resilience](../advanced/resilience.md#expansion-limit-zip-and-xlsx)); legitimate data does not change.
- **`semana_url`:** `conab.progresso_safra` and `datasets.progresso_safra` only accept pages under `https://www.gov.br/conab/`.
  Any other URL (including `http://` or another port) raises `InvalidParameterError`, before the request.
- **Snapshot name:** the Windows reserved names (`CON`, `PRN`, `AUX`, `NUL`, `COM1`–`COM9` and `LPT1`–`LPT9`, with or without
  an extension, in any case) and names ending in a dot raise `ValueError` on every system, so the snapshot opens on Windows.

## 78. Argument outside the signature raises `TypeError`

In 1.1.0, the 110 public functions below accepted `**kwargs`: an unknown or misspelled argument was silently dropped, or passed on to
the fetcher, which read only the keys it knew. The cut came out wider than requested, without an error:
`ibama.embargos(municipio="X")` returned the whole of Brazil, and so did `datasets.progresso_safra("soja", uf="MT")`. In
2.0, 77 of them get an explicit signature, and an argument outside it raises `TypeError` before the network; the other
33 keep `**kwargs` and reject the unknown name.

- **Datasets (32):** `abate_trimestral`, `balanco`, `cadastro_rural`, `censo_agropecuario`, `censo_agropecuario_historico`, `censo_agropecuario_legado`, `censo_agropecuario_municipal_1985`, `clima`, `comercio_internacional`, `condicao_lavouras`, `credito_rural`, `embarques_anec`, `estimativa_safra`, `exportacao`, `extrativismo_vegetal`, `fertilizante`, `futuros_agricolas`, `importacao`, `leite_industrial`, `movimentacao_portuaria`, `oferta_demanda_global`, `pecuaria_municipal`, `pib_agro`, `posicionamento_fundos`, `preco_atacado`, `producao_anual`, `progresso_safra`, `queimadas`, `seguro_rural`, `serie_historica_safra`, `silvicultura`, `uso_do_solo`.
- **Source functions (45):** `abiove.exportacao`, `acervo_fundiario.assentamentos`, `acervo_fundiario.assentamentos_geo`, `acervo_fundiario.sigef`, `acervo_fundiario.sigef_geo`, `acervo_fundiario.snci`, `acervo_fundiario.snci_geo`, `alt.sicar.imoveis`, `alt.sicar.imoveis_geo`, `alt.sicar.resumo`, `ana.demanda_irrigacao`, `ana.demanda_irrigacao_geo`, `ana.disponibilidade_hidrica`, `ana.disponibilidade_hidrica_geo`, `ana.hidrografia`, `ana.hidrografia_geo`, `ana.pivos_irrigacao`, `ana.pivos_irrigacao_geo`, `anda.entregas`, `b3.ajustes`, `b3.historico`, `b3.posicoes_abertas`, `b3.posicoes_abertas_historico`, `conab.ceasa_precos`, `conab.progresso_safra`, `deral.condicao_lavouras`, `ibama.embargos`, `ibama.embargos_geo`, `icmbio.ucs_geo`, `imea.cotacoes`, `inmet.clima_uf`, `inmet.estacao`, `inmet.historico`, `mapbiomas_alerta.alertas`, `mapbiomas_alerta.alertas_geo`, `queimadas.focos`, `queimadas.focos_geo`, `rio_verde.ensaio_soja`, `sfb.cnfp`, `sfb.cnfp_geo`, `sfb.concessoes`, `sfb.concessoes_geo`, `sfb.ifn_conglomerados`, `sfb.ifn_conglomerados_geo`, `usda.psd`.
- **Functions that keep `**kwargs` (33):** in 1.1.0 they also dropped an unknown name; in 2.0 they reject it before the network. `TypeError`: `anec.comparacao_anual`, `anec.destinos`, `anec.embarques`, `anec.embarques_mensais`, `desmatamento.prodes`, `prodes_geo`, `deter`, `deter_geo`, `embrapa_solos.perfis`, `perfis_geo`, `mapa_solos`, `mapa_solos_geo`, `funai.terras_indigenas`, `terras_indigenas_geo`, `icmbio.ucs`, `incra.quilombolas`, `quilombolas_geo` and the `custo_producao`, `desmatamento` and `zoneamento_agricola` datasets. `InvalidParameterError`: `comtrade.comercio`, `comtrade.trade_mirror`, `defensivos.formulados`, `autorizacoes`, `tecnicos`, `lista_suja.empregadores`, `mapbiomas.cobertura`, `mapbiomas.transicao`, `nasa_power.clima_ponto`, `clima_uf`, `rnc.registradas`, `protegidas` and `zarc.zoneamento`.
- **Arguments that only reached the fallback:** `datasets.exportacao(mes=...)` went to ABIOVE (section 39), and
  `datasets.producao_anual(safra=...)` to CONAB. In 2.0, both raise `TypeError`. Use `abiove.exportacao(ano, mes=...)` or
  filter the result's `mes`, and `conab.safras(produto, safra=...)` or `producao_anual`'s `ano`.

What to do: remove the argument or use the name in the signature (`help(function)` shows the accepted ones). The
`CHANGELOG` lists the changes by source.

## 79. ZARC: contract 2.1, without a primary key

The `zarc.zoneamento` contract, of the `zoneamento_agricola` dataset, moves from 1.0 to 2.1. Version 1.0 declared the key
`[cultura, safra, geocodigo, solo_codigo, ciclo_codigo]`; 2.1 declares no key: the source publishes more than one row for
the same combination (literal duplicates and distinct risks), and agrobr preserves every published occurrence.

What to do: deduplicating by those 5 columns discards rows the source publishes. To identify a row, use `registro_origem`
(the CSV position) together with `meta.raw_content_hash` (the body's SHA-256): the position only holds within the same
body and does not identify the observation across revisions. See the [contract](../contracts/zoneamento_agricola.md).

## 80. Body in another format: `ParseError`, not `SourceUnavailableError`

With an HTTP 200 response in another format (for example, a JSON `{"value": []}` where the source publishes another
structure), SGS, ComexStat, ZARC and Desmatamento raised `SourceUnavailableError` in 1.1.0. In 2.0, they raise
`ParseError`. In Desmatamento, the empty body also becomes `ParseError`.

What to do: nothing, if you catch `AgrobrError`, the base of both. Code that caught only `SourceUnavailableError` for this
case should also catch `ParseError`.

## 81. Municipal 1985 census: cell by cell, each with its `status` (contract 2.0)

In 1.1.0, `ibge.censo_agro_municipal_1985` read 53 OCR-extracted CSVs for 22 states. Those CSVs had swapped themes,
wrong currency, lost columns and confused levels, and 2.0.0 redid the whole extraction from the IBGE's 28 PDFs.

**The API** has the same signature (`tema`, and `uf`, `nivel`, `as_polars` and `return_meta` by name only), but what it returns
changes:

- **The state level goes from `total` to `uf`**, in the argument and in the `nivel` column. `nivel="total"` returned the state rows and now raises `InvalidParameterError`; `nivel="uf"` raised `ValueError` and now returns those rows. Replace
  `"total"` with `"uf"` in the argument and in filters on the `nivel` column.
- **1 row per PDF cell**, keyed by `(volume, tabela, pagina_pdf, linha, coluna)`. There is no longer 1 row per place and
  variable. Pivot on `coluna` (or `coluna_nome`) if you need the wide format.
- **`valor` is filled only in cells confirmed by the printed sums.** `valor_lido` always carries the reading, and `status` gives
  the confidence level. Anyone who used every 1.1.0 value chooses the accepted `status`: `df[df["valor"].notna()]` (confirmed only)
  or `df[df["status"].isin([...])]`.
- **The theme follows the printed title**, the same in every volume. 19 names changed, because the old map was shifted (for
  example, table 91 is `depositos_producao`, not `meios_transporte`). See `temas_censo_agro_municipal_1985()`.
- **27 states** (was 22): MA, PI, CE, RN and TO join.
- **Refusals:** an invalid theme, state or level raises `InvalidParameterError`, and so does a state whose volume lacks the table,
  with the states that have it. A table that is in the volume but of which the extraction read no cell (AM 80, AP 80, RR 80 and
  RR 119) raises `ParseError`. In 1.1.0, an invalid theme, state or level and a state without the table raised `ValueError`.
  Of the 4 tables that now raise `ParseError`, AM 80, AP 80 and RR 80 raised `ValueError`, and RR 119 returned 36 rows.

**The contract goes from 1.0 to 2.0**:

| 1.1.0 (contract 1.0) | 2.0 (contract 2.0) |
|---|---|
| `ano`, `uf`, `nivel`, `tema`, `localidade` | the same; `localidade` is the name as read, with the reading noise |
| `uf_cod`, `localidade_cod` | removed: 1985 municipalities do not map 1:1 to today's codes |
| `categoria`, `variavel` | `coluna_nome` (confirmed only), `coluna_nome_lido`, `coluna_nome_status` and `variavel` |
| `valor` | `valor` (confirmed only) and `valor_lido` |
| `unidade` | `unidade` (only with a confirmed leaf) and `unidade_lida` |
| `confianca` | the cell's `status`, with the precision measured in the contract |
| `fonte` | removed; provenance is in `MetaInfo` (the IBGE PDF and its SHA-256) |
| — | new: `volume`, `tabela`, `pagina_pdf`, `pagina_impressa`, `linha`, `coluna`, `marcador` and `reparado` |

Contract 1.0 remains at `agrobr.contracts.ibge.IBGE_CENSO_AGRO_MUNICIPAL_V1`, the same import as in 1.1.0, now with a
`DeprecationWarning`.

**Anyone who read the package CSVs directly** (`agrobr/data/censo_1985/tab_067.csv` to `tab_119.csv` and `_index.csv`): they are
removed. The package is now a Parquet with 1 row per cell, `cobertura.parquet` and `manifesto.json`, which are internal: read
through the API or the `censo_agropecuario_municipal_1985` dataset.

See [contract 2.0](../contracts/censo_agropecuario_municipal_1985.en.md).

## 82. CLI: invalid `--formato` exits with an error, and Windows CSV is UTF-8

In 1.1.0, `-o`/`--formato` with a value outside the list (`-o xml`) exited with code 0 and the table, and `health --output`
did the same with the text. In 2.0, the 8 data commands (the 7 from 1.1.0 plus `conab levantamentos`, which gains `--formato`) accept only `table`, `csv` and `json` in `--formato`, and
`health`, `doctor` and `snapshot list` only `text` and `json` ([§92](#92-cli-formato-in-every-command-and-snapshot-use-removed)): any other value exits with code 2, the message on standard error and an empty
standard output. Scripts that checked the exit code now see the error.

On Windows, the 1.1.0 CSV had `\r\r\n` on every line and, redirected to a file or pipe, was cp1252: the default
`pandas.read_csv` failed on "Paranaguá", and `csv.reader` saw blank lines in between. In 2.0, the CLI standard output is
UTF-8, and the CSV has one line break per row (`\r\n` on Windows, `\n` on Linux and macOS). On Linux and macOS, the CSV
bytes do not change.

## 83. Cache: the CEPEA policy only, and `load_baseline_fingerprint` is gone

In 1.1.0, `agrobr.cache.get_policy` had 21 policies (CEPEA, IBGE, CONAB, BCB, ComexStat, INMET, ANDA, NASA POWER, and
Notícias Agrícolas) and silently returned CEPEA's for a source outside the table or any text. Only CEPEA's indicator cache
expires by it; the other sources have no such cache (see section 57). In 2.0, `POLICIES` and `SOURCE_POLICY_MAP` keep only
CEPEA (`cepea_diario`), and `get_policy`, `calculate_expiry`, and `get_next_update_info` for another source raise
`InvalidParameterError` ("não tem cache no agrobr"), listing the sources that have one. CEPEA's `endpoint="semanal"` moves
to the daily policy, the one the cache uses for every product. `agrobr doctor` shows the expiry for CEPEA only, in the text
and in the `--formato json` `cache_expiry`; before, 1 line per source, with a TTL for sources without a cache.

`load_baseline_fingerprint` and `save_baseline_fingerprint` leave `agrobr.cepea.parsers`, unused in agrobr. To write and
read a `Fingerprint` as JSON: `fingerprint.model_dump(mode="json")` and `Fingerprint.model_validate(data)`
(`agrobr.models`).

## 84. Desmatamento: pagination, cut and cost of the default call

In 1.1.0, `desmatamento.prodes`, `deter`, `prodes_geo` and `deter_geo` made 1 WFS request with up to 50,000 features
(10,000 in the `_geo` functions). In 2.0, agrobr paginates:

- `tamanho_pagina`: 500 features per page (100 in `prodes_geo` and `deter_geo`), up to 2,000 (500 in the `_geo` functions);
- `max_registros`: 50,000 (10,000 in the `_geo` functions); `max_registros=None` reads the whole selection;
- each request waits 2 s (agrobr's pace for TerraBrasilis). Without filters, the default call makes up to 100 pages, over
  3 min of waiting alone. Examples: 497 s for `prodes(bioma="Amazônia")`, 187 s for `prodes_geo` for Cerrado,
  MT, 2023, and over 600 s for `deter_geo` for the Amazon in PA. With `tamanho_pagina=2000`,
  the Amazon PRODES dropped to 142 s.

**The cut.** When the selection exceeds `max_registros`, agrobr reads the prefix in ascending `fid` (PRODES) or `gid`
(DETER) order, not by date, and raises a `UserWarning` ("retornadas N de M ocorrências por limite local"). The 50,000 of the
default call are only part of the layer: on 2026-09-27, there were 802,281 features in the Amazon PRODES and 460,092 in DETER.

**The recipe.** `ano` (PRODES), `uf`, `inicio`, `fim` and `classe` (DETER) go into the server's CQL filter and
reduce the pages: filter before raising the limit. Raise `tamanho_pagina` to the maximum and use `max_registros=None` only
with filters. See the [source page](../sources/desmatamento.en.md).

## 85. Parameter names: one vocabulary across the API

In 2.0, the same concept has the same name in every function: `inicio`/`fim` for a date range, `ano_inicio`/`ano_fim` for a
year range, `uf` for the state, `produto` for the product and `municipio` for the municipality (section 86). The old names
raise `TypeError`, naming the rejected argument also in `desmatamento.*`, `datasets.desmatamento` and
`datasets.zoneamento_agricola`, which accept `**kwargs`. In `zarc.zoneamento`, `mapbiomas.cobertura` and
`mapbiomas.transicao`, they raise `InvalidParameterError` naming the rejected argument. Positional calls are unchanged, except where section 88 says otherwise.

| Function | 1.1.0 | 2.0 |
|---|---|---|
| `bcb.sgs`, `bcb.ptax` | `data_inicial`, `data_final` | `inicio`, `fim` |
| `bcb.focus` | `data_inicial` | `inicio` |
| `cftc.cot` | `commodity`, `start`, `end` | `produto`, `inicio`, `fim` |
| `datasets.posicionamento_fundos` | `start`, `end`, `combined` | `inicio`, `fim`, `combinado` |
| `desmatamento.deter`, `deter_geo`, `datasets.desmatamento` | `data_inicio`, `data_fim` | `inicio`, `fim` |
| `mapbiomas_alerta.alertas`, `alertas_geo` | `start_date`, `end_date` | `inicio`, `fim` |
| `alt.antt_pedagio.fluxo_pedagio` | `data_inicio`, `data_fim` (development versions) | `inicio`, `fim` |
| `conab.serie_historica`, `datasets.serie_historica_safra` | `inicio`, `fim` (years) | `ano_inicio`, `ano_fim` |
| `normalize.lista_safras` | `inicio`, `fim` (crop years) | `safra_inicio`, `safra_fim` |
| `conab.progresso_safra` | `cultura`, `estado` | `produto`, `uf` |
| `datasets.progresso_safra` | `estado` | `uf` |
| `mapbiomas.cobertura`, `mapbiomas.transicao`, `datasets.uso_do_solo` | `estado` | `uf` |
| `conab.custo_producao`, `custo_producao_total` | `cultura` | `produto` |
| `usda.psd` | `commodity` | `produto` |
| `alt.mapa_psr.apolices`, `sinistros` | `cultura` | `produto` |
| `zarc.zoneamento`, `datasets.zoneamento_agricola` | `cultura` | `produto` |
| `alt.sicar.imoveis`, `imoveis_geo`, `imoveis_geo_stream`, `resumo` | `cod_municipio` | `municipio` |
| `alt.sicar.imoveis_geo` | `max_features` | `max_registros` |
| `ana.hidrografia`, `pivos_irrigacao`, `demanda_irrigacao`, `disponibilidade_hidrica` and the 4 `_geo` functions | `max_features` | `max_registros` |
| `datasets.oferta_demanda_global` | `country`, `market_year`, `attributes`, `pivot` | `pais`, `ano_comercial`, `atributos`, `pivotar` |
| `datasets.comercio_internacional` | `reporter`, `partner`, `freq` | `declarante`, `parceiro`, `frequencia` |

`inicio` and `fim` accept `date`, `datetime` (the time is dropped) and `YYYY-MM-DD` text. `DD/MM/YYYY` text
(`"01/02/2024"` is 1 February) works in BCB, B3, CFTC, Desmatamento, MapBiomas Alerta, the ANTT flow and their datasets;
in `cepea.indicador`, `datasets.preco_diario`, `alt.anp_diesel.*`, `datasets.precos_diesel`, `inmet.estacao`,
`inmet.historico_periodo`, station-mode `datasets.clima` and `nasa_power.clima_ponto`, it raises `InvalidParameterError`.
Before, each source accepted its own format. The `ano_inicio`/`ano_fim` filter of the historical series rejects
wrong types and inverted ranges before the network.

Names that are a source's technical term, or that have no 2.0 counterpart, stay: in the Comtrade source, `reporter`,
`partner`, `freq` and `require_complete`; in the USDA source, `country`, `market_year`, `attributes` and `pivot`, and the
English columns; `parameters` in NASA POWER; `year` in ANEC; `lat`/`lon`; `sources` in MapBiomas Alerta; `especie` in
`ibge.ppm` and `ibge.abate`; `setor` in `ibge.pib_agro`; `data` (a single day) in `bcb.ptax`; `nivel="estado"` in
MapBiomas; `combined` in `cftc.cot`, the COT Disaggregated Combined report term (in the dataset, `posicionamento_fundos(..., combinado=True)`); and `cultura` in the pesticide functions (AGROFIT), where the product is the pesticide and the crop is the target.
The `cultura` output column of ZARC, PSR and the CONAB costs also stays.

**Provenance.** In SGS and PTAX, `source_details["query"]["defaulted_fields"]` lists `inicio`/`fim` instead of
`data_inicial`/`data_final`. In SICAR, the `source_details["sicar"]["max_features"]` key becomes `max_registros`. Code that
reads those keys must use the new name.

## 86. Municipality: full name or IBGE code

`municipio` accepts the 7-digit IBGE code (`int` or text) or the municipality's **full name**, ignoring case, accents and
repeated spaces; known former names (`"Açu"`) map to the current one. A partial name, a name shared by more than one
municipality without `uf`, a name from another state and a code outside the registry raise `InvalidParameterError` listing
the candidates, before the network. The lookup is the one in `normalize.resolver_municipio(valor, uf=None)`, new in 2.0,
which returns `codigo_ibge`, `nome` and `uf`.

It applies to `alt.sicar` (the 4 functions) and `datasets.cadastro_rural`, `mapbiomas.cobertura` and the municipal
`datasets.uso_do_solo`, `alt.mapa_psr.apolices`, `sinistros` and `datasets.seguro_rural`, `zarc.zoneamento` and
`datasets.zoneamento_agricola`, and `alt.anp_diesel.precos_diesel` and `datasets.precos_diesel`. SICAR's `cod_municipio`
filter, which already existed in 1.1.0, is gone, and so are the ones only the 2.0 development versions had (`cod_municipio`
in `cadastro_rural`, `geocodigo` in MapBiomas and `cd_ibge` in PSR): the code goes in `municipio`.

**Silent change.** In 1.1.0, the name matched a substring. It now matches the whole name, and the same call can return a
different cut without an error:

| Source | 1.1.0 | 2.0 |
|---|---|---|
| SICAR | substring of the name, with the exact accent (`ILIKE '%nome%'`) | the municipality with the exact name; where the state has the exact name and others containing it, only the exact one |
| MapBiomas | `municipio="Pinheiro"` returned the 7 municipalities with "pinheiro" in the name | only Pinheiro (MA) |
| PSR and `seguro_rural` | substring of the published label, in any state | the municipality code: policies labelled with a district name come in (Caxias do Sul, 2024: 274 → 693 policies) and namesakes in other states go out |
| ZARC | substring of the name | the full name: `municipio="herval"` returned Santa Maria do Herval and now returns Herval (RS, 4307104) |
| ANP | the spreadsheet name, in any state | the IBGE code or name, with the municipality's state in the filter: `"Santana do Livramento"` becomes `"Sant'Ana do Livramento"` or `4317103` |

**What to do:** use the IBGE code or the full name, with `uf` when the name repeats. To find the name from a fragment,
`normalize.buscar_municipios("sorr", uf="MT")`. For the old PSR behaviour (published label), filter the `municipio` column of
the result.

## 87. Renamed output columns

**Silent change for exporters.** Code that reads the column by its old name gets `KeyError`; code that only writes the frame
(`to_csv`, Parquet, a database) or iterates the columns gets a different schema, without an error.

**`estado` → `uf`.** In `conab.progresso_safra` and `datasets.progresso_safra` (contract `progresso_safra` 2.0), and in
`mapbiomas.cobertura`, `mapbiomas.transicao` and `datasets.uso_do_solo`, state and municipal (contracts `mapbiomas_cobertura`
2.0, `mapbiomas_transicao` 2.0 and `mapbiomas_cobertura_municipal` 1.1), including the primary key. In the progress data,
`MEDIA_ESTADOS` and `BR` remain as values. `queimadas.focos` keeps the `estado` column, with the state name published by
INPE, next to `uf`, with the code.

**`datasets.posicionamento_fundos`, contract 2.0.** The `cftc.cot` source keeps the CFTC report names; the dataset is in
Portuguese. The map is `agrobr.contracts.datasets.POSICIONAMENTO_FUNDOS_COLUNAS_V2` (old name → new).

| 1.1.0 | 2.0 |
|---|---|
| `commodity` | `produto` |
| `open_interest` | `posicoes_abertas` |
| `managed_money_long` / `_short` / `_spread` / `_net` | `fundos_compra` / `fundos_venda` / `fundos_spread` / `fundos_saldo` |
| `producer_long` / `_short` | `produtores_compra` / `produtores_venda` |
| `swap_long` / `_short` | `swap_compra` / `swap_venda` |
| `other_long` / `_short` | `outros_compra` / `outros_venda` |
| `nonreportable_long` / `_short` | `nao_reportaveis_compra` / `nao_reportaveis_venda` |
| `change_managed_money_long` / `_short` | `variacao_fundos_compra` / `variacao_fundos_venda` |
| `change_open_interest` | `variacao_posicoes` |

`swap_spread` and `outros_spread` are new columns in 2.0 (in the development versions, `swap_spread` and `other_spread`).

**`datasets.oferta_demanda_global`, contract 2.0.** The `usda.psd` source keeps the English columns.

| 1.1.0 | 2.0 |
|---|---|
| `commodity_code` | `codigo_produto` |
| `commodity` | `produto` |
| `country_code` | `codigo_pais` |
| `country` | `pais` |
| `market_year` | `ano_comercial` |
| `attribute` | `atributo` |
| `attribute_br` | `atributo_br` |
| `value` | `valor` |
| `unit` | `unidade` |

`codigo_atributo`, `codigo_unidade`, `ano_atualizacao` and `mes_atualizacao` are new columns in 2.0 (in the development
versions, `attribute_id`, `unit_id`, `last_update_year` and `last_update_month`; section 49).

In pivot mode, the fixed identifiers use the new names, and the attribute columns keep each attribute's label.

**`datasets.comercio_internacional`, contract 3.0.** The `comtrade.comercio` source keeps the technical names.

| 1.1.0 | 2.0 |
|---|---|
| `reporter_code` | `codigo_declarante` |
| `reporter_iso` | `iso_declarante` |
| `reporter` | `declarante` |
| `partner_code` | `codigo_parceiro` |
| `partner_iso` | `iso_parceiro` |
| `partner` | `parceiro` |
| `fluxo_code` | `codigo_fluxo` |
| `hs_code` | `codigo_hs` |
| `produto_desc` | `descricao_produto` |

**`conab.brasil_total`.** `produto` becomes the normalized identifier, without footnotes, and the new `rotulo` column carries
the published text: filtering `produto == "SOJA"` returns nothing, and the right filter is `produto == "soja"`.
`feijao_cores_1`, `_2` and `_3` tell the 3 crops apart.

**What to do:** change the names in the consumer. To read files written before, rename with the map
(`df.rename(columns={new: old ...})`).

## 88. Flags and secondary filters by keyword only

`as_polars`, `return_meta` and the options after them become keyword-only in every source and dataset: by position,
`TypeError`, before any download. Examples: `bcb.credito_rural`, `cepea.indicador` (also `validate_sanity`, `force_refresh`
and `offline`), `comexstat.exportacao`/`importacao`, `conab.safras` (`levantamento` stays in 4th position), `balanco` and
`brasil_total` (also `levantamento`),
`conab.custo_producao`, `inmet.*`, `nasa_power.*` (also `parameters`), `alt.anp_diesel.*`, `alt.antt_pedagio.*`,
`alt.mapa_psr.*` and the matching datasets. In `datasets.cadastro_rural`, the first 7 arguments (`uf` to `criado_apos`) stay
positional.

In IBGE, only the main filters stay positional, the same in the source and the dataset:

| 1.1.0 | 2.0 |
|---|---|
| `ibge.pam("soja", 2023, "MT")` | `ibge.pam("soja", 2023, uf="MT")` |
| `datasets.producao_anual("soja", 2023, "municipio", "MT")` | `datasets.producao_anual("soja", 2023, uf="MT", nivel="municipio")` |
| `ibge.censo_agro("efetivo_rebanho", 2017)` | `ibge.censo_agro("efetivo_rebanho", ano=2017)` |
| `ibge.pib_agro("2024T1")` | `ibge.pib_agro(trimestre="2024T1")` |
| `datasets.leite_industrial("leite", "2024T1")` | `datasets.leite_industrial("2024T1", produto="leite")` |

Annual surveys keep product (or species) and year positional; slaughter, species and quarter; the censuses, only the theme;
milk, only the quarter; GDP, only the sector (source) or the product (dataset). A quarter in the sector position
(`ibge.pib_agro("2024T1")`) raises `InvalidParameterError` listing the valid sectors.

## 89. Output dtypes

2.0 sets one rule for every source: dates as `datetime64[ns]` (an instant with a time zone as `datetime64[ns, UTC]`),
integers by nature (year, code, count) as `Int64`, measures as `float64`, text in the installed pandas default dtype
(`object` on pandas 2, `str` on 3), and the empty result with the same dtypes as the full one. `Column.validate` and
`validate_dataset` require `datetime64` in date columns: convertible text and `date` objects become a contract error. Period
labels (crop year, `YYYY-MM`, quarter) stay as text.

**Silent change: date columns that were text.**

| Function | Column | 1.1.0 | 2.0 |
|---|---|---|---|
| `deral.condicao_lavouras`, `datasets.condicao_lavouras` | `data` | text with the sheet name: `dd-mm-yyyy`, `dd-mm-yy`, `"Atual"` and `"Anterior"` | `datetime64[ns]` with the cell date (contract 2.0) |
| `imea.cotacoes` | `data_publicacao` | `YYYY-MM-DD HH:MM:SS` text | `datetime64[ns]` |
| `inmet.estacoes` | `inicio_operacao`, `DT_FIM_OPERACAO` | ISO text with offset | `datetime64[ns, UTC]`, the same instant |
| `antaq.movimentacao`, `datasets.movimentacao_portuaria` | `data_atracacao` | text | `datetime64[ns]`, with the time (contract 2.0) |
| `alt.antt_pedagio.pracas_pedagio` | `data_da_inativacao` | text, `""` when empty | `datetime64[ns]`, `NaT` when empty (contract 2.0) |

A date that does not exist in the source (such as `31/02/2024`) becomes `NaT`, with a warning in `meta.validation_warnings`;
text that is not a date raises `ParseError`. In FUNAI and INCRA, dates were already `datetime64` in 1.1.0 and stay so: FUNAI's
`data_atualizacao`, which 1.1.0 read month first, is now read day first (section 34), and INCRA's `0001-01-01` placeholder
becomes `NaT` (section 25). In INMET, the same instant in UTC can fall on the next day: for the published local date, use
`.dt.tz_convert("America/Sao_Paulo").dt.date`.

**The text-filter trap.** Comparing a `datetime64` column with `dd/mm/yyyy` text does not raise: pandas reads the text month
first when the day is 12 or less. `df[df.data == "01/02/2026"]` returns 2 January, with no warning. Compare with
`pd.Timestamp("2026-02-01")` or `date(2026, 2, 1)`. For the old text, `df["data"].dt.strftime("%d/%m/%Y")`. Exported, the
text changes too: `to_csv` and `astype(str)` give `2026-09-14`, not `14/09/2026`.

In DERAL, the filter for the current week (`data == "Atual"`) comes back empty, and the filter by sheet name (`"02-10-23"`)
can come back empty or match another date, with no warning: use `df[df.data == df.data.max()]` or `pd.Timestamp`. A published `-` in `pct`, `plantio_pct` and
`colheita_pct` becomes 0 (null in 1.1.0), and a sheet whose name differs from the cell date follows the cell, with a
warning.

**Silent change: integers and measures.**

| Function | Column | 1.1.0 | 2.0 |
|---|---|---|---|
| `ibge.abate`, `datasets.abate_trimestral` | `animais_abatidos` | `float64` | `Int64` (contract 2.0) |
| `bcb.sgs`, `datasets.series_economicas` | `codigo` | `int64` | `Int64` (contract `bcb_sgs` 3.0) |
| `alt.antt_pedagio.pracas_pedagio` | `km_m`, `ano_do_pnv_snv` | text | `float64`, `Int64` |
| `mapbiomas_alerta.alertas`, `alertas_geo` | `alert_code` | `int64` | `Int64` |
| `b3.ajustes`, `b3.historico` | `vencimento_mes`, `vencimento_ano` | `int64` | `Int64` |
| `alt.mapa_psr.*`, `datasets.seguro_rural` | `ano_apolice` | `int64` | `Int64` |
| `conab.brasil_total` | measures | `Decimal`/`object` | `float64` |

IBGE, ANA, SFB and Embrapa Solos codes and years are also `Int64`, full and empty. Nothing is truncated: a fraction in a count
raises `ParseError`.

**Silent change: text.** Text comes in the installed pandas default dtype, not `string[python]`; nulls are `NaN` on pandas 3
and `None` on 2, instead of `pd.NA`. The ComexStat source and dictionaries and `antt_pedagio.fluxo` (contract 3.0) keep
`string[python]` on purpose, because the memory cap of those queries counts each stored text. CEPEA's `anomalies` column is
`object`, with the JSON or `None` (`String` or `null` in Polars). `ibge.pib_agro` returns `precos` in canonical form
(`"corrente"`, not `" CORRENTE "`).

**Silent change: the empty result.** A query with no rows returns the contract's columns and dtypes, no longer all `object`:
B3, MapBiomas Alerta (with CRS `EPSG:4326` in `_geo`), `ibama.embargos_geo`, `icmbio.ucs_geo`, IBGE, CONAB and the others.
`ibge.pam` always returns the 14 declared columns, with the measures not requested as nulls.

**What to do:** test the dtype family with `pd.api.types` (`is_integer_dtype`, `is_string_dtype`,
`is_datetime64_any_dtype`), nulls with `isna()`, not `is pd.NA` or `== "NULL"`; compare dates with `Timestamp` or `date`; to
get `int64`, `astype("int64")` when there are no nulls.

**Contracts whose version changes under these rules:**

| Contract | 1.1.0 | 2.0 |
|---|---|---|
| `abate_trimestral` | 1.0 | 2.0 |
| `antt_pedagio_pracas` | 1.0 | 2.0 |
| `condicao_lavouras` | 1.0 | 2.0 |
| `movimentacao_portuaria` | 1.0 | 2.0 |
| `oferta_demanda_global` | 1.0 | 2.0 |
| `posicionamento_fundos` | 1.0 | 2.0 |
| `comercio_internacional` | 1.0 | 3.0 |
| `bcb_sgs` | — | 3.0 |
| `embrapa_solos_perfis` | — | 3.0 |

For contracts that existed in 1.1.0, the old constant keeps the previous schema (except `POSICIONAMENTO_FUNDOS_V1`,
which moves to 1.1, with `swap_spread` and `other_spread`), and the new one has another name:
`IBGE_ABATE_V2` (`agrobr.contracts.ibge`), `ANTT_PEDAGIO_PRACAS_V2`, `CONDICAO_LAVOURAS_V2`, `MOVIMENTACAO_PORTUARIA_V2`,
`OFERTA_DEMANDA_GLOBAL_V2` and `POSICIONAMENTO_FUNDOS_V2` (`agrobr.contracts.datasets`), `COMERCIO_INTERNACIONAL_V3`
(`agrobr.contracts.comtrade`). `bcb_sgs`, which had no contract in 1.1.0, is `BCB_SGS_V3` (`agrobr.contracts.bcb_sgs`). The registry
(`get_contract(name)`) always returns the current one.

## 90. Errors: the class tells the cause

**A layout failure in every source of a dataset raises `ParseError`**, with `errors`, `attempted_sources` and the original
cause in `__cause__`, no longer `SourceUnavailableError`. A mixed failure (network in one source, layout in another) is still
`SourceUnavailableError`. A programming error (an internal `TypeError`, for example) propagates as is, without triggering the
fallback. Code that used `except SourceUnavailableError` for "the source failed" must handle `ParseError` separately, or
`AgrobrError` at the application boundary.

**An unknown name in a catalog raises `UnknownNameError`**, a subclass of both `InvalidParameterError` and `KeyError`,
listing the valid names: `datasets.get_dataset`, `contracts.get_contract`, `normalize.uf_para_nome`, `uf_para_regiao` and
`uf_para_ibge`. `except KeyError` and `except ValueError` still catch it. The exception is exported from `agrobr`.

**Invalid input raises `InvalidParameterError` before the network**, listing the valid values, where 1.1.0 returned empty
data, unfiltered data or a raw error (`ValueError`, `TypeError`, `AttributeError`). The rule in section 66 now applies to
every source. Examples: a contract outside `b3.contratos()`, a product without a CFTC contract, an unknown IMEA chain, a
`uf` or `tipo` outside the `inmet.estacoes` catalogue, a product outside the DERAL spreadsheet, a malformed UNICA crop year,
a text `ano` in ABIOVE, an unknown Notícias Agrícolas product, an out-of-range year in USDA, Comtrade and ANDA, ANTAQ filters,
ANTT's `tipo_veiculo`, `evento` with `tipo="apolices"` in `seguro_rural`, a non-text `uf`, an out-of-range `bbox`, an invalid
crop year in `normalize` and `max_pages` ≤ 0 in the progress weeks. The fire satellite and the MapBiomas class are checked
after the download, against what the file publishes. `InvalidParameterError` is a `ValueError` subclass: code that checked
`df.empty` after user input must catch the exception. In `normalize.municipio_para_ibge`, a name shared by more than one municipality without `uf` raises the same error as `resolver_municipio`, listing the candidates; 1.1.0 returned the code of the first one on the list.

**A malformed response raises `ParseError`**, no longer an empty result that looks like "no data": an ArcGIS count without
`count` (ANA and SFB), a SICOR envelope without `value`, CEASA prices without `resultset`, an ANEC page without the article
list, an empty IMEA list, an INMET catalogue that is not a list, a MapBiomas Alerta `alerta_info` without the period, a CONAB
totals sheet without a header, a corrupted structural baseline and a CEPEA consensus without records. Legitimate empties
(`count` zero, `value=[]`, a day without prices) stay empty. A requested field missing from an ArcGIS layer (ANA and SFB) also raises `ParseError`, naming the field; before, the column was dropped from the result without an error.

**INMET without observations.** `inmet.estacao` and `inmet.clima_uf` with no observation in the period raise
`SourceUnavailableError`, no longer `ParseError`. Code that caught `ParseError` for "no data" must change the class.

**Messages.** "UF invalida" becomes "UF inválida: 'XX'. Valores válidos: AC, AL, …", and "after N retries" becomes
"after N attempts". Code that matched the text must change the term.

**New warnings**, as `UserWarning` and in `meta.validation_warnings`: an empty SIDRA query (on every call), an ambiguous
creation year in SFB's CNFP, an ANTT plaza filter without results, identical repeated rows in ANP (removed), an Embrapa
`ordem` without a match in a complete read and a mistyped date in the source (it becomes `NaT`). Code that treats warnings as
errors must accommodate them.

## 91. Environment variables

**Silent change.** The [environment variables page](../advanced/ambiente.md) lists all of them, with the default and the
accepted values.

- **Cache folder.** In 1.1.0, only `AGROBR_CACHE_CACHE_DIR` worked; `AGROBR_CACHE_DIR` was ignored, and an empty variable
  pointed to the current directory. In 2.0, `AGROBR_CACHE_DIR` is the main name and the old one stays as an alias; when both
  are set and differ, the new one wins, with a `UserWarning`; empty means the default `~/.agrobr/cache`. Anyone who already
  had `AGROBR_CACHE_DIR` set to another folder now writes the cache there. Set only one; prefer `AGROBR_CACHE_DIR`.
- **Disabled cache.** `AGROBR_ANEC_CACHE_DISABLED` and `AGROBR_ACERVO_FUNDIARIO_CACHE_DISABLED` accept `1`, `true`, `yes` and
  `on` (disable) and `0`, `false`, `no`, `off` and empty (keep), case-insensitive. In 1.1.0, only `1` disabled the cache, and
  `true` was ignored. Any other value (`sim`) raises `InvalidParameterError`.
- **`AGROBR_HTTP_MAX_RETRIES`** counts the total attempts per request, as it always did: the default 3 makes up to 3
  requests. `0` now makes 1 request, without retries (in 1.1.0, it broke every request), and a negative value is rejected with
  `ValidationError` when the settings load.
- **`AGROBR_HTTP_MAX_CONCURRENT_<SOURCE>`** below 1 is rejected with `ValidationError` (in 1.1.0, `0` hung the call).
- **`AGROBR_HTTP_RATE_LIMIT_<SOURCE>`** and a `rate_limit_*` passed to `HTTPSettings` reject negative values, `inf` and `nan` with `ValidationError` (1.1.0 accepted them); `0` is still allowed.

In code, the same applies to `retry_async` and `with_retry`: `max_attempts=0` makes 1 attempt, and a `0` delay is a zero delay
(before, both became the defaults); for the default, pass `None` or omit it. `run_all_checks` with `concurrency` below 1,
`generate_report` with a format other than `json`, `html` and `md`, `send_alert` with an invalid `level`, `benchmark_*` with
`iterations` below 1 and `cache.get_policy` with an unknown `endpoint` raise `InvalidParameterError`.

## 92. CLI: `--formato` in every command and `snapshot use` removed

The [CLI reference](../advanced/cli.md) lists every command and option.

- `health`, `doctor` and `snapshot list` use `--formato text|json` (`-o`), as the data commands use
  `--formato table|csv|json`. `health --output json`, `doctor --json` and `snapshot list --json` exit with code 2: use
  `--formato json`.
- `snapshot use` is gone: the command activated nothing. Deterministic mode is configured in code, per process
  (`async with datasets.deterministic("YYYY-MM-DD")`), and a snapshot is read with
  `snapshots.load_from_snapshot(..., snapshot_name=...)`.
- **Silent change:** `ibge produtos --pesquisa PAM` queried LSPA (the upper case fell through to the default). In 2.0, `pam`
  and `lspa` are case-insensitive, and any other name exits with code 2. Anyone who used `PAM` to get LSPA must pass
  `--pesquisa lspa`.
- An invalid year, an unknown survey and an unknown source exit with code 2 before the query; a collection failure still
  exits with code 1. Logs are human-readable on standard error (`WARNING`; `INFO` with `--verbose`), and JSON and CSV are
  alone on standard output.
- New options: `cepea indicador --praca`, `conab safras --levantamento` and `conab balanco --safra/--levantamento`.
- **Silent change:** `agrobr conab levantamentos` lists every survey, not just the first 10, and the `Listando levantamentos...` notice goes to standard error. To read the list from a script, use `--formato csv` or `--formato json`.

## 93. CONAB: one path per function

The `agrobr.conab.custo_producao` and `agrobr.conab.serie_historica` subpackages are gone; the names of the same name in
`agrobr.conab` are the functions. `import agrobr.conab.custo_producao` and
`from agrobr.conab.serie_historica import produtos_disponiveis` raise `ModuleNotFoundError`: use
`from agrobr.conab import custo_producao, serie_historica` and `conab.produtos_serie_historica()`, which lists the products,
categories and URLs of the series without a request. `agrobr.conab.ceasa` keeps an empty `__all__`
(`from agrobr.conab.ceasa import *` exports nothing): use `conab.ceasa_precos`, `ceasa_produtos`, `ceasa_categorias` and
`lista_ceasas`.

## 94. Heavy downloads and cache

- **Silent change:** `ibama.embargos` and `embargos_geo` keep the CSV (~208 MB) in the cache folder for 1 hour. Within the
  hour, the same call returns the previous collection, with `meta.from_cache=True` and `meta.fetched_at` at the collection
  time; concurrent calls download once. `use_cache=False`, a new argument, downloads again without reading or writing the
  cache.
- `acervo_fundiario` with `use_cache=False` or `AGROBR_ACERVO_FUNDIARIO_CACHE_DISABLED` no longer writes anything to the
  cache: the ZIP (up to 766 MB per state) goes to a temporary folder, deleted at the end of the query. In 1.1.0, the option
  only skipped the read.
- `b3.historico` downloads at most `AGROBR_HTTP_MAX_CONCURRENT_B3` days at a time (default 3), instead of one download per
  business day at once. A long period is slower at the peak; raise the variable for more parallelism.
- `validators.save_baseline`, `load_baseline` and `validate_against_baseline` write and read in `structures` inside the cache
  folder, no longer in `.structures` in the current directory. For the old path, pass `baselines_dir=".structures"`.

## 95. Returns are copies

**Silent change.** `datasets.get_dataset`, `contracts.get_contract`, `datasets.list_products` and `datasets.info`,
`normalize.ibge_para_municipio`, `buscar_municipios`, `coordenada_para_municipio` and `listar_ufs`,
`health.get_affected_datasets` and the ANEC articles return copies. Mutating the returned object no longer changes the
catalog or the next call. Code that used mutation as global configuration must use the returned instance instead: for
example, `get_dataset(name)` and its `fetch`.

In `utils.parse_links_from_html` with `base_url`, a relative `href` without a slash (`"arquivo.pdf"`) now comes back as an
absolute URL; before, it came back unchanged.

## 96. What is public API

2.0 declares what follows semantic versioning: the source functions, `datasets`, `contracts`, the exceptions, `MetaInfo`,
`normalize`, `agrobr.sync` and the CLI commands. The full list is on the [public API page](../api/index.md). The rest
(`cache`, `http`, `utils`, `health`, `alerts`, `benchmark`, `validators`, `constants` and each source's internal
subpackages) is internal, except the few names the docs teach and the page lists, and may change in a minor version. No import changes in 2.0; code that uses an internal name should
pin the agrobr version.

## 97. Other per-source changes

- `datasets.clima`: `agregacao` defaults to `None`. In state mode, `None` and `"mensal"` return months, and `"diario"` raises
  `InvalidParameterError` (in 1.1.0, it was the default, accepted and ignored: the output was already monthly); for daily
  data, use station mode. In state mode without `fonte`, a year before 2000 goes straight to NASA POWER, and `fonte="inmet"`
  with such a year raises `InvalidParameterError`.
- `zarc.zoneamento(cultura=...)` raises `InvalidParameterError: Argumentos desconhecidos: ['cultura']`: use `produto=`.
- `imea.cotacoes` accepts the crop year as `"2024/25"` and `"2024/2025"`, besides `"24/25"`; `unica.producao_historica`, as
  `"2018/19"` and `"18/19"`, besides `"2018/2019"`. `deral.condicao_lavouras` accepts agrobr's product synonyms, and
  `datasets.condicao_lavouras` accepts `"milho"` and `"feijao"` (both crops) and declares `as_polars`.
- `datasets.preco_diario` declares `as_polars`.
- ANP: identical weekly rows are removed, with a warning and the count in `meta.validation_warnings`; conflicting values still
  raise `ParseError`.
- ANP, `vendas_diesel`: **silent change.** `produto` uses the `precos_diesel` label (`DIESEL S-10` becomes `DIESEL S10`),
  and the join by product between sales and prices matches; the other fuels (`DIESEL S-500`, `DIESEL S-1800`,
  `DIESEL MARÍTIMO`, `DIESEL (OUTROS )`) stay as published. `regiao` comes with the canonical name: `REGIÃO CENTRO-OESTE`
  becomes `Centro-Oeste` (likewise `Norte`, `Nordeste`, `Sudeste` and `Sul`). Code that filtered by the published text
  changes the value; volumes do not change.
- ANP, `precos_diesel`: a period starting after today raises `InvalidParameterError` before the network, at every level
  (in 1.1.0, an empty result with no warning for a state or Brazil); for a municipality, a bound outside 2022 through the
  current year too. A year inside the range whose file ANP has not published yet remains `SourceUnavailableError`.
- Embrapa Solos: `mapa_solos` and `mapa_solos_geo` match `ordem` against the whole `ordem1` class (one of the 15
  published), accepting case, accents and the singular (`"latossolo"`). A name fragment (`"latos"`) no longer matches and
  raises `InvalidParameterError` with the list of classes: use the class name (`"latossolos"`).
- ANTT: `rodovia` compares ignoring case, spaces, hyphens and leading zeros (`"BR 40"`, `"br-040"`); `tipo_veiculo` and
  `tipo_cobranca`, ignoring case and accents.
- RNC and cultivars: text filters ignore case and accents (`especie="feijao"` finds `"Feijão"`).
- Fires: `satelite` is case-insensitive.
- Default years and year bounds follow the Brasília calendar, not the machine clock, in SGS, PTAX, fires, PRODES, USDA,
  Comtrade, ComexStat, ANEC, ANTT and ANP. On a UTC server, from 21:00 to 24:00 on 31 December, the next year is no longer
  accepted.
- `defensivos`: the cache format moves to 2; the first query downloads again.
- SICAR, `resumo(uf)` without a municipality: the count is of published features, and versions of the same `cod_imovel` count separately; `MetaInfo` carries `source_details["sicar"]["unidade"] = "feicoes_publicadas"` and a warning in `validation_warnings`. To count properties, use the municipality summary.
- SFB, IFN: `sfb.ifn_conglomerados` and its `_geo` read the active IFN layers (the `Conglomerado` service went offline and both
  functions failed); `lote` comes from a join with the lot registry on `co_lote` (an orphan or duplicate code raises
  `ParseError`), and a `ciclo` column is added (nullable text, IFN schema 1.1): adjust positional selections. The `bioma` filter
  no longer returns empty because of letter case. Per-page provenance is on the [source page](../sources/sfb.md).

## 98. B3: `oi_historico` is renamed `posicoes_abertas_historico`

`b3.oi_historico`, from 1.1.0, is renamed `b3.posicoes_abertas_historico`, also in `sync.b3`. The options do not
change; only `**kwargs` is removed (section 78), and the old name no longer exists (`AttributeError`):

```python
df = await b3.posicoes_abertas_historico(contrato="boi", inicio="2026-09-01", fim="2026-09-04")
```

The dataset still uses `datasets.futuros_agricolas(..., tipo="oi_historico")`: the `tipo` values do not change. Columns,
units and the policy for days without a file also stay.

## 99. CFTC: a period without reports returns a typed empty frame

`cftc.cot` for a period without reports returns an empty DataFrame, with the columns and dtypes of a populated one, no
longer `SourceUnavailableError`. Code that used the exception to detect "no observations" should check `df.empty`
(`is_empty()` in Polars); transport failures remain `SourceUnavailableError`, and a response outside the format, `ParseError`.
The 18 counts and changes come as `Int64`, in empty and populated results (in 1.1.0, the 13 counts were `int64` and the 3 changes were
already `Int64`; `swap_spread` and `other_spread` are new). Above 50,000 records, the
query warns that completeness was not proven (`UserWarning` and `meta.validation_warnings`): shorten the period.
