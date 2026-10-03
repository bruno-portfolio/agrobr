# Public API

This page states what agrobr guarantees across releases. What is listed here follows
[SemVer](../contracts/semver.md): name, parameters, return type and exceptions only break in a major release, with the change
in the CHANGELOG and in the [migration guide](../guides/migracao-2.md). The rest of the package is internal and may change in
any release, minor or patch included. If you use an internal name, pin the agrobr version.

There is a single rule: what the documentation teaches you to import is public, and this page names each case.

## Package root

The names in `agrobr.__all__`:

- the source modules, `datasets`, `contracts` and `deterministic`;
- the exceptions `AgrobrError`, `CacheMigrationError`, `ContractViolationError`, `InvalidParameterError`, `ParseError`,
  `ResourceLimitError`, `SnapshotError`, `SourceUnavailableError` and `UnknownNameError`;
- `MetaInfo` and `__version__`.

## Sources

Everything in the `__all__` of the source module. The public path is the module's (`agrobr.<source>` or
`agrobr.alt.<source>`); the submodules and subpackages inside a source (`client`, `parser`, `models`, `parsers`, `cache`,
`agrobr.conab.progresso`, `agrobr.incra.andamento`...) are internal, even when they expose the same function.

| Module | Page |
|---|---|
| `agrobr.abiove` | [ABIOVE](abiove.md) |
| `agrobr.acervo_fundiario` | [Acervo Fundiário](../sources/acervo_fundiario.md) |
| `agrobr.alt.anp_diesel` | [ANP Diesel](anp_diesel.md) |
| `agrobr.alt.antt_pedagio` | [ANTT Pedágio](antt_pedagio.md) |
| `agrobr.alt.mapa_psr` | [MAPA PSR](mapa_psr.md) |
| `agrobr.alt.sicar` | [SICAR](sicar.md) |
| `agrobr.ana` | [ANA](../sources/ana.md) |
| `agrobr.anda` | [ANDA](anda.md) |
| `agrobr.anec` | [ANEC](anec.md) |
| `agrobr.antaq` | [ANTAQ](antaq.md) |
| `agrobr.b3` | [B3](b3.md) |
| `agrobr.bcb` | [BCB](bcb.md) and [economic series](series_economicas.md) |
| `agrobr.bruto` | [Raw collection](bruto.md): original files and pages with a manifest |
| `agrobr.cepea` | [CEPEA](cepea.md) |
| `agrobr.cftc` | [CFTC](cftc.md) |
| `agrobr.cnuc` | [CNUC](../sources/cnuc.md) |
| `agrobr.comexstat` | [ComexStat](comexstat.md) |
| `agrobr.comtrade` | [UN Comtrade](comtrade.md) |
| `agrobr.conab` | [CONAB](conab.md), [progress](conab_progresso.md) and [CEASA](conab_ceasa.md) |
| `agrobr.defensivos` | [Pesticides](defensivos.md) |
| `agrobr.deral` | [DERAL](deral.md) |
| `agrobr.desmatamento` | [Deforestation](desmatamento.md) |
| `agrobr.embrapa_solos` | [Embrapa Solos](embrapa_solos.md) |
| `agrobr.funai` | [FUNAI](../sources/funai.md) |
| `agrobr.ibama` | [IBAMA](../sources/ibama.md) |
| `agrobr.ibge` | [IBGE](ibge.md) |
| `agrobr.icmbio` | [ICMBio](../sources/icmbio.md) |
| `agrobr.imea` | [IMEA](imea.md) |
| `agrobr.incra` | [INCRA](../sources/incra.md) |
| `agrobr.inmet` | [INMET](inmet.md) |
| `agrobr.lista_suja` | [Lista Suja](../sources/lista_suja.md) |
| `agrobr.mapbiomas` | [MapBiomas](mapbiomas.md) |
| `agrobr.mapbiomas_alerta` | [MapBiomas Alerta](../sources/mapbiomas_alerta.md) |
| `agrobr.nasa_power` | [NASA POWER](nasa_power.md) |
| `agrobr.noticias_agricolas` | [Notícias Agrícolas](noticias_agricolas.md) |
| `agrobr.queimadas` | [Fire hotspots](queimadas.md) |
| `agrobr.rio_verde` | [Fundação Rio Verde](rio_verde.md) |
| `agrobr.rnc` | [RNC](rnc.md) |
| `agrobr.sfb` | [SFB](../sources/sfb.md) |
| `agrobr.unica` | [UNICA](unica.md) |
| `agrobr.usda` | [USDA](usda.md) |
| `agrobr.zarc` | [ZARC](zarc.md) |

`agrobr.alt` is the namespace of the 4 `alt` modules.

## Datasets, contracts, exceptions and normalization

| Module | What is public |
|---|---|
| `agrobr.datasets` | the `__all__`: the datasets ([overview](../contracts/index.md)) and the catalogue (`list_datasets`, `list_products`, `info`, `describe`, `describe_all`, `get_dataset`), `deterministic`, `is_deterministic` and `get_snapshot` |
| `agrobr.contracts` | the `__all__`: `get_contract`, `list_contracts`, `has_contract`, `validate_dataset`, `register_contract`, `generate_json_schemas`, `Contract`, `Column`, `ColumnType` and `BreakingChangePolicy` |
| `agrobr.exceptions` | the `__all__`: the root exceptions plus `FingerprintMismatchError`, `NetworkError`, `ValidationError`, `SourceFallbackWarning` and `StaleDataWarning` |
| `agrobr.models` | `MetaInfo` and `Fingerprint` |
| `agrobr.normalize` | the `__all__` ([normalization](../guides/normalizacao.md)) |

The public path to a contract is its registered name: `contracts.get_contract("<name>")`. The constants the documentation
teaches you to import are public too: `BCB_FOCUS_V2` (`agrobr.contracts.bcb_focus`), `BCB_PTAX_V2` and `BCB_PTAX_MOEDAS_V1`
(`agrobr.contracts.bcb_ptax`), `BCB_SGS_V3` (`agrobr.contracts.bcb_sgs`), `CLIMA_V3` (`agrobr.contracts.clima`),
`COMERCIO_INTERNACIONAL_V3` (`agrobr.contracts.comtrade`), `CONAB_CUSTOS_V3` (`agrobr.contracts.conab_custos`),
`LISTA_SUJA_EMPREGADORES_V2` (`agrobr.contracts.lista_suja`) and `POSICIONAMENTO_FUNDOS_COLUNAS_V2`
(`agrobr.contracts.datasets`). The other `*_V<n>` constants mentioned in the contract pages identify the version; read the
contract through `get_contract`. The historical constants mentioned in the migration guide are not public; the
renamed ones (such as `agrobr.contracts.datasets.CLIMA_V2`) emit a `DeprecationWarning`.

## Sync, snapshots and configuration

| Name | What is public | Page |
|---|---|---|
| `agrobr.sync` | the sync mirror of every public function of the sources and of `datasets` (`agrobr.sync.<source>.<function>`, `agrobr.sync.alt.<source>.<function>`) | [Async](../guides/async.md) |
| `agrobr.snapshots` | the `__all__`: `create_snapshot`, `list_snapshots`, `get_snapshot`, `load_from_snapshot`, `delete_snapshot`, `SnapshotInfo` and `SnapshotManifest` | [Snapshots](../guides/snapshots.md) |
| `agrobr.config` | `set_mode`, `get_config` and `reset_config` | [Snapshots](../guides/snapshots.md) |
| `deterministic_decorator` | the decorator, imported from `agrobr.datasets.deterministic` | [Reproducibility](../advanced/reproducibility.md) |

## Other names taught in the documentation

Outside the modules above, only these:

- `agrobr.http.get_timeout` ([resilience](../advanced/resilience.md));
- `agrobr.validators.sanity.PRICE_RULES` ([resilience](../advanced/resilience.md#statistical-validation));
- `agrobr.defensivos.cache.invalidate` ([pesticides](../sources/defensivos.md));
- `agrobr.normalize.dates.converter_datas` ([normalization](../guides/normalizacao.md));
- `agrobr.normalize.dates.normalizar_safra`, the same object as `normalize.normalizar_safra`
  ([migration guide](../guides/migracao-2.md)).

## CLI and environment variables

The [CLI](../advanced/cli.md) commands and options are public, and so is the exit code. The
[environment variables](../advanced/ambiente.md) are public, except `AGROBR_ALERT_*`.

## Internal

No SemVer guarantee:

- `agrobr.alerts`, `agrobr.benchmark`, `agrobr.cache`, `agrobr.health`, `agrobr.utils` and `agrobr.constants`;
- `agrobr.http`, `agrobr.validators`, `agrobr.models` and `agrobr.config`, except the names listed above;
- the submodules of the sources, of `agrobr.normalize`, of `agrobr.contracts` and of `agrobr.datasets`, except the names
  listed above;
- every name starting with `_`.

Internal names show up in the `__all__` of some packages and sometimes in examples on advanced pages, for diagnostics. Use
them freely, but pin the version: they may change without a deprecation notice.
