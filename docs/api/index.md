# API pública

Esta página diz o que o agrobr garante entre versões. O que está listado aqui segue o
[SemVer](../contracts/semver.md): nome, parâmetros, tipo de retorno e exceções só quebram em versão major, com a mudança no
CHANGELOG e no [guia de migração](../guides/migracao-2.md). O resto do pacote é interno e pode mudar em qualquer versão,
inclusive minor ou patch. Quem usa um nome interno deve fixar a versão do agrobr.

A regra é uma só: o que a documentação ensina a importar é público, e esta página nomeia cada caso.

## Raiz do pacote

Os nomes do `agrobr.__all__`:

- os módulos das fontes, `datasets`, `contracts` e `deterministic`;
- as exceções `AgrobrError`, `CacheMigrationError`, `ContractViolationError`, `InvalidParameterError`, `ParseError`,
  `ResourceLimitError`, `SnapshotError`, `SourceUnavailableError` e `UnknownNameError`;
- `MetaInfo` e `__version__`.

## Fontes

Tudo o que está no `__all__` do módulo da fonte. O caminho público é o do módulo (`agrobr.<fonte>` ou
`agrobr.alt.<fonte>`); os submódulos e subpacotes dentro de uma fonte (`client`, `parser`, `models`, `parsers`, `cache`,
`agrobr.conab.progresso`, `agrobr.incra.andamento`...) são internos, mesmo quando expõem a mesma função.

| Módulo | Página |
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
| `agrobr.bcb` | [BCB](bcb.md) e [séries econômicas](series_economicas.md) |
| `agrobr.cepea` | [CEPEA](cepea.md) |
| `agrobr.cftc` | [CFTC](cftc.md) |
| `agrobr.cnuc` | [CNUC](../sources/cnuc.md) |
| `agrobr.comexstat` | [ComexStat](comexstat.md) |
| `agrobr.comtrade` | [UN Comtrade](comtrade.md) |
| `agrobr.conab` | [CONAB](conab.md), [progresso](conab_progresso.md) e [CEASA](conab_ceasa.md) |
| `agrobr.defensivos` | [Defensivos](defensivos.md) |
| `agrobr.deral` | [DERAL](deral.md) |
| `agrobr.desmatamento` | [Desmatamento](desmatamento.md) |
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
| `agrobr.queimadas` | [Queimadas](queimadas.md) |
| `agrobr.rio_verde` | [Fundação Rio Verde](rio_verde.md) |
| `agrobr.rnc` | [RNC](rnc.md) |
| `agrobr.sfb` | [SFB](../sources/sfb.md) |
| `agrobr.unica` | [UNICA](unica.md) |
| `agrobr.usda` | [USDA](usda.md) |
| `agrobr.zarc` | [ZARC](zarc.md) |

`agrobr.alt` é o espaço dos 4 módulos `alt`.

## Datasets, contratos, exceções e normalização

| Módulo | O que é público |
|---|---|
| `agrobr.datasets` | o `__all__`: os datasets ([visão geral](../contracts/index.md)) e o catálogo (`list_datasets`, `list_products`, `info`, `describe`, `describe_all`, `get_dataset`), `deterministic`, `is_deterministic` e `get_snapshot` |
| `agrobr.contracts` | o `__all__`: `get_contract`, `list_contracts`, `has_contract`, `validate_dataset`, `register_contract`, `generate_json_schemas`, `Contract`, `Column`, `ColumnType` e `BreakingChangePolicy` |
| `agrobr.exceptions` | o `__all__`: as exceções da raiz e `FingerprintMismatchError`, `NetworkError`, `ValidationError`, `SourceFallbackWarning` e `StaleDataWarning` |
| `agrobr.models` | `MetaInfo` e `Fingerprint` |
| `agrobr.normalize` | o `__all__` ([normalização](../guides/normalizacao.md)) |

O caminho público de um contrato é o nome registrado: `contracts.get_contract("<nome>")`. As constantes que a documentação
ensina a importar também são públicas: `BCB_FOCUS_V2` (`agrobr.contracts.bcb_focus`), `BCB_PTAX_V2` e `BCB_PTAX_MOEDAS_V1`
(`agrobr.contracts.bcb_ptax`), `BCB_SGS_V3` (`agrobr.contracts.bcb_sgs`), `CLIMA_V3` (`agrobr.contracts.clima`),
`COMERCIO_INTERNACIONAL_V3` (`agrobr.contracts.comtrade`), `CONAB_CUSTOS_V3` (`agrobr.contracts.conab_custos`),
`LISTA_SUJA_EMPREGADORES_V2` (`agrobr.contracts.lista_suja`) e `POSICIONAMENTO_FUNDOS_COLUNAS_V2`
(`agrobr.contracts.datasets`). As demais constantes `*_V<n>` citadas nas páginas de contrato identificam a versão; leia o
contrato pelo `get_contract`. As constantes históricas que o guia de migração cita não são públicas; as que
mudaram de nome (como `agrobr.contracts.datasets.CLIMA_V2`) emitem `DeprecationWarning`.

## Síncrono, snapshots e configuração

| Nome | O que é público | Página |
|---|---|---|
| `agrobr.sync` | o espelho síncrono de toda função pública das fontes e de `datasets` (`agrobr.sync.<fonte>.<função>`, `agrobr.sync.alt.<fonte>.<função>`) | [Async](../guides/async.md) |
| `agrobr.snapshots` | o `__all__`: `create_snapshot`, `list_snapshots`, `get_snapshot`, `load_from_snapshot`, `delete_snapshot`, `SnapshotInfo` e `SnapshotManifest` | [Snapshots](../guides/snapshots.md) |
| `agrobr.config` | `set_mode`, `get_config` e `reset_config` | [Snapshots](../guides/snapshots.md) |
| `deterministic_decorator` | o decorator, importado de `agrobr.datasets.deterministic` | [Reprodutibilidade](../advanced/reproducibility.md) |

## Outros nomes ensinados na documentação

Fora dos módulos acima, só estes:

- `agrobr.http.get_timeout` ([resiliência](../advanced/resilience.md));
- `agrobr.validators.sanity.PRICE_RULES` ([resiliência](../advanced/resilience.md#validacao-estatistica));
- `agrobr.defensivos.cache.invalidate` ([defensivos](../sources/defensivos.md));
- `agrobr.normalize.dates.converter_datas` ([normalização](../guides/normalizacao.md));
- `agrobr.normalize.dates.normalizar_safra`, o mesmo objeto de `normalize.normalizar_safra`
  ([guia de migração](../guides/migracao-2.md)).

## CLI e variáveis de ambiente

Os comandos e as opções da [CLI](../advanced/cli.md) são públicos, e o código de saída também. As
[variáveis de ambiente](../advanced/ambiente.md) são públicas, menos as `AGROBR_ALERT_*`.

## Interno

Sem garantia de SemVer:

- `agrobr.alerts`, `agrobr.benchmark`, `agrobr.cache`, `agrobr.health`, `agrobr.utils` e `agrobr.constants`;
- `agrobr.http`, `agrobr.validators`, `agrobr.models` e `agrobr.config`, menos os nomes listados acima;
- os submódulos das fontes, de `agrobr.normalize`, de `agrobr.contracts` e de `agrobr.datasets`, menos os nomes listados
  acima;
- todo nome que começa com `_`.

Nomes internos aparecem no `__all__` de alguns pacotes e às vezes em exemplos de páginas avançadas, para diagnóstico. Use à
vontade, mas fixe a versão: eles podem mudar sem aviso de depreciação.
