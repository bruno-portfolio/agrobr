# Política de Versionamento (Semver)

O agrobr segue [Semantic Versioning 2.0.0](https://semver.org/) com granularidade
por dataset. Cada dataset tem `schema_version` proprio (independente de `lib_version`).

## Regras

A versão da biblioteca também segue SemVer e sobe major quando o comportamento
muda de forma ampla, mesmo quando nem todo contrato de dataset muda. A garantia vale para a
[API pública](../api/index.md); o resto do pacote é interno e pode mudar em qualquer versão.

| Tipo de mudanca | Bump | Exemplo |
|---|---|---|
| Campo removido ou renomeado | **Major** | Renomear `preco` > `price` |
| Tipo de dado alterado (narrowing) | **Major** | `price: float64` > `price: str` |
| Coluna obrigatoria vira opcional | **Major** | `uf: required` > `uf: nullable` |
| Chave primária altera a identidade da observação | **Major** | `estimativa_safra` 3.0 distingue fonte e mês LSPA |
| Nova coluna opcional adicionada | Minor | Adiciona `latitude` |
| Constraint adicionada | Minor | Adiciona `price_min: 0` |
| Nova fonte de fallback | Patch | Adiciona ABIOVE como backup |
| Fix de parsing (mesmas colunas) | Patch | Corrige encoding de municipio |
| Tipo de dado alargado | Patch | `int` > `float` (compativel) |
| Nova fonte de dados (modulo) | Minor | `agrobr.bcb` |

**Principio:** `schema_version` do dataset so incrementa major quando a mudanca
pode quebrar codigo downstream que depende do schema atual.

## Garantias por Dataset

### `preco_diario`

| Coluna | Tipo | Garantia | Desde |
|---|---|---|---|
| `data` | `date` | obrigatória | v0.4.0 |
| `produto` | `str` | obrigatória | v0.4.0 |
| `valor` | `float` | obrigatória, > 0 | v0.4.0 |
| `unidade` | `str` | obrigatória | v0.4.0 |
| `fonte` | `str` | obrigatória | v0.6.0 |

### `estimativa_safra`

Contrato 3.0: chave `[fonte, safra, produto, uf, levantamento, ano_lspa, mes_lspa]`. A mudança evita colisões entre meses LSPA e exige atualizar chaves persistidas, embora as dez colunas anteriores sejam mantidas. `CONAB_SAFRA_V2` permanece como contrato 2.0 da fonte.

| Coluna | Tipo | Garantia | Desde |
|---|---|---|---|
| `produto` | `str` | obrigatória | v0.4.0 |
| `safra` | `str` | obrigatória, formato `YYYY/YY` | v0.4.0 |
| `uf` | `str` | opcional | v0.4.0 |
| `area_plantada` | `float` | opcional, >= 0 | v0.4.0 |
| `area_colhida` | `float` | opcional, >= 0 | v0.4.0 |
| `produtividade` | `float` | opcional, >= 0 | v0.4.0 |
| `producao` | `float` | opcional, >= 0 | v0.4.0 |
| `levantamento` | `int` | opcional, 1-12; nulo quando a fonte é o LSPA | v1.2.0 |
| `data_publicacao` | `date` | opcional; nula quando a fonte é o LSPA | v1.2.0 |
| `ano_lspa` | `Int64` | nullable; ano observado LSPA, nulo para CONAB | contrato 3.0 |
| `mes_lspa` | `Int64` | nullable, 1–12; mês observado LSPA, nulo para CONAB | contrato 3.0 |
| `fonte` | `str` | obrigatória | v0.6.0 |

### `producao_anual` (IBGE PAM)

| Coluna | Tipo | Garantia | Desde |
|---|---|---|---|
| `ano` | `int` | obrigatória, >= 1974 | v0.4.0 |
| `localidade` | `str` | opcional | v0.4.0 |
| `produto` | `str` | obrigatória | v0.4.0 |
| `area_plantada` | `float` | opcional, >= 0 | v0.4.0 |
| `area_colhida` | `float` | opcional, >= 0 | v0.4.0 |
| `producao` | `float` | opcional, >= 0 | v0.4.0 |
| `rendimento` | `float` | opcional, >= 0 | v0.4.0 |
| `valor_producao` | `float` | opcional, >= 0 | v0.4.0 |
| `fonte` | `str` | obrigatória | v0.6.0 |

### Source Layer — Contratos por módulo

Módulos da source layer (`agrobr.cepea`, `agrobr.conab`, etc.) retornam
DataFrames com colunas documentadas, mas com garantia **menor** que a
camada de datasets. A camada de datasets normaliza e valida.

#### `comexstat.exportacao` (v1.1)

| Coluna | Tipo | Garantia |
|---|---|---|
| `ano` | `int` | obrigatória |
| `mes` | `int` | obrigatória, 1-12 |
| `produto` | `str` | obrigatória |
| `uf` | `str` | opcional |
| `kg_liquido` | `float` | opcional, >= 0 |
| `valor_fob_usd` | `float` | opcional, >= 0 |
| `volume_ton` | `float` | opcional, >= 0 |

#### `bcb.credito_rural` (v2.0)

| Coluna | Tipo | Garantia |
|---|---|---|
| `safra` | `str` | obrigatória |
| `produto` | `str` | obrigatória |
| `uf` | `str` | opcional |
| `finalidade` | `str` | obrigatória |
| `agregacao` | `str` | obrigatória (`uf` ou `programa`) |
| `programa` | `str` | opcional |
| `cd_programa` | `str` | opcional |
| `qtd_contratos` | `int` | opcional, >= 0 |
| `valor` | `float` | opcional, >= 0 |
| `area_financiada` | `float` | opcional, >= 0 |
| `fonte` | `str` | obrigatória |

#### `inmet.clima_uf` (contrato `clima` v3.1)

| Coluna | Tipo | Garantia |
|---|---|---|
| `mes` | `date` | obrigatória |
| `uf` | `str` | obrigatória |
| `precip_acum_mm` | `float` | opcional, >= 0 |
| `temp_media` | `float` | opcional |
| `temp_max_media` | `float` | opcional |
| `temp_min_media` | `float` | opcional |
| `num_estacoes` | `int` | opcional, >= 0 |
| `umidade_media` | `float` | opcional, 0-100 |
| `radiacao_media_mj` | `float` | opcional, >= 0 |
| `vento_medio_ms` | `float` | opcional, >= 0 |
| `fonte` | `str` | obrigatória |

#### `anda.entregas` (v2.0)

| Coluna | Tipo | Garantia |
|---|---|---|
| `ano` | `int` | obrigatória |
| `mes` | `int` | obrigatória, 1-12 |
| `uf` | `str` | opcional |
| `produto_fertilizante` | `str` | obrigatória |
| `volume_ton` | `float` | opcional, >= 0 |

## MetaInfo

Todas as funções com `return_meta=True` retornam `MetaInfo` com campos
de proveniência. Campos do MetaInfo são **aditivos** (nunca removidos),
portanto não constituem breaking change.

## Deprecation

Antes de remover uma coluna ou alterar um tipo (breaking change):

1. Coluna marcada como `deprecated` por pelo menos 1 minor release
2. Warning emitido via `DeprecationWarning` no runtime
3. Documentado no CHANGELOG
4. Removida no próximo major
