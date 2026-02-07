# Política de Versionamento (Semver)

O agrobr segue [Semantic Versioning 2.0.0](https://semver.org/) com granularidade
por dataset.

## Regras

| Tipo de mudança | Versão | Exemplo |
|---|---|---|
| Coluna removida ou tipo alterado | **Major** | `valor: float` vira `valor: str` |
| Nova coluna opcional adicionada | **Minor** | Adiciona `variacao_pct` |
| Fix de parsing sem alterar schema | **Patch** | Corrige regex do parser |
| Nova fonte de dados | **Minor** | Adiciona ANDA como fonte |
| Novo módulo de source layer | **Minor** | `agrobr.bcb` |

## Garantias por Dataset

### `preco_diario`

| Coluna | Tipo | Garantia | Desde |
|---|---|---|---|
| `data` | `date` | obrigatória | v0.4.0 |
| `produto` | `str` | obrigatória | v0.4.0 |
| `valor` | `float` | obrigatória, > 0 | v0.4.0 |
| `unidade` | `str` | obrigatória | v0.4.0 |
| `fonte` | `str` | obrigatória | v0.6.0 |
| `variacao_pct` | `float` | opcional | v0.4.0 |

### `estimativa_safra`

| Coluna | Tipo | Garantia | Desde |
|---|---|---|---|
| `produto` | `str` | obrigatória | v0.4.0 |
| `safra` | `str` | obrigatória, formato `YYYY/YY` | v0.4.0 |
| `uf` | `str` | obrigatória | v0.4.0 |
| `area_plantada` | `float` | obrigatória, >= 0 | v0.4.0 |
| `producao` | `float` | obrigatória, >= 0 | v0.4.0 |
| `produtividade` | `float` | obrigatória, >= 0 | v0.4.0 |
| `levantamento` | `int` | obrigatória, 1-12 | v0.6.0 |

### `producao_anual` (IBGE PAM)

| Coluna | Tipo | Garantia | Desde |
|---|---|---|---|
| `ano` | `int` | obrigatória | v0.4.0 |
| `produto` | `str` | obrigatória | v0.4.0 |
| `localidade` | `str` | obrigatória | v0.4.0 |
| `producao` | `float` | obrigatória, >= 0 | v0.4.0 |
| `area_plantada` | `float` | obrigatória, >= 0 | v0.4.0 |

### Source Layer — Contratos por módulo

Módulos da source layer (`agrobr.cepea`, `agrobr.conab`, etc.) retornam
DataFrames com colunas documentadas, mas com garantia **menor** que a
camada de datasets. A camada de datasets normaliza e valida.

#### `comexstat.exportacao` (v0.7.0)

| Coluna | Tipo | Garantia |
|---|---|---|
| `ano` | `int` | obrigatória |
| `mes` | `int` | obrigatória, 1-12 |
| `ncm` | `str` | obrigatória, 8 dígitos |
| `uf` | `str` | obrigatória |
| `kg_liquido` | `float` | obrigatória, >= 0 |
| `valor_fob_usd` | `float` | obrigatória, >= 0 |
| `volume_ton` | `float` | apenas em agregação mensal |

#### `bcb.credito_rural` (v0.7.0)

| Coluna | Tipo | Garantia |
|---|---|---|
| `safra` | `str` | obrigatória |
| `uf` | `str` | obrigatória |
| `produto` | `str` | obrigatória |
| `finalidade` | `str` | obrigatória |
| `valor` | `float` | obrigatória, >= 0 |
| `area_financiada` | `float` | obrigatória, >= 0 |
| `qtd_contratos` | `int` | obrigatória, >= 0 |

#### `inmet.clima_uf` (v0.7.0)

| Coluna | Tipo | Garantia |
|---|---|---|
| `mes` | `int` | obrigatória, 1-12 |
| `uf` | `str` | obrigatória |
| `precip_acum_mm` | `float` | obrigatória |
| `temp_media` | `float` | obrigatória |
| `num_estacoes` | `int` | obrigatória |

#### `anda.entregas` (v0.7.0)

| Coluna | Tipo | Garantia |
|---|---|---|
| `ano` | `int` | obrigatória |
| `mes` | `int` | obrigatória, 1-12 |
| `uf` | `str` | obrigatória |
| `produto_fertilizante` | `str` | obrigatória |
| `volume_ton` | `float` | obrigatória, >= 0 |

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
