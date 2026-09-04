# API ComexStat

O modulo ComexStat fornece dados de exportacao e importacao brasileira do MDIC/SECEX — volumes, valores FOB (USD) por produto, UF e pais.

## Funcoes

### `exportacao`

Dados de exportacao por produto agricola.

```python
async def exportacao(
    produto: str,
    ano: int | None = None,
    uf: str | None = None,
    agregacao: str = "mensal",
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

### `importacao`

Dados de importacao por produto agricola. Mesma interface de `exportacao()`.

```python
async def importacao(
    produto: str,
    ano: int | None = None,
    uf: str | None = None,
    agregacao: str = "mensal",
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parametros (ambas):**

| Parametro | Tipo | Descricao |
|-----------|------|-----------|
| `produto` | `str` | Alias da tabela abaixo ou prefixo NCM |
| `ano` | `int \| None` | Ano de referencia. Default: ano anterior |
| `uf` | `str \| None` | Filtrar por UF |
| `agregacao` | `str` | `"mensal"` (default) ou `"detalhado"` |
| `as_polars` | `bool` | Se True, retorna polars.DataFrame |
| `return_meta` | `bool` | Se True, retorna tupla (DataFrame, MetaInfo) |

**Aliases de produto:**

| Alias | NCM ou prefixo |
|-------|----------------|
| `soja` | `12019000` |
| `soja_grao` | `12019000` |
| `soja_semeadura` | `12011000` |
| `oleo_soja` | `1507` |
| `oleo_soja_bruto` | `15071000` |
| `farelo_soja` | `23040010` |
| `milho` | `10059010` |
| `arroz` | `10063021` |
| `trigo` | `10019900` |
| `algodao` | `520100` |
| `algodao_cardado` | `520300` |
| `cafe` | `09011110` |
| `cafe_arabica` | `09011110` |
| `cafe_conilon` | `09011190` |
| `acucar` | `17011400` |
| `etanol` | `22071000` |
| `carne_bovina` | `02023000` |
| `carne_frango` | `02071400` |
| `carne_suina` | `02032900` |
| `fertilizantes` | `31` |
| `ureia` | `31021010` |
| `sulfato_amonio` | `31022100` |
| `nitrato_amonio` | `31023000` |
| `ssp` | `31031900` |
| `tsp` | `31031100` |
| `kcl` | `31042090` |
| `map` | `31054000` |
| `dap` | `31053000` |
| `npk` | `3105` |
| `defensivos` | `3808` |
| `agrotoxicos` | `3808` |

**Retorno:**

- `agregacao="mensal"`: `ano`, `mes`, `ncm`, `uf`, `kg_liquido`,
  `valor_fob_usd`, `volume_ton`.
- `agregacao="detalhado"`: `ano`, `mes`, `ncm`, `cod_unidade`, `cod_pais`,
  `uf`, `cod_via`, `cod_porto`, `qtd_estatistica`, `kg_liquido`,
  `valor_fob_usd`.

**Exemplo:**

```python
from agrobr import comexstat

# Exportacao soja 2024
df = await comexstat.exportacao("soja", ano=2024)

# Importacao soja 2024
df = await comexstat.importacao("soja", ano=2024)

# Filtrar por UF
df = await comexstat.exportacao("milho", ano=2024, uf="MT")

# Detalhado (por registro)
df = await comexstat.exportacao("cafe", ano=2024, agregacao="detalhado")
```

## Versao Sincrona

```python
from agrobr.sync import comexstat

df = comexstat.exportacao("soja", ano=2024)
df = comexstat.importacao("soja", ano=2024)
```

## Notas

- Fonte: [ComexStat/MDIC](https://comexstat.mdic.gov.br) — licenca livre
- 31 aliases mapeados por prefixo NCM; `oleo_soja` cobre `1507` e
  `oleo_soja_bruto` permanece específico para `15071000`
- Arquivos CSV anuais de ~100MB cada
- Dados disponiveis a partir de 1997
