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
| `produto` | `str` | Alias da tabela abaixo ou prefixo NCM de 2 a 8 dígitos, sem pontos (ex.: `"1507"`, `"22071010"`) |
| `ano` | `int \| None` | Ano de referencia. Default: ano anterior |
| `uf` | `str \| None` | Filtrar por UF |
| `agregacao` | `str` | `"mensal"` (default) ou `"detalhado"` |
| `as_polars` | `bool` | Se True, retorna polars.DataFrame |
| `return_meta` | `bool` | Se True, retorna tupla (DataFrame, MetaInfo) |

**Aliases de produto:**

| Alias | NCM (prefixos) | Entra | Não entra |
|-------|----------------|-------|-----------|
| `soja`, `soja_grao` | `12019000`; `12010090` (código até 2013) | soja em grão, mesmo triturada | semeadura (`soja_semeadura`), óleo (`1507`), farelo (`2304`) |
| `soja_semeadura` | `12011000`; `12010010` (código até 2011) | soja para semeadura | — |
| `oleo_soja` | `1507` | óleo de soja bruto, refinado e outros | — |
| `oleo_soja_bruto` | `15071000` | óleo bruto, mesmo degomado | refinado |
| `farelo_soja` | `2304` | farinhas e pellets (`23040010`) e bagaços e outros resíduos (`23040090`) | — |
| `milho` | `1005` | grão, semeadura e demais | farinha, amido e óleo (outros capítulos) |
| `arroz` | `1006` | com casca, descascado, semibranqueado ou branqueado (parboilizado ou não) e quebrado | farinha (`1102`) |
| `trigo` | `1001` | trigo duro e demais trigos, inclusive semeadura, e mistura com centeio | farinha (`1101`) |
| `algodao` | `5201`, `5203` | não cardado nem penteado; cardado ou penteado | desperdícios (`5202`), fios e tecidos |
| `algodao_cardado` | `520300` | cardado ou penteado | — |
| `cafe` | `09011`, `09012` | não torrado e torrado, com ou sem cafeína | cascas, películas e sucedâneos (`09019000`), solúvel (`2101`) |
| `acucar` | `1701` | bruto de cana e de beterraba e refinado | melaço (`1703`) |
| `etanol` | `2207` | álcool etílico não desnaturado e desnaturado | — |
| `carne_bovina` | `0201`, `0202` | fresca, refrigerada e congelada | miudezas (`0206`), salgada, seca ou defumada (`0210`), preparações (`1602`) |
| `carne_frango` | `02071` | galos e galinhas inteiros e em pedaços, com miudezas, frescos e congelados | peru, pato e outras aves; salgada (`0210`); preparações (`1602`) |
| `carne_suina` | `0203` | fresca, refrigerada e congelada | miudezas (`0206`), salgada (`0210`), preparações (`1602`) |
| `fertilizantes` | `31` | capítulo 31 inteiro | — |
| `ureia` | `310210` | ureia de qualquer teor de nitrogênio | — |
| `sulfato_amonio` | `31022100` | sulfato de amônio | sais duplos e misturas (`310229`) |
| `nitrato_amonio` | `31023000` | nitrato de amônio | misturas com carbonato de cálcio (`31024000`) |
| `ssp` | `31031900` (desde 2017) | superfosfatos com menos de 35 % de P2O5 | anos anteriores a 2017: `InvalidParameterError` (use o prefixo `310310`) |
| `tsp` | `31031100` (desde 2017) | superfosfatos com 35 % ou mais de P2O5 | anos anteriores a 2017: `InvalidParameterError` (use o prefixo `310310`) |
| `kcl` | `310420` | cloreto de potássio de qualquer teor de K2O | — |
| `map` | `31054000` | fosfato monoamônico | — |
| `dap` | `310530` | fosfato diamônico (`31053000`; `31053010` e `31053090` até 2019) | — |
| `npk` | `31052000` | adubos com os três elementos N, P e K | MAP, DAP e demais adubos da posição `3105` (use o prefixo `3105`) |
| `defensivos`, `agrotoxicos` | `3808` | inseticidas, fungicidas, herbicidas, reguladores de crescimento, desinfetantes e raticidas | 27 códigos de uso exclusivamente domissanitário (ex.: `38089119`, `38089419`) |

`cafe_arabica` e `cafe_conilon` foram removidos: a NCM não separa espécie de café
(`09011110` é café em grão das duas espécies) e a chamada levanta
`InvalidParameterError` explicando isso.

**Retorno:**

- `agregacao="mensal"`: `ano`, `mes`, `ncm`, `uf`, `kg_liquido`,
  `valor_fob_usd`, `volume_ton`.
- `agregacao="detalhado"`: `ano`, `mes`, `ncm`, `cod_unidade`, `cod_pais`,
  `uf`, `cod_via`, `cod_urf`, `qtd_estatistica`, `kg_liquido`,
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
- Cada alias soma os códigos vigentes em cada ano: quando a nomenclatura desdobra
  ou renumera um código (etanol `22071000` → `22071010`/`22071090` em 2011; soja
  `12010090` → `12019000` em 2012; frango `02071400` → 14 subitens em 2024), o
  alias cobre os dois períodos, inclusive no ano de transição
- Ano sem código equivalente na nomenclatura levanta `InvalidParameterError`
  antes da rede (`ssp` e `tsp` antes de 2017)
- Prefixo NCM informado em `produto` seleciona todos os códigos que começam por
  ele, sem as exclusões dos aliases
- `MetaInfo.source_details["query"]` registra `ncm_prefixos` e `ncm_excluidos`
- Arquivos CSV anuais de ~100MB cada
- Dados disponiveis a partir de 1997
