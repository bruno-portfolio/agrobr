# API BCB/SICOR

O modulo BCB fornece dados de credito rural do SICOR (Sistema de Operacoes do Credito Rural e do Proagro) do Banco Central do Brasil.

## Funcoes

### `credito_rural`

Dados de financiamento rural por produto, safra, UF e municipio, com dimensoes de programa, fonte de recurso, tipo de seguro, modalidade e atividade.

```python
async def credito_rural(
    produto: str,
    safra: str | None = None,
    finalidade: str = "custeio",
    uf: str | None = None,
    agregacao: str = "municipio",
    programa: str | None = None,
    tipo_seguro: str | None = None,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parametros:**

| Parametro | Tipo | Descricao |
|-----------|------|-----------|
| `produto` | `str` | Produto (soja, milho, arroz, feijao, trigo, algodao, cafe, cana, sorgo) |
| `safra` | `str \| None` | Safra formato "2024/25". Default: safra mais recente |
| `finalidade` | `str` | `"custeio"`, `"investimento"` ou `"comercializacao"` |
| `uf` | `str \| None` | Filtrar por UF (ex: "MT", "PR") |
| `agregacao` | `str` | `"municipio"` (default), `"uf"` ou `"programa"` |
| `programa` | `str \| None` | Filtrar por programa (ex: "Pronamp", "Pronaf") |
| `tipo_seguro` | `str \| None` | Filtrar por tipo de seguro (ex: "Proagro", "Seguro privado") |
| `return_meta` | `bool` | Se True, retorna tupla (DataFrame, MetaInfo) |

**Retorno:**

DataFrame com colunas:

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `safra` | str | Safra "2024/2025" |
| `ano_emissao` | int | Ano de emissao |
| `mes_emissao` | int | Mes de emissao |
| `uf` | str | UF do municipio |
| `municipio` | str | Nome do municipio |
| `produto` | str | Produto financiado |
| `finalidade` | str | Finalidade (custeio, investimento, comercializacao) |
| `valor` | float | Valor financiado (R$) |
| `area_financiada` | float | Area financiada (ha) |
| `qtd_contratos` | int | Quantidade de contratos |
| `cd_programa` | str | Codigo do programa SICOR |
| `programa` | str | Nome do programa (ex: "Pronamp", "Pronaf") |
| `cd_sub_programa` | str | Codigo do sub-programa |
| `cd_fonte_recurso` | str | Codigo da fonte de recurso |
| `fonte_recurso` | str | Nome da fonte (ex: "LCA", "FNE", "Poupanca rural controlados") |
| `cd_tipo_seguro` | str | Codigo do tipo de seguro |
| `tipo_seguro` | str | Nome do seguro (ex: "Proagro", "Seguro privado") |
| `cd_modalidade` | str | Codigo da modalidade |
| `modalidade` | str | Nome da modalidade (ex: "Individual", "Coletiva") |
| `cd_atividade` | str | Codigo da atividade |
| `atividade` | str | Nome da atividade (ex: "Agricola", "Pecuaria") |
| `regiao` | str | Regiao (ex: "SUL", "CENTRO-OESTE") |

**Exemplo:**

```python
from agrobr import bcb

# Credito custeio soja MT
df = await bcb.credito_rural("soja", safra="2024/25", uf="MT")

# Agregado por UF
df = await bcb.credito_rural("milho", agregacao="uf")

# Agregado por programa
df = await bcb.credito_rural("soja", safra="2024/25", agregacao="programa")

# Filtrar por programa
df = await bcb.credito_rural("soja", safra="2024/25", programa="Pronamp")

# Filtrar por tipo de seguro
df = await bcb.credito_rural("soja", safra="2024/25", tipo_seguro="Proagro")

# Com metadados
df, meta = await bcb.credito_rural("soja", return_meta=True)
print(meta.schema_version)  # "1.1"
```

## Dimensoes SICOR

As dimensoes sao enriquecidas automaticamente pelo parser com dicionarios hardcoded. Codigos desconhecidos geram `"Desconhecido ({code})"` com log warning.

| Dimensao | Codigos conhecidos |
|----------|-------------------|
| Programa | Pronaf, Pronamp, Funcafe, Moderfrota, ABC, Inovagro, etc. |
| Fonte de recurso | Recursos obrigatorios, Poupanca rural, LCA, FNO/FNE/FCO, Funcafe, etc. |
| Tipo de seguro | Proagro, Sem seguro, Seguro privado, Nao se aplica |
| Modalidade | Individual, Coletiva |
| Atividade | Agricola, Pecuaria |

## Versao Sincrona

```python
from agrobr.sync import bcb

df = bcb.credito_rural("soja", safra="2024/25")
df = bcb.credito_rural("soja", safra="2024/25", programa="Pronamp")
```

## Fallback

Quando a API OData do BCB falha, o agrobr usa automaticamente BigQuery (Base dos Dados) como fallback. Requer `pip install agrobr[bigquery]`.

## Notas

- Fonte: [BCB/SICOR](https://olinda.bcb.gov.br) — licenca livre
- Dados disponiveis a partir de 2013
- Contract v1.1 — 11 novas colunas nullable desde v0.10.1
