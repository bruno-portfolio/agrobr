# API ANTAQ

O modulo ANTAQ fornece dados de movimentacao portuaria de carga do Estatistico Aquaviario, publicados pela Agencia Nacional de Transportes Aquaviarios.

## Funcoes

### `movimentacao`

Movimentacao portuaria de carga de um ano.

```python
async def movimentacao(
    ano: int,
    *,
    tipo_navegacao: str | None = None,
    natureza_carga: str | None = None,
    mercadoria: str | None = None,
    porto: str | None = None,
    uf: str | None = None,
    sentido: str | None = None,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parametros:**

| Parametro | Tipo | Descricao |
|-----------|------|-----------|
| `ano` | `int` | Ano dos dados (2010-2025) |
| `tipo_navegacao` | `str \| None` | longo_curso, cabotagem, interior, apoio_maritimo, apoio_portuario |
| `natureza_carga` | `str \| None` | granel_solido, granel_liquido, carga_geral, conteiner |
| `mercadoria` | `str \| None` | Filtro por mercadoria (substring case-insensitive) |
| `porto` | `str \| None` | Filtro por porto (substring case-insensitive) |
| `uf` | `str \| None` | Filtro por UF (ex: SP, PR, MT) |
| `sentido` | `str \| None` | embarque ou desembarque |
| `return_meta` | `bool` | Se True, retorna tupla (DataFrame, MetaInfo) |

**Retorno:**

DataFrame com colunas: `ano`, `mes`, `data_atracacao`, `tipo_navegacao`, `tipo_operacao`,
`natureza_carga`, `sentido`, `porto`, `complexo_portuario`, `terminal`, `municipio`, `uf`,
`regiao`, `cd_mercadoria`, `mercadoria`, `grupo_mercadoria`, `origem`, `destino`,
`peso_bruto_ton`, `qt_carga`, `teu`

**Exemplo:**

```python
from agrobr import antaq

# Movimentacao 2024
df = await antaq.movimentacao(2024)

# Filtrar por UF
df = await antaq.movimentacao(2024, uf="SP")

# Filtrar por tipo de navegacao e natureza da carga
df = await antaq.movimentacao(
    2024,
    tipo_navegacao="longo_curso",
    natureza_carga="granel_solido",
)

# Filtrar por mercadoria
df = await antaq.movimentacao(2024, mercadoria="soja")
```

## Versao Sincrona

```python
from agrobr.sync import antaq

df = antaq.movimentacao(2024)
```

## Notas

- Fonte: [ANTAQ Estatistico Aquaviario](https://web3.antaq.gov.br/ea/sense/download.html) — licenca `livre`
- Dados: ZIP bulk (TXT com `;`, encoding UTF-8-sig)
- Historico: 2010+
- ZIPs anuais (~80MB) — download pode levar alguns segundos
