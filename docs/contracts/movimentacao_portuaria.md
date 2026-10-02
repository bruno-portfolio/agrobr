# Contrato: movimentacao_portuaria

Movimentação portuária de cargas — ANTAQ.

!!! warning "Fonte indisponivel desde 23/06/2026"
    A ANTAQ tirou o Estatistico Aquaviario do ar ([aviso oficial](https://www.gov.br/antaq/pt-br/central-de-conteudos/publicacoes-da-antaq/publicacoes-off/painel-estatistico-aquaviario-indisponivel)).
    O host `estatistica.antaq.gov.br` nao serve mais os arquivos: responde `403` (Cloudflare
    challenge) ou redireciona para o aviso de indisponibilidade, conforme o cliente.
    Chamadas a `antaq.movimentacao()` levantam `SourceUnavailableError`. Nao ha fonte
    alternativa com cobertura equivalente — a Base dos Dados cobre apenas 2014-2020.
    Ultima verificacao: 31/08/2026.

## Schema

| Coluna | Tipo | Nullable | Unidade | Restrições |
|--------|------|----------|---------|------------|
| `ano` | INTEGER | Não | — | ≥ 2010 |
| `mes` | INTEGER | Não | — | 1-12 |
| `data_atracacao` | DATETIME | Sim | — | — |
| `tipo_navegacao` | STRING | Sim | — | — |
| `tipo_operacao` | STRING | Sim | — | — |
| `natureza_carga` | STRING | Sim | — | — |
| `sentido` | STRING | Sim | — | Embarcados/Desembarcados |
| `porto` | STRING | Sim | — | — |
| `complexo_portuario` | STRING | Sim | — | — |
| `terminal` | STRING | Sim | — | — |
| `municipio` | STRING | Sim | — | — |
| `uf` | STRING | Sim | — | UF válida |
| `regiao` | STRING | Sim | — | — |
| `cd_mercadoria` | STRING | Sim | — | — |
| `mercadoria` | STRING | Sim | — | — |
| `grupo_mercadoria` | STRING | Sim | — | — |
| `origem` | STRING | Sim | — | — |
| `destino` | STRING | Sim | — | — |
| `peso_bruto_ton` | FLOAT | Sim | ton | ≥ 0 |
| `qt_carga` | FLOAT | Sim | — | ≥ 0 |
| `teu` | INTEGER | Sim | — | ≥ 0 |

**PK:** `(ano, mes, porto, cd_mercadoria, sentido, tipo_navegacao)`

## Agregacao

`datasets.movimentacao_portuaria` **agrega** a saida da fonte pela PK
`(ano, mes, porto, cd_mercadoria, sentido, tipo_navegacao)`: `peso_bruto_ton`, `qt_carga` e `teu` sao
somados; `complexo_portuario`, `municipio`, `uf`, `regiao`, `mercadoria` e `grupo_mercadoria` usam o
primeiro valor nao nulo do grupo; `data_atracacao`, `tipo_operacao`, `natureza_carga`, `terminal`,
`origem` e `destino` so sobrevivem quando o grupo tem um unico valor - caso contrario saem nulos.
Linhas sem `ano` ou `mes` (carga sem atracacao correspondente) sao descartadas antes da agregacao.
Num recorte de 2024, 10 cargas viram 6 linhas. Nulo em `peso_bruto_ton`, `qt_carga` ou `teu` em
qualquer linha do grupo deixa a soma nula: ausência não é zero.

Se o TXT da ANTAQ vier sem uma coluna que o join, os filtros ou a PK usam, o dataset levanta
`ParseError` com o nome da coluna (lista na página da fonte).

`qt_carga` nao tem unidade canonica: a ANTAQ publica `QTCarga` sem unidade e o valor muda de sentido
conforme o tipo de carga. A coluna usa `float64`, inclusive no vazio.

## Parâmetros

- `ano: int` — ano da movimentação (obrigatório, ≥ 2010)
- `mercadoria: str | None` — filtro por mercadoria (substring, case-insensitive)
- `porto: str | None` — filtro por porto (substring, case-insensitive)
- `uf: str | None` — filtro por UF (match exato, uppercase)
- `sentido: str | None` — "embarque" ou "desembarque"
- `tipo_navegacao: str | None` — tipo de navegação
- `natureza_carga: str | None` — natureza da carga

## Exemplo

```python
from agrobr import datasets

# Movimentação 2024
df = await datasets.movimentacao_portuaria(ano=2024)

# Soja embarcada em Santos
df = await datasets.movimentacao_portuaria(
    ano=2024, mercadoria="Soja", porto="Santos", sentido="embarque"
)

# Com metadados
df, meta = await datasets.movimentacao_portuaria(ano=2024, return_meta=True)
```

`data_atracacao` preserva a data e a hora publicadas em `datetime64[ns]`; erro de calendário vira `NaT` com aviso em `MetaInfo.validation_warnings`. Texto que não é data levanta `ParseError`. `ano`, `mes` e `teu` usam `Int64`; `peso_bruto_ton` e `qt_carga` usam `float64`. Cheio e vazio têm os mesmos tipos. `sentido`, `tipo_navegacao` e `natureza_carga` aceitam os aliases documentados e os rótulos publicados inteiros, ignorando caixa, acento e espaço nas pontas. Valor não suportado levanta `InvalidParameterError` antes de baixar os ZIPs.
