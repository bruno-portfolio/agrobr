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
| `data_atracacao` | STRING | Sim | — | — |
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
