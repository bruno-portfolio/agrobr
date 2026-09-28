# Contrato: queimadas

Focos de calor detectados por satélite — INPE Queimadas.

## Schema

| Coluna | Tipo | Nullable | Unidade | Restrições |
|--------|------|----------|---------|------------|
| `data` | DATE | Não | — | Data válida |
| `hora_gmt` | STRING | Sim | — | — |
| `lat` | FLOAT | Não | — | -35 a 6 |
| `lon` | FLOAT | Não | — | -74 a -30 |
| `satelite` | STRING | Não | — | — |
| `municipio` | STRING | Sim | — | — |
| `municipio_id` | INTEGER | Sim | — | — |
| `cod_municipio` | INTEGER | Sim | — | código IBGE de 7 dígitos, do `municipio_id` |
| `estado` | STRING | Sim | — | — |
| `uf` | STRING | Sim | — | — |
| `bioma` | STRING | Sim | — | — |
| `numero_dias_sem_chuva` | FLOAT | Sim | dias | ≥ 0 |
| `precipitacao` | FLOAT | Sim | mm | ≥ 0 |
| `risco_fogo` | FLOAT | Sim | — | 0 a 1 |
| `frp` | FLOAT | Sim | MW | ≥ 0 |

**PK:** `(data, lat, lon, satelite, hora_gmt)`

## Fuso e sentinelas

`data` e `hora_gmt` vêm de `data_hora_gmt`, publicado pelo INPE em **GMT**; não há conversão para
hora local. `numero_dias_sem_chuva`, `precipitacao`, `risco_fogo` e `frp` usam `-999` como sentinela
de ausência na publicação e saem **nulos**, nunca zero; célula vazia também sai nula. `bioma` pode
vir vazio na publicação (ocorre em focos sobre corpos d'água, por exemplo) e é preservado como texto
vazio, sem ser descartado nem substituído.

## FRP negativo e foco repetido

A fonte publica, em alguns meses, focos fora do contrato. Na 2.0, `queimadas.focos` e o dataset tratam os casos e seguem, em vez
de recusar o mês inteiro. A chave é `(data, hora_gmt, lat, lon, satelite)`:

- **cópia igual em todas as colunas:** sai uma vez só (`source_details["duplicatas_colapsadas"]`);
- **chave repetida que difere só no FRP:** sai em 1 linha, com o `frp` **nulo** (`source_details["frp_divergente"]`, com os
  focos e as linhas). O foco existe; só a potência é ambígua;
- **chave repetida que difere em outra coluna:** nenhum dos focos é o certo, e a chave sai do resultado
  (`source_details["chaves_repetidas"]`, com as chaves). Nenhum caso na varredura de 2023–2026;
- **FRP negativo**, fisicamente impossível: sai **nulo** (`source_details["frp_negativo_anulado"]`). O `-999` segue como
  sentinela e sai nulo sem entrar nessa contagem.

Cada caso vem em `meta.validation_warnings` e como `UserWarning`, com a contagem. A contagem é a do resultado devolvido, depois
dos filtros `uf`, `bioma` e `satelite`. Na leitura de 20 meses (jun–out de 2023 a 2025, jun–ago de 2026, fev/2024 e mar/2025),
estes 7 eram recusados inteiros pelo dataset antes da correção, com `ContractViolationError`; as chaves repetidas diferem só no FRP:

| Mês | Focos | FRP negativo | Cópias iguais | Chaves com FRP diferente (linhas) |
|-----|------:|-------------:|--------------:|----------------------------------:|
| jun/2023 | 206.059 | 0 | 32 | 0 |
| jul/2023 | 308.393 | 0 | 52 | 51 (102) |
| ago/2024 | 2.263.154 | 28 | 0 | 1 (2) |
| set/2024 | 2.567.880 | 7 | 0 | 2 (4) |
| out/2024 | 1.006.520 | 2 | 0 | 0 |
| mar/2025 | 49.265 | 0 | 0 | 1 (2) |
| set/2025 | 833.039 | 0 | 9 | 1 (2) |

Em ago/2024, a chave repetida é a de 29/08, 17:07 GMT, NOAA-20, em São Félix do Xingu (FRP 5,5 × 8,5), e o FRP negativo vai de
−3,8 a −0,1, sempre em satélites VIIRS.

## Mês corrente parcial

O INPE publica o arquivo do período corrente, o atualiza durante o período e o fecha depois do fim dele: o mensal fecha no dia 1
do mês seguinte, às 23:56 GMT (jul a dez/2025, jul e ago/2026), e o diário, em D+1 às 12:05 GMT (os 25 diários fechados de
set/2026). Em 26/09/2026, o mensal de setembro tinha `Last-Modified` de 26/09 21:56 GMT e ia até o foco de 25/09 23:50 GMT; o
diário do dia tinha `Last-Modified` de 23:06 GMT e ia até o foco de 22:40 GMT.

O resultado é parcial quando o `Last-Modified` do arquivo é anterior ao fechamento, com alguns minutos de margem (o dia 1 do mês
seguinte às 23:50 GMT no mensal; D+1 às 12:00 GMT no diário), ou, sem o cabeçalho, quando o relógio está antes do fechamento
mais 1 h. Nesse caso, vem com aviso em `meta.validation_warnings` e como `UserWarning`, e o `source_details` traz `mes_parcial`
(`dia_parcial`, no diário), `ultimo_foco` (data e hora GMT do último foco do arquivo, antes dos filtros) e `last_modified`. O
mensal republicado depois (jan a jun/2026 foram republicados em 17 e 21/07/2026) é revisão, e não parcial.

O diário de um dia passado é um retrato de D+1: para um dia de mês fechado, o mensal filtrado pelo dia é o mais completo. Em
14/09/2026, o mensal tem 27.953 focos, 506 (1,8 %) a mais que o diário (27.447).

## Parâmetros obrigatórios

- `ano: int` — ano dos focos
- `mes: int` — mês dos focos

## Filtro de bioma

`bioma` aceita Amazônia, Cerrado, Mata Atlântica, Caatinga, Pampa e Pantanal, com ou sem acentos e sem distinção entre maiúsculas e minúsculas. Valores desconhecidos levantam `ValueError` antes da consulta à fonte.

## Exemplo

```python
from agrobr import datasets

# Focos de agosto/2024
df = await datasets.queimadas(ano=2024, mes=8)

# Com filtros
df = await datasets.queimadas(ano=2024, mes=8, uf="TO", bioma="Cerrado")

# Com metadados
df, meta = await datasets.queimadas(ano=2024, mes=8, return_meta=True)
```
