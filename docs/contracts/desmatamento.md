# Contrato: desmatamento

Desmatamento consolidado (PRODES) e alertas em tempo real (DETER) por bioma.

## Modos

| `tipo=` | Contrato | Fonte |
|---------|----------|-------|
| `"prodes"` (default) | `DESMATAMENTO_PRODES_V2` | INPE TerraBrasilis |
| `"deter"` | `DESMATAMENTO_DETER_V2` | INPE TerraBrasilis |

## Schema: PRODES

| Coluna | Tipo | Nullable | Unidade | Restrições |
|--------|------|----------|---------|------------|
| `ano` | INTEGER | Não | — | 1 a 9999, inteiro |
| `uf` | STRING | Não | — | UF válida |
| `classe` | STRING | Não | — | — |
| `area_km2` | FLOAT | Sim | km² | ≥ 0 |
| `satelite` | STRING | Sim | — | — |
| `sensor` | STRING | Sim | — | — |
| `bioma` | STRING | Não | — | Bioma válido |

**PK:** `(ano, uf, classe, bioma)`

## Schema: DETER

| Coluna | Tipo | Nullable | Unidade | Restrições |
|--------|------|----------|---------|------------|
| `data` | DATE | Não | — | Data válida |
| `classe` | STRING | Não | — | — |
| `uf` | STRING | Não | — | UF válida |
| `municipio` | STRING | Sim | — | — |
| `municipio_id` | STRING | Sim | — | — |
| `cod_municipio` | INTEGER | Sim | — | código IBGE de 7 dígitos, do `municipio_id`; sem ele (DETER Cerrado, cuja camada não traz o código), pelo nome inteiro de `municipio` na UF, com `normalize.resolver_municipio`; nulo onde a linha não é de município ou o nome não está no cadastro, com aviso |
| `area_km2` | FLOAT | Sim | km² | ≥ 0 |
| `satelite` | STRING | Sim | — | — |
| `sensor` | STRING | Sim | — | — |
| `bioma` | STRING | Não | — | Amazônia ou Cerrado |

**PK:** `(data, classe, uf, municipio, municipio_id, bioma)`

O texto sai no dtype padrão do pandas instalado (`str` no pandas 3, `object` no 2), e o nulo do texto,
como `NaN` ou `None`. Nas feições da fonte, `pub_date` (PRODES Cerrado e Pampa) sai em `datetime64[ns]`.

## Restrições

- DETER só disponível para **Amazônia** e **Cerrado** (fail-fast com `ValueError`)
- Bioma é normalizado automaticamente (`"cerrado"` → `"Cerrado"`)
- No PRODES, a Amazônia é o recorte do bioma, não a Amazônia Legal da taxa de destaque do INPE ([fonte](../sources/desmatamento.md))

## Agregação

Cada linha é um grupo da chave primária, não uma feição. `area_km2` é a **soma das áreas publicadas
das feições** do grupo (`area_km` no PRODES, `areamunkm` no DETER) — não é a taxa oficial de
desmatamento. Qualquer área ausente no grupo torna o total ausente, sem soma parcial. `satelite` e
`sensor` trazem o valor publicado quando ele é único no grupo e ficam nulos quando o grupo mistura
valores; `meta.source_details["aggregation"]["heterogeneous"]` conta esses casos. A agregação exige
seleção reconciliada: se o limite local cortar a seleção, o dataset levanta `ContractViolationError`
em vez de publicar um agregado parcial. A contagem do WFS é conferida antes da descarga: quando passa
do `max_registros` (padrão 50.000), a recusa vem na hora, com a contagem na mensagem, sem baixar
nenhuma feição. Para uma seleção maior, use `max_registros=None`: o Cerrado inteiro de 2023 tem
68.620 feições, e a descarga leva minutos. Para as feições individuais, use
`agrobr.desmatamento.prodes` / `deter` (contratos `desmatamento.prodes_feicoes` e
`desmatamento.deter_feicoes`).

## Exemplo

```python
from agrobr import datasets

# PRODES — desmatamento anual consolidado (103 feições no DF em 2023)
df = await datasets.desmatamento("Cerrado", tipo="prodes", ano=2023, uf="DF")

# DETER — alertas de desmatamento no Acre, 1º trimestre de 2024
df = await datasets.desmatamento(
    "Amazônia", tipo="deter", uf="AC", inicio="2024-01-01", fim="2024-03-31"
)

# Com metadados (todos os anos do Cerrado no DF: 3.643 feições)
df, meta = await datasets.desmatamento("Cerrado", uf="DF", return_meta=True)
```
