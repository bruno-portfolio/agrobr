# progresso_safra v2.0

Progresso semanal de semeadura e colheita (CONAB).

## Fontes

| Prioridade | Fonte | Descrição |
|------------|-------|-----------|
| 1 | CONAB | Companhia Nacional de Abastecimento |

## Produtos (culturas)

`algodao`, `arroz`, `feijao_1`, `milho_1`, `milho_2`, `soja`, `trigo`

O parâmetro `produto` do dataset é normalizado via `normalizar_cultura()` para o nome esperado pela API CONAB (ex: `"soja"` → `"Soja"`).

## Schema

| Coluna | Tipo | Nullable | Unidade | Estável |
|--------|------|----------|---------|---------|
| `cultura` | str | ❌ | - | Sim |
| `safra` | str | ❌ | - | Sim |
| `operacao` | str | ❌ | - | Sim |
| `estado` | str | ❌ | - | Sim |
| `semana_atual` | str | ❌ | - | Sim |
| `pct_ano_anterior` | float | ✅ | fração | Sim |
| `pct_semana_anterior` | float | ✅ | fração | Sim |
| `pct_semana_atual` | float | ✅ | fração | Sim |
| `pct_media_5_anos` | float | ✅ | fração | Sim |
| `revisado` | bool | ✅ | - | Não |
| `n_estados` | int | ✅ | - | Não |
| `cobertura_area_pct` | float | ✅ | fração | Não |

`revisado` é opcional no contrato 1.1 e retorna como booleano nulável. A CONAB usa `*` para marcar valor revisado: `10%*` é lido como 0,10. A marca vale `True` se algum percentual da linha veio com o sufixo, `False` se nenhum veio e nulo se a linha não tem percentual numérico. O contrato 1.0 permanece em `CONAB_PROGRESSO_V1` e o 1.1 em `CONAB_PROGRESSO_V1_1`; o ativo é `CONAB_PROGRESSO_V2`.

No contrato 2.0, a linha "N estados" da planilha sai com `estado = "MEDIA_ESTADOS"`: é a média da própria CONAB dos estados
monitorados, não o Brasil. `n_estados` e `cobertura_area_pct` saem da nota "(Esses N estados correspondem a X% da área
cultivada)", sem recálculo (0,98 = 98%), e são nulos nas linhas de UF. `BR` só aparece se a CONAB publicar a linha "Brasil";
o filtro `estado="BR"` sem essa linha levanta `InvalidParameterError` com a cobertura publicada.

O `MEDIA_ESTADOS` não é a média simples das UFs do bloco. Os números são compatíveis com uma média ponderada pela área, com
pesos que a CONAB não publica: no trigo de 14 a 20/09/2026, a média da CONAB é 21,6%, e a média simples das UFs, 44,4%.

Texto com `%` sempre é dividido por 100, inclusive `0,5%` → `0,005` e `1%` → `0,01`. Valores numéricos de células Excel já expressos como fração são preservados. Blocos com datas semanais incompletas ou chaves duplicadas geram erro de parsing.

**Primary key:** `[cultura, safra, operacao, estado, semana_atual]`

**Constraints:** Valores percentuais entre 0.0 e 1.0 (fração, não %)

## Garantias

- PK única por combinação cultura + safra + operação + estado + semana
- Valores percentuais entre 0.0 e 1.0 (fração, não %)
- Dados semanais publicados pela CONAB
- `estado` é a UF de 2 letras (ex: MT, GO, PR); `MEDIA_ESTADOS` é a média da própria CONAB dos estados monitorados
  (`n_estados`, `cobertura_area_pct`), não a média simples das UFs nem o Brasil; `BR` só quando a CONAB publica Brasil
- `n_estados` e `cobertura_area_pct` saem da nota publicada, sem recálculo
- Percentuais com sufixo `*` preservam o valor e a indicação de revisão pela CONAB

## Exemplo

```python
from agrobr import datasets

# Async
df = await datasets.progresso_safra("soja")
df = await datasets.progresso_safra("milho_1", estado="MT", operacao="Semeadura")

# Com metadados
df, meta = await datasets.progresso_safra("soja", return_meta=True)

# Sync
from agrobr.sync import datasets
df = datasets.progresso_safra("soja")
```

## Schema JSON

Disponível em `agrobr/schemas/progresso_safra.json`.

```python
from agrobr.contracts import get_contract
contract = get_contract("progresso_safra")
print(contract.to_json())
```
