# embarques_anec v1.0

Embarques semanais da ANEC por porto, produto e período do relatório.

## Fonte

| Prioridade | Fonte | Descrição |
|------------|-------|-----------|
| 1 | ANEC | Relatório semanal em PDF |

O dataset requer o extra de PDF:

```bash
pip install agrobr[pdf]
```

## Schema

| Coluna | Tipo | Nullable | Unidade | Restrições |
|--------|------|----------|---------|------------|
| `porto` | str | ❌ | — | Nome canônico do porto |
| `produto` | str | ❌ | — | Produto canônico da ANEC |
| `periodo` | str | ❌ | — | `last_week` ou `current_week` |
| `valor_ton` | float64 | ✅ | ton | >= 0 quando presente |

**Chave primária:** `[porto, produto, periodo]`

## Produtos

`soybean`, `soybean_meal`, `maize`, `wheat`, `ddgs`, `sorghum`.

## Garantias

- Os nomes das colunas nunca mudam; apenas adições são permitidas.
- `valor_ton` é maior ou igual a zero quando presente.
- Há uma linha por combinação de porto, produto e período.

## Exemplo

```python
from agrobr import datasets

df = await datasets.embarques_anec(ano=2026)
df = await datasets.embarques_anec(
    ano=2026,
    semana=4,
    produto="soja",
    tipo="efetivado",
)
df, meta = await datasets.embarques_anec(ano=2026, return_meta=True)
```

## Schema JSON

Disponível em `agrobr/schemas/embarques_anec.json`.

```python
from agrobr.contracts import get_contract

contract = get_contract("embarques_anec")
print(contract.primary_key)  # ["porto", "produto", "periodo"]
print(contract.to_json())
```

## Licença

Classificação `zona_cinza`: a ANEC não publica termos de uso explícitos. O uso
comercial pode exigir autorização da associação.
