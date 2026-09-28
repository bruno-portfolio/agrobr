# embarques_anec v1.1

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
| `ano` | int | ❌ | — | Coluna opcional; ano da edição impresso no boletim |
| `semana` | int | ❌ | — | Coluna opcional; semana da edição, de 1 a 53 |
| `data_inicio` | date | ✅ | — | Coluna opcional; primeiro dia do período, lido do rótulo |
| `data_fim` | date | ✅ | — | Coluna opcional; último dia do período, lido do rótulo |

**Chave primária:** `[porto, produto, periodo]`

## Produtos

| Código ANEC (`produto`) | Produto | Nome no agrobr |
|---|---|---|
| `soybean` | soja em grão | `soja` |
| `soybean_meal` | farelo de soja | `farelo_soja` |
| `maize` | milho | `milho` |
| `wheat` | trigo | `trigo` |
| `sorghum` | sorgo | `sorgo` |
| `ddgs` | DDGS (grãos secos de destilaria) | sem equivalente |

`produto` conserva o código da ANEC, em inglês. O nome no agrobr é o canônico de `normalize.crops`, o mesmo de `exportacao` e `estimativa_safra`; o `normalizar_cultura` converte cada código no nome da tabela, e `ddgs` fica como está.

## Garantias

- Os nomes das colunas nunca mudam; apenas adições são permitidas.
- `valor_ton` é maior ou igual a zero quando presente.
- Há uma linha por combinação de porto, produto e período.
- As datas vêm dos rótulos do boletim, nunca da semana ISO, e ficam nulas quando os dois rótulos não formam
  semanas consecutivas de 7 dias (veja a [fonte](../sources/anec.md)).

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

## Conferência dos boletins

Os seis produtos devem estar presentes nos cabeçalhos dos dois períodos. Uma coluna ausente interrompe a leitura, com causa ParseError preservada em SourceUnavailableError. Totais não são portos e células vazias do último porto publicado permanecem nulas. A conferência abrange as edições 04, 08, 12, 13, 34 e 36/2026; períodos são os rótulos do boletim, sem inferir datas pela semana ISO.
