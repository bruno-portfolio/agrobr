# fertilizante v2.0

Entregas mensais de fertilizantes ao mercado brasileiro (total nacional, `uf="BR"`).

## Fontes

| Prioridade | Fonte | Descrição |
|------------|-------|-----------|
| 1 | ANDA | Associação Nacional para Difusão de Adubos |

## Produtos

`total`

!!! warning "Migração da v1.0"
    `npk`, `ureia`, `map`, `dap`, `ssp`, `tsp` e `kcl` nunca eram filtrados
    na fonte: o volume total era apenas reetiquetado com o valor solicitado.
    A v2.0 recusa esses valores com `ValueError` para impedir dado incorreto.

## Schema

| Coluna | Tipo | Nullable | Unidade | Estável |
|--------|------|----------|---------|---------|
| `ano` | int | ❌ | - | Sim |
| `mes` | int | ❌ | - | Sim |
| `uf` | str | ✅ | - | Sim |
| `produto_fertilizante` | str | ❌ | - | Sim |
| `volume_ton` | float | ✅ | ton | Sim |

**Primary key:** `[ano, mes, uf, produto_fertilizante]`

**Constraints:** `ano >= 2000`, `mes` entre 1 e 12, `volume_ton >= 0`

## Garantias

- Nomes de coluna nunca mudam (só adicionam)
- `ano` sempre >= 2000
- `mes` entre 1 e 12
- Valores numéricos sempre >= 0
- `produto_fertilizante` é sempre `total`

Sem `ano`, o dataset usa o ano corrente pela data de Brasília. O ano corrente é parcial: o resultado avisa em
`validation_warnings` e em `UserWarning`, traz `ano_em_curso` e `meses_cobertos` em `source_details` e muda até a
edição do ano fechado.

## Exemplo

```python
from agrobr import datasets

# Async
df = await datasets.fertilizante(ano=2024)
df = await datasets.fertilizante(ano=2024, produto="total")

# Com metadados
df, meta = await datasets.fertilizante(ano=2024, return_meta=True)

# Sync
from agrobr.sync import datasets
df = datasets.fertilizante(ano=2024)
```

## Schema JSON

Disponível em `agrobr/schemas/fertilizante.json`.

```python
from agrobr.contracts import get_contract
contract = get_contract("fertilizante")
print(contract.to_json())
```

## Requisitos

```bash
pip install agrobr[pdf]
```


## Cobertura e validação da publicação

O catálogo público contém 11 PDFs, de 2016 a 2026, todos com entregas nacionais mensais (`uf="BR"`). O boletim 2026 publica janeiro a junho; meses posteriores vazios não são zero. Como nenhum deles publica recorte estadual, a 2.0.0 tirou o parâmetro `uf` da fonte e do dataset (guia de migração 2.0, seção 50).

O parser 3 exige a seção `Fertilizantes Entregues ao Mercado (em toneladas de produto)` e procura o ano somente nela. Se o ano ou essa identificação estiver ausente, a fonte levanta `ParseError`, e o dataset também (`"Todas as fontes falharam por layout"`), com o motivo da fonte em `errors`. Produção, importação, exportação e relações de troca do mesmo PDF não podem substituir entregas. Valores publicados e o contrato 2.0 permanecem iguais.

As flags de saída são somente por nome; anos e meses usam `Int64` anulável, e `volume_ton` usa `float64`.
