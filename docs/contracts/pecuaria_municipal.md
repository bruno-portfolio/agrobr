# pecuaria_municipal v1.0

Efetivo de rebanhos e producao de origem animal por UF ou municipio.

## Fontes

| Prioridade | Fonte | Descricao |
|------------|-------|-----------|
| 1 | IBGE PPM | Pesquisa da Pecuaria Municipal |

## Produtos

### Rebanhos

`bovino`, `bubalino`, `equino`, `suino_total`, `suino_matrizes`, `caprino`, `ovino`, `galinaceos_total`, `galinhas_poedeiras`, `codornas`

### Producao de origem animal

`leite`, `ovos_galinha`, `ovos_codorna`, `mel`, `casulos`, `la`

## Schema

| Coluna | Tipo | Nullable | Descricao |
|--------|------|----------|-----------|
| `ano` | int | ❌ | Ano de referencia |
| `localidade` | str | ✅ | UF ou municipio |
| `localidade_cod` | int | ✅ | Codigo IBGE |
| `especie` | str | ❌ | Nome da especie/produto |
| `valor` | float64 | ✅ | Valor (unidade varia por especie) |
| `unidade` | str | ❌ | Unidade de medida |
| `fonte` | str | ❌ | Origem dos dados |

## Primary Key

`[ano, especie, localidade]`

## Garantias

- Dados consolidados do ano civil (referencia 31/dez)
- Latencia tipica: Y+1 (dados disponiveis no ano seguinte)
- Serie historica desde 1974

## Exemplo

```python
from agrobr import datasets

# Rebanho bovino por UF
df = await datasets.pecuaria_municipal("bovino", ano=2023)

# Producao de leite por municipio
df = await datasets.pecuaria_municipal("leite", ano=2023, nivel="municipio", uf="MG")

# Filtrar por UF
df = await datasets.pecuaria_municipal("bovino", ano=2023, uf="MT")

# Com metadados
df, meta = await datasets.pecuaria_municipal("bovino", ano=2023, return_meta=True)
```

## Schema JSON

Disponivel em `agrobr/schemas/pecuaria_municipal.json`.

```python
from agrobr.contracts import get_contract
contract = get_contract("pecuaria_municipal")
print(contract.to_json())
```

## Niveis Territoriais

| Nivel | Descricao |
|-------|-----------|
| `brasil` | Total nacional |
| `uf` | Por Unidade Federativa (default) |
| `municipio` | Por municipio |
