# censo_agropecuario v1.0

Dados do Censo Agropecuario 2017 por tema, UF e nivel territorial.

## Fontes

| Prioridade | Fonte | Descricao |
|------------|-------|-----------|
| 1 | IBGE Censo Agro | Censo Agropecuario 2017 |

## Temas

`efetivo_rebanho`, `uso_terra`, `lavoura_temporaria`, `lavoura_permanente`

## Schema

| Coluna | Tipo | Nullable | Descricao |
|--------|------|----------|-----------|
| `ano` | int | ❌ | Ano de referencia (2017) |
| `localidade` | str | ✅ | UF ou municipio |
| `localidade_cod` | int | ✅ | Codigo IBGE |
| `tema` | str | ❌ | Tema do censo |
| `categoria` | str | ❌ | Categoria dentro do tema |
| `variavel` | str | ❌ | Nome da variavel |
| `valor` | float64 | ✅ | Valor da variavel |
| `unidade` | str | ❌ | Unidade de medida |
| `fonte` | str | ❌ | Origem dos dados |

## Primary Key

`[ano, tema, categoria, variavel, localidade]`

## Formato

Long format: cada linha tem um par variavel/valor.

### Variaveis por tema

| Tema | Variavel | Unidade |
|------|----------|---------|
| `efetivo_rebanho` | `estabelecimentos` | unidades |
| `efetivo_rebanho` | `cabecas` | cabecas |
| `uso_terra` | `estabelecimentos` | unidades |
| `uso_terra` | `area` | hectares |
| `lavoura_temporaria` | `estabelecimentos` | unidades |
| `lavoura_temporaria` | `producao` | varia |
| `lavoura_temporaria` | `area_colhida` | hectares |
| `lavoura_permanente` | `estabelecimentos` | unidades |
| `lavoura_permanente` | `producao` | varia |
| `lavoura_permanente` | `area_colhida` | hectares |

## Garantias

- Dados decenais consolidados (Censo Agropecuario 2017)
- Periodo de referencia: outubro/2016 a setembro/2017
- Cache com TTL de 30 dias (dados estáveis)

## Exemplo

```python
from agrobr import ibge

# Efetivo de rebanho por UF
df = await ibge.censo_agro('efetivo_rebanho')

# Uso da terra em Mato Grosso
df = await ibge.censo_agro('uso_terra', uf='MT')

# Lavoura temporaria por municipio
df = await ibge.censo_agro('lavoura_temporaria', nivel='municipio', uf='PR')

# Com metadados
df, meta = await ibge.censo_agro('efetivo_rebanho', return_meta=True)
```

## Schema JSON

Disponivel em `agrobr/schemas/censo_agropecuario.json`.

```python
from agrobr.contracts import get_contract
contract = get_contract("censo_agropecuario")
print(contract.to_json())
```

## Niveis Territoriais

| Nivel | Descricao |
|-------|-----------|
| `brasil` | Total nacional |
| `uf` | Por Unidade Federativa (default) |
| `municipio` | Por municipio |
