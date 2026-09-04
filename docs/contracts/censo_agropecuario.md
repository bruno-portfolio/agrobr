# censo_agropecuario v1.0

Dados do Censo Agropecuario 1995/2006/2017 por tema, UF e nivel territorial.

## Fontes

| Prioridade | Fonte | Descricao |
|------------|-------|-----------|
| 1 | IBGE Censo Agro | Censo Agropecuario 1995, 2006 e 2017 |

## Temas

`efetivo_rebanho`, `uso_terra`, `lavoura_temporaria`, `lavoura_permanente`, `preparo_solo`, `adubacao`, `calagem`, `agrotoxicos`, `praticas_agricolas`, `irrigacao`, `despesa_adubos`

### Cobertura temporal por tema

| Tema | 1995 | 2006 | 2017 |
|------|:----:|:----:|:----:|
| `efetivo_rebanho` | ✅ | — | ✅ |
| `uso_terra` | ✅ | — | ✅ |
| `lavoura_temporaria` | ✅ | — | ✅ |
| `lavoura_permanente` | ✅ | — | ✅ |
| `preparo_solo` | — | ✅ | ✅ |
| `adubacao` | — | ✅ | ✅ |
| `calagem` | — | ✅ | ✅ |
| `agrotoxicos` | — | ✅ | ✅ |
| `praticas_agricolas` | — | ✅ | ✅ |
| `irrigacao` | — | ✅ | ✅ |
| `despesa_adubos` | — | — | ✅ |

## Schema

| Coluna | Tipo | Nullable | Descricao |
|--------|------|----------|-----------|
| `ano` | int | ❌ | Ano de referencia (1995, 2006 ou 2017) |
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

### Variaveis por tema (temas originais)

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

### Novos temas — categorias

| Tema | Categorias (exemplos) |
|------|----------------------|
| `preparo_solo` | Cultivo convencional, Cultivo minimo, Plantio direto na palha |
| `adubacao` | Quimica, Organica, Adubacao verde (2006); Fez adubacao, Quimica, Organica (2017) |
| `calagem` | Fez aplicacao, Nao fez aplicacao |
| `agrotoxicos` | Utilizou, Nao utilizou |
| `praticas_agricolas` | Plantio em nivel, Rotacao de culturas, Pousio |
| `irrigacao` | Gotejamento, Pivo central, Inundacao, Aspersao |

## Garantias

- Dados decenais consolidados (Censo Agropecuario 1995, 2006 e 2017)
- Periodo de referencia 2017: outubro/2016 a setembro/2017
- Cache com TTL de 30 dias (dados estaveis)
- Parametro `ano` filtra por ano censal; `ano=None` retorna todos os anos disponiveis

## Exemplo

```python
from agrobr import ibge

# Efetivo de rebanho por UF (2017)
df = await ibge.censo_agro('efetivo_rebanho')

# Uso da terra em Mato Grosso
df = await ibge.censo_agro('uso_terra', uf='MT')

# Preparo do solo — ambos os anos
df = await ibge.censo_agro('preparo_solo')

# Irrigacao apenas 2017
df = await ibge.censo_agro('irrigacao', ano=2017)

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

## Temas Legados (FTP)

6 temas adicionais do Censo 1995/96 estão disponíveis via `censo_agro_legado()` com contrato separado. Ver [censo_agropecuario_legado](./censo_agropecuario_legado.md).
