# CONAB CEASA/PROHORT

Precos diarios de atacado de hortifruti em 43 CEASAs do Brasil (48 produtos).

## `conab.ceasa_precos()`

Precos mais recentes de atacado por produto x CEASA.

```python
import agrobr

df = await agrobr.conab.ceasa_precos(produto="tomate")
```

### Parametros

| Parametro | Tipo | Obrigatorio | Descricao |
|-----------|------|-------------|-----------|
| `produto` | `str` | Nao | Filtrar por produto (ex: "tomate", "ABACAXI"). Case-insensitive |
| `ceasa` | `str` | Nao | Filtrar por CEASA (ex: "CEAGESP - SAO PAULO", "SAO PAULO"). Case-insensitive, busca parcial |
| `return_meta` | `bool` | Nao | Se True, retorna `(DataFrame, MetaInfo)` |

### Colunas de Retorno

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `data` | datetime | Data do preco (por CEASA, extraida do header) |
| `produto` | str | Nome do produto (ex: TOMATE, ABACAXI) |
| `categoria` | str | FRUTAS ou HORTALICAS |
| `unidade` | str | KG, UN ou DZ |
| `ceasa` | str | Nome da CEASA (ex: CEAGESP - SAO PAULO) |
| `ceasa_uf` | str | UF da CEASA (ex: SP) |
| `preco` | float | Preco em R$ (nulls filtrados) |

---

## `conab.ceasa_produtos()`

Lista dos 48 produtos monitorados pelo PROHORT.

```python
import agrobr

produtos = agrobr.conab.ceasa_produtos()
```

### Retorno

Lista ordenada de strings (ex: `["ABACATE", "ABACAXI", ..., "VAGEM"]`).

---

## `conab.lista_ceasas()`

Lista das 43 CEASAs com UF.

```python
import agrobr

ceasas = agrobr.conab.lista_ceasas()
for c in ceasas[:3]:
    print(c["nome"], c["uf"])
```

### Retorno

Lista de dicts com `nome` e `uf` para cada CEASA, ordenada por nome.

---

## `conab.ceasa_categorias()`

Categorias de produtos (FRUTAS, HORTALICAS).

```python
import agrobr

cats = agrobr.conab.ceasa_categorias()
print(f"Frutas: {len(cats['FRUTAS'])}")
print(f"Hortalicas: {len(cats['HORTALICAS'])}")
```

---

## Uso Sincrono

```python
from agrobr import sync

df = sync.conab.ceasa_precos(produto="tomate")
produtos = sync.conab.ceasa_produtos()
ceasas = sync.conab.lista_ceasas()
```

## Exemplos

### Preco do tomate em SP

```python
import agrobr

df = await agrobr.conab.ceasa_precos(produto="tomate", ceasa="SAO PAULO")
print(df[["ceasa", "preco", "unidade"]])
```

### Todas as frutas

```python
import agrobr

df = await agrobr.conab.ceasa_precos()
frutas = df[df["categoria"] == "FRUTAS"]
print(frutas.groupby("produto")["preco"].mean().sort_values(ascending=False))
```

### Comparar precos entre CEASAs

```python
import agrobr

df = await agrobr.conab.ceasa_precos(produto="tomate")
print(df[["ceasa", "ceasa_uf", "preco"]].sort_values("preco"))
```

## Fonte dos Dados

- **Provedor:** CONAB — Companhia Nacional de Abastecimento
- **Sistema:** PROHORT (Programa Brasileiro de Modernizacao do Mercado Hortigranjeiro)
- **Frequencia:** Diaria (precos de atacado)
- **Cobertura:** 48 produtos (20 frutas, 28 hortalicas), 43 CEASAs, 21 UFs
- **Formato:** JSON (Pentaho CDA REST API)
- **Licenca:** zona_cinza (credenciais publicas embutidas, API nao documentada oficialmente)
- **Portal:** [Portal de Informacoes CONAB](https://portaldeinformacoes.conab.gov.br/mercado-atacadista-hortigranjeiro.html)
