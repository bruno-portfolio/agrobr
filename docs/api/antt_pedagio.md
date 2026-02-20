# ANTT Pedagio

Fluxo de veiculos em pracas de pedagio rodoviario.

## `fluxo_pedagio`

```python
from agrobr.alt import antt_pedagio

# Fluxo completo do ano
df = await antt_pedagio.fluxo_pedagio(ano=2023)

# Apenas veiculos pesados (proxy de transporte de graos)
df = await antt_pedagio.fluxo_pedagio(
    ano_inicio=2022,
    ano_fim=2023,
    apenas_pesados=True,
    uf="MT",
)

# Com metadados
df, meta = await antt_pedagio.fluxo_pedagio(ano=2023, return_meta=True)
```

### Parametros

| Parametro | Tipo | Default | Descricao |
|-----------|------|---------|-----------|
| `ano` | `int \| None` | `None` | Ano unico |
| `ano_inicio` | `int \| None` | `None` | Ano inicial do range |
| `ano_fim` | `int \| None` | `None` | Ano final do range |
| `concessionaria` | `str \| None` | `None` | Busca parcial |
| `rodovia` | `str \| None` | `None` | Ex: "BR-163" |
| `uf` | `str \| None` | `None` | Ex: "MT" |
| `praca` | `str \| None` | `None` | Busca parcial |
| `tipo_veiculo` | `str \| None` | `None` | "Passeio"/"Comercial"/"Moto" |
| `apenas_pesados` | `bool` | `False` | n_eixos >= 3 AND Comercial |
| `return_meta` | `bool` | `False` | Retorna MetaInfo |

### Colunas de saida

| Coluna | Tipo | Nullable | Notas |
|--------|------|----------|-------|
| `data` | DATE | N | 1o dia do mes |
| `concessionaria` | STRING | N | |
| `praca` | STRING | N | |
| `sentido` | STRING | Y | Crescente/Decrescente |
| `n_eixos` | INTEGER | N | 2-18 |
| `tipo_veiculo` | STRING | Y | Passeio/Comercial/Moto |
| `volume` | INTEGER | N | >= 0 |
| `rodovia` | STRING | Y | Do join com cadastro |
| `uf` | STRING | Y | Do join com cadastro |
| `municipio` | STRING | Y | Do join com cadastro |

## `pracas_pedagio`

```python
from agrobr.alt import antt_pedagio

# Todas as pracas
df = await antt_pedagio.pracas_pedagio()

# Filtro por UF
df = await antt_pedagio.pracas_pedagio(uf="SP")

# Filtro por rodovia
df = await antt_pedagio.pracas_pedagio(rodovia="BR-163")
```

### Parametros

| Parametro | Tipo | Default | Descricao |
|-----------|------|---------|-----------|
| `uf` | `str \| None` | `None` | Filtro de UF |
| `rodovia` | `str \| None` | `None` | Filtro de rodovia |
| `situacao` | `str \| None` | `None` | Ex: "Ativa" |
| `return_meta` | `bool` | `False` | Retorna MetaInfo |

## Uso sincrono

```python
from agrobr import sync

df = sync.alt.antt_pedagio.fluxo_pedagio(ano=2023, apenas_pesados=True)
df_pracas = sync.alt.antt_pedagio.pracas_pedagio(uf="SP")
```
