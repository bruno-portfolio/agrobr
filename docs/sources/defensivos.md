# Agrofit/MAPA — Defensivos Agricolas

> **Licenca:** CC-BY 4.0 (Portal de Dados Abertos MAPA).
> Classificacao: `livre`

Dados de agrotoxicos registrados no Brasil via sistema Agrofit do Ministerio
da Agricultura, Pecuaria e Abastecimento (MAPA).

## Visao Geral

| Campo | Valor |
|-------|-------|
| **Operador** | MAPA — Ministerio da Agricultura |
| **Website** | [dados.agricultura.gov.br](https://dados.agricultura.gov.br) |
| **Licenca** | `livre` — CC-BY 4.0 |
| **Formato** | CSV (`;` separador) |
| **Atualizacao** | Continua (cache 24h) |
| **Cobertura** | ~8K formulados, ~267K autorizacoes, ~2.8K tecnicos |

## Dados Disponiveis

### Produtos Formulados

Agrotoxicos comerciais registrados. Cada registro e um produto unico
identificado por `nr_registro`.

**Colunas:** `nr_registro`, `marca_comercial`, `ingrediente_ativo`, `titular`,
`classe`, `formulacao`, `classe_toxicologica`, `classe_ambiental`, `organicos`,
`modo_de_acao`

### Autorizacoes de Uso

Relacao 1:N com formulados — cada autorizacao vincula um produto a uma cultura
e praga especifica.

**Colunas:** `nr_registro`, `marca_comercial`, `ingrediente_ativo`, `titular`,
`classe`, `cultura`, `praga`, `praga_nome_comum`, `modalidade_de_emprego`

### Produtos Tecnicos

Ingredientes ativos antes da formulacao comercial.

**Colunas:** `nr_registro`, `marca_comercial`, `ingrediente_ativo`, `titular`,
`classe`, `grupo_quimico`, `nome_cientifico`, `classe_toxicologica`, `classe_ambiental`

## API

```python
from agrobr import defensivos

# Todos os formulados
df = await defensivos.formulados()

# Filtrar por ingrediente ativo
df = await defensivos.formulados(ingrediente_ativo="glifosato")

# Apenas organicos
df = await defensivos.formulados(organicos="SIM")

# Autorizacoes para soja
df = await defensivos.autorizacoes(cultura="soja")

# Produtos tecnicos
df = await defensivos.tecnicos()

# Com metadados
df, meta = await defensivos.formulados(return_meta=True)
```

## Notas Tecnicas

- CSV de formulados e grande (~100MB+). Timeout de download: 300s
- Parser corrige encoding Windows-1252 (en-dash `\x96` → UTF-8)
- Coluna composta `INGREDIENTE_ATIVO(GRUPO_QUIMICO)(CONCENTRACAO)` em tecnicos
  e separada automaticamente via regex
- Filtros usam `str.contains()` case-insensitive (exceto `organicos` que e exato)
- Cache local em CSV com TTL de 24h

## Fonte

- URL: `https://dados.agricultura.gov.br/dataset/6c913699-e82e-4da3-a0a1-fb6c431e367f`
- Formato: CSV (`;`)
- Atualizacao: continua
- Licenca: `livre` — CC-BY 4.0
