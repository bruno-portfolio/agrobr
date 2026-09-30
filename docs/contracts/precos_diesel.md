# precos_diesel v1.0

Preços semanais de diesel da ANP e médias mensais derivadas

Fonte: **ANP**. Registro do contrato: `precos_diesel`.

## Schema

| Coluna | Tipo | Nulo | Unidade | Descrição |
|---|---|---|---|---|
| `data` | date | Não | — | Início semanal ou primeiro dia do mês de início das semanas |
| `uf` | str | Não | — | Vazia para Brasil |
| `municipio` | str | Não | — | Vazio para Brasil e UF |
| `produto` | str | Não | — | `DIESEL` (S500 comum) ou `DIESEL S10` |
| `preco_venda` | float | Não | BRL/litro | Município: média simples dos postos; UF e Brasil: ponderado pelas vendas |
| `preco_compra` | float | Sim | BRL/litro | — |
| `n_postos` | int | Sim | — | Postos da amostra, no semanal; nula no mensal; não é o peso da UF e do Brasil |
| `margem` | float | Sim | BRL/litro | Diferença derivada entre as médias de revenda e distribuição |
| `periodo_inicio` | date | Não | — | Menor início das semanas selecionadas |
| `periodo_fim` | date | Não | — | Maior término publicado das semanas selecionadas |
| `nivel` | str | Não | — | — |
| `unidade` | str | Não | — | — |
| `agregacao` | str | Não | — | — |
| `n_semanas` | int | Não | — | — |
| `n_postos_media` | float | Sim | — | Média das contagens semanais; não são postos únicos |

**Chave primária:** Não definida no contrato. A seleção remove linhas semanais inteiramente idênticas com aviso; uma identidade semanal com valores conflitantes é recusada antes de calcular as médias.

Todas as colunas estáveis devem existir, inclusive anuláveis e em resultados vazios. Mudanças incompatíveis exigem versão major do contrato.

## Semântica e proveniência

Na UF e no Brasil, `preco_venda` é o publicado, ponderado pelas vendas das distribuidoras desde 31/10/2004; no município, é a média simples dos postos. `n_postos` é o tamanho da amostra, não o peso. `DIESEL` é o óleo diesel B S500 comum. Detalhes e exemplo na [fonte](../sources/anp_diesel.md).

A semana é identificada pelo início publicado e conserva seu fim, mesmo atravessando meses. `nivel` distingue município, UF e Brasil. O mensal é uma derivação do agrobr: média aritmética simples das médias semanais disponíveis, atribuídas ao mês da data inicial, sem ponderar por postos ou dias. Conserva `n_semanas` e `n_postos_media`; não representa uma pesquisa mensal independente da ANP. Linhas inteiramente idênticas na seleção são deduplicadas com `UserWarning` e registro em `MetaInfo.validation_warnings`, inclusive entre arquivos sobrepostos. Valores conflitantes para a mesma semana geram `ParseError`, tanto no semanal quanto no mensal. Nulos permanecem nulos. Período válido sem arquivo no catálogo gera `SourceUnavailableError`; datas malformadas ou invertidas geram `InvalidParameterError`. Não há chave primária contratual artificial.

`return_meta=True` retorna dados e `MetaInfo`, com fontes tentadas/selecionada, aquisição, versão contratual e diagnósticos da fonte. Estas publicações correntes não permitem selecionar uma revisão histórica via `deterministic`.

As colunas de texto usam o dtype padrão do pandas instalado (`str` no pandas 3 e `object` no pandas 2), inclusive no vazio. Datas usam `datetime64[ns]`, valores monetários usam `float64` e contagens usam `Int64`. Passe `as_polars` e `return_meta` por nome.

## Parâmetros

| Parâmetro | Tipo | Padrão |
|---|---|---|
| `produto` | `str` | `'DIESEL S10'` |
| `uf` | `str \| None` | `None` |
| `municipio` | `str \| None` | `None` |
| `inicio` | `str \| date \| None` | `None` |
| `fim` | `str \| date \| None` | `None` |
| `agregacao` | `str` | `'semanal'` |
| `nivel` | `str` | `'municipio'` |
| `as_polars` | `bool` | `False` |
| `return_meta` | `bool` | `False` |

## Exemplo

```python
from agrobr import contracts, datasets

df, meta = await datasets.precos_diesel("DIESEL S10", nivel="uf", uf="MT", inicio="2025-01-01", fim="2025-01-31", return_meta=True)
contracts.validate_dataset(df, "precos_diesel")
```

Para chamadas síncronas, use `from agrobr.sync import datasets` e remova `await`. `as_polars=True` requer `pip install agrobr[polars]`.

## Schema JSON e licença

`agrobr/schemas/precos_diesel.json` · `get_contract("precos_diesel")`.

`livre` — consulte as [licenças](../licenses.md) e os [detalhes da API/fonte](../api/anp_diesel.md).

## Semanas na virada do ano

Nas planilhas municipais, uma semana iniciada no fim de dezembro pode estar no arquivo do período seguinte. A seleção inclui o arquivo adjacente disponível quando necessário: a semana de 31/12/2023 a 06/01/2024 está em `2024–2025`, pertence ao filtro de dezembro de 2023 e entra na média desse mês. A consulta pode baixar dois arquivos mesmo com início e fim no mesmo ano.

O mensal desta API é calculado a partir das semanas selecionadas; não usa as planilhas mensais separadas que a ANP também publica. A ausência de preço de distribuição nos arquivos municipais mantém `preco_compra` e `margem` nulos. Os recibos de aquisição identificam todos os arquivos usados.
