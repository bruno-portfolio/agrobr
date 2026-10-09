# ANTT Pedágio

Contagens de veículos em praças de pedágio rodoviário. `frequencia="mensal"` seleciona recursos mensais publicados; `"diaria"` seleciona recursos diários. Recurso ausente gera erro, sem trocar a frequência.

## `fluxo_pedagio`

```python
from agrobr.alt import antt_pedagio

df = await antt_pedagio.fluxo_pedagio(ano=2023)
df, meta = await antt_pedagio.fluxo_pedagio(
    ano=2025, frequencia="diaria",
    inicio="2025-08-01", fim="2025-08-01",
    enriquecer=False, return_meta=True,
)
```

### Parâmetros

| Parâmetro | Tipo | Padrão |
|---|---|---|
| `ano` | `int \| None` | `None` |
| `ano_inicio` | `int \| None` | `None` |
| `ano_fim` | `int \| None` | `None` |
| `concessionaria` | `str \| None` | `None` |
| `rodovia` | `str \| None` | `None` |
| `uf` | `str \| None` | `None` |
| `praca` | `str \| None` | `None` |
| `tipo_veiculo` | `str \| None` | `None` |
| `apenas_pesados` | `bool` | `False` |
| `as_polars` | `bool` | `False` |
| `return_meta` | `bool` | `False` |
| `frequencia` | `Literal['mensal', 'diaria']` | `'mensal'` |
| `tipo_cobranca` | `str \| None` | `None` |
| `inicio` | `str \| date \| datetime \| None` | `None` |
| `fim` | `str \| date \| datetime \| None` | `None` |
| `enriquecer` | `bool` | `True` |
| `max_linhas` | `int` | `500000` |
| `max_memoria_bytes` | `int` | `268435456` |

Use `ano` ou `ano_inicio`/`ano_fim`, sem combiná-los. Sem seleção de ano, o padrão é o ano anterior e o corrente. `as_polars`, `return_meta` e o que vem depois deles são somente nomeados. `inicio` e `fim` são datas civis e inclusivas (`date`, `datetime`, cuja hora é descartada, ou texto `AAAA-MM-DD` ou `DD/MM/AAAA`), dentro dos anos escolhidos; referências mensais exigem dia 1. `enriquecer=False` dispensa o cadastro de praças e não aceita filtros de UF/rodovia. Filtros textuais são literais, não expressões regulares. `tipo_veiculo` aceita `Comercial`, `Moto` ou `Passeio`, sem diferenciar caixa e acento; outro valor gera `InvalidParameterError` antes da rede. `tipo_cobranca` compara o nome inteiro, sem caixa e acento; `concessionaria` e `praca` comparam por trecho, sem caixa. Quando esses filtros não casam nenhum registro, o resultado vem vazio com aviso em `UserWarning` e em `meta.validation_warnings`.

### Saída — contrato 3.0

| Coluna | Tipo | Nulo |
|---|---|---|
| `data` | date | Não |
| `concessionaria` | str | Não |
| `praca` | str | Não |
| `sentido` | str | Sim |
| `n_eixos` | int | Sim |
| `tipo_veiculo` | str | Sim |
| `volume` | int | Não |
| `rodovia` | str | Sim |
| `uf` | str | Sim |
| `municipio` | str | Sim |
| `categoria_eixo` | str | Sim |
| `tipo_cobranca` | str | Sim |
| `frequencia` | str | Não |

**Chave primária:** `data`, `concessionaria`, `praca`, `sentido`, `tipo_veiculo`, `categoria_eixo`, `tipo_cobranca`, `frequencia`.

Todas as 13 colunas são obrigatórias, inclusive anuláveis. `data` é o dia publicado ou o primeiro dia do mês. `volume` é contagem inteira exata; todas as ocorrências validadas contribuem dentro da chave, exceto a segunda cópia de um bloco concessionária × mês publicado duas vezes com linhas idênticas (mantida uma cópia, com aviso). Linha com volume que não é contagem (fracionário ou negativo) é descartada com aviso, e referência mensal publicada fora do dia 1 vale para o seu mês, com aviso; o ano não é descartado e as linhas afetadas ficam em `source_details`. Cobranças manuais, automáticas e outras permanecem separadas por `tipo_cobranca`; `frequencia` também integra a identidade.

Os textos saem como publicados, sem os espaços externos, e `sentido` sai em maiúsculas. O texto sai em `string[python]`, e não no dtype padrão do pandas instalado: o limite de memória conta cada texto retido pela identidade do objeto. `n_eixos` só recebe contagem textual explícita. Categoria tarifária numérica, inclusive um número isolado, não comprova contagem física e fica nula. `apenas_pesados=True` exige tipo comercial e contagem ou faixa explícita que garanta pelo menos três eixos. Categorias desconhecidas não recebem contagem inventada.

### Aquisição e limites

CSVs são baixados para arquivos temporários em disco, com tetos de 512 MiB por CSV, 1 GiB de temporários retidos e 3 GiB transferidos por aquisição, incluindo tentativas. O parser usa por padrão 500 mil linhas de saída (grupos da chave) e 256 MiB de memória de trabalho retida estimada; a estimativa não é teto de RSS do processo. Exceder orçamento gera `ResourceLimitError`, sem devolver resultado parcial. Um recorte de data pequeno ainda baixa e valida o arquivo anual inteiro.

Não há cache persistente de CSV. Cada aquisição consulta novamente o catálogo CKAN, reutiliza a sessão e fecha temporários ao concluir, falhar ou cancelar. Redirecionamentos são rejeitados. HTML HTTP 200 de WAF ou manutenção gera `SourceUnavailableError`; CSV malformado gera `ParseError`.

`MetaInfo` identifica schema/contrato 3.0, fonte `antt_pedagio`, primeiro CSV de tráfego, recursos e hashes. `raw_content_hash` e `raw_content_size` são os do manifesto da consulta e das aquisições; o total recebido de catálogos e CSVs em todas as tentativas fica em `source_details["received_bytes"]`, e o dos CSVs que entraram no dado, em `data_file_bytes`; `source_details` preserva consulta, manifesto, estatísticas de validação/EOF, cobertura e diagnóstico do enriquecimento. O cadastro corrente de praças não é geografia histórica. Enriquecimento opcional indisponível recebe diagnóstico; com `uf`/`rodovia`, cadastro indisponível gera erro. Esses filtros devolvem só praças comprovadamente na UF/rodovia pelo cadastro: registro sem vínculo único (concessionária e praça sem diferença de caixa nem de espaços, com o nome anterior da concessionária levado ao atual; ver a [fonte](../sources/antt_pedagio.md)) ou sem o campo pedido no cadastro sai com aviso, e linhas, volume e pares excluídos ficam em `source_details["geographic_filter"]`. `deterministic` não é suportado para recursos CKAN mutáveis.

Veja a [fonte e o dicionário](../sources/antt_pedagio.md).

## `pracas_pedagio`

O contrato do cadastro é 2.0. O cabeçalho oficial `municipal` fornece `municipio`, com precedência da coluna canônica quando ambas existem; `municipal` continua na saída, como publicada. `km_m` sai em `float64` (quilômetro), `ano_do_pnv_snv` em `Int64` e `data_da_inativacao` em `datetime64[ns]` (publicada em `DD/MM/AAAA`; data inexistente, como `31/02/2024`, vira nulo com aviso, e texto fora dessa forma gera `ParseError`). O texto sai no dtype padrão do pandas instalado (`str` no pandas 3, `object` no 2). `rodovia` compara sem caixa, espaço, hífen e zero à esquerda (`"BR 40"`, `"br-040"` e `"BR-40"` são a mesma rodovia); `situacao` compara por trecho, sem caixa. Filtro que não casa nenhuma praça devolve vazio com aviso que lista os valores publicados. `as_polars` e `return_meta` são somente nomeados. O cadastro corrente enriquece o fluxo; não representa necessariamente a geografia histórica do ano consultado.

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
| `situacao` | `str \| None` | `None` | Ex: "Ativo" |
| `as_polars` | `bool` | `False` | Retorna polars.DataFrame |
| `return_meta` | `bool` | `False` | Retorna MetaInfo |

## Uso sincrono

```python
from agrobr import sync

df = sync.alt.antt_pedagio.fluxo_pedagio(ano=2023, apenas_pesados=True)
df_pracas = sync.alt.antt_pedagio.pracas_pedagio(uf="SP")
```
