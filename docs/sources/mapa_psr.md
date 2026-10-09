# MAPA PSR — Seguro Rural

> **Licenca:** CC-BY (dados publicos governo federal).
> Classificacao: `livre`

Dados abertos do SISSER/MAPA — Sistema de Subvencao Economica ao Premio do
Seguro Rural. Apolices e sinistros (indenizacoes) do seguro rural brasileiro
com subvencao federal, publicados pelo Ministerio da Agricultura.

As indenizações são associadas ao ano de contratação da apólice. Esta saída
não informa o trimestre do evento ou do pagamento.

## Instalacao

Nao requer dependencias opcionais. Usa apenas httpx + pandas (core).

## API

```python
from agrobr.alt import mapa_psr

# Sinistros de seguro rural (indenizacoes pagas)
df = await mapa_psr.sinistros()

# Filtrar por cultura e UF
df = await mapa_psr.sinistros(produto="SOJA", uf="MT")

# Filtrar por ano ou range
df = await mapa_psr.sinistros(ano=2023)
df = await mapa_psr.sinistros(ano_inicio=2020, ano_fim=2024)

# Filtrar por evento preponderante
df = await mapa_psr.sinistros(evento="seca")

# Filtrar pelo município, por nome ou código IBGE (ver "Município e código IBGE")
df = await mapa_psr.sinistros(municipio="Sorriso", uf="MT")
df = await mapa_psr.apolices(municipio=4305108)

# Todas as apolices (incluindo sem sinistro)
df = await mapa_psr.apolices()

# Apolices filtradas
df = await mapa_psr.apolices(produto="MILHO", uf="PR", ano=2023)

# API sincrona
from agrobr.sync import alt
df = alt.mapa_psr.sinistros(produto="SOJA")
df = alt.mapa_psr.apolices(uf="MT")
```

## Parametros — `sinistros`

| Parametro | Tipo | Default | Descricao |
|---|---|---|---|
| `produto` | str \| None | None | Filtro por cultura (busca parcial, accent-insensitive, ex: "cafe" matcha "CAFE ARABICA") |
| `uf` | str \| None | None | Filtro por UF (sigla, ex: "MT") |
| `ano` | int \| None | None | Filtro de ano unico (ex: 2023) |
| `ano_inicio` | int \| None | None | Ano inicial do range (inclusive) |
| `ano_fim` | int \| None | None | Ano final do range (inclusive) |
| `municipio` | int \| str \| None | None | Código IBGE de 7 dígitos ou nome inteiro do município; ver "Município e código IBGE" |
| `evento` | str \| None | None | Filtro por evento preponderante (ex: "seca") |
| `as_polars` | bool | False | Se True, retorna `polars.DataFrame`; somente nomeado, como `return_meta` |
| `return_meta` | bool | False | Retorna tupla (DataFrame, MetaInfo) |

O `produto` é filtrado por trecho do nome publicado no CSV (`NM_CULTURA_GLOBAL`). Não há catálogo estático: a lista vem
no próprio CSV (311 MB no período mais recente), então uma cultura fora do publicado só aparece como resultado vazio,
depois da descarga.

## Colunas — `sinistros`

| Coluna | Tipo | Nullable | Descricao |
|---|---|---|---|
| `nr_apolice` | str | Nao | Numero da apolice |
| `ano_apolice` | int | Nao | Ano da apolice |
| `uf` | str | Nao | Sigla UF da propriedade |
| `municipio` | str | Sim | Nome do municipio |
| `cd_ibge` | str | Sim | Código IBGE do município; nulo quando o MAPA publica "-" no lugar do geocódigo (1.516 apólices entre 2006 e 2025), e `municipio` segue com o nome publicado |
| `cultura` | str | Nao | Cultura segurada (uppercase) |
| `classificacao` | str | Sim | Classificacao do produto (AGRICOLA, PECUARIO, etc.) |
| `evento` | str | Nao | Evento preponderante (lowercase) |
| `area_total` | float | Sim | Area total segurada (ha) |
| `valor_indenizacao` | float | Nao | Valor da indenizacao (R$) — sempre > 0 |
| `valor_premio` | float | Sim | Premio liquido (R$) |
| `valor_subvencao` | float | Sim | Subvencao federal (R$) |
| `valor_limite_garantia` | float | Sim | Limite de garantia (R$) |
| `produtividade_estimada` | float | Sim | Produtividade estimada; o MAPA não publica a unidade |
| `produtividade_segurada` | float | Sim | Produtividade segurada; o MAPA não publica a unidade |
| `nivel_cobertura` | float | Sim | Nível de cobertura em fração (0,65 = 65%): produtividade segurada ÷ estimada, como o MAPA publica |
| `seguradora` | str | Sim | Razao social da seguradora |
| `cod_municipio` | int | Sim | Código IBGE do município (`Int64`) tirado de `cd_ibge`; nulo quando `cd_ibge` é nulo |
| `inicio_vigencia` | datetime | Sim | Início da vigência (`DT_INICIO_VIGENCIA`); nulo onde o início é igual ao fim (vigência não publicada; todo o período 2006–2015) |
| `fim_vigencia` | datetime | Sim | Fim da vigência (`DT_FIM_VIGENCIA`); nulo junto com `inicio_vigencia` quando início e fim publicados são iguais; data ilegível ou com ano fora de 1900–2099 anula só esta coluna |
| `data_apolice` | datetime | Sim | Data da apólice (`DT_APOLICE`); o ano é o `ano_apolice` |

## Parametros — `apolices`

| Parametro | Tipo | Default | Descricao |
|---|---|---|---|
| `produto` | str \| None | None | Filtro por cultura (busca parcial, accent-insensitive) |
| `uf` | str \| None | None | Filtro por UF |
| `ano` | int \| None | None | Filtro de ano unico |
| `ano_inicio` | int \| None | None | Ano inicial do range |
| `ano_fim` | int \| None | None | Ano final do range |
| `municipio` | int \| str \| None | None | Código IBGE de 7 dígitos ou nome inteiro do município; ver "Município e código IBGE" |
| `as_polars` | bool | False | Se True, retorna `polars.DataFrame`; somente nomeado, como `return_meta` |
| `return_meta` | bool | False | Retorna tupla (DataFrame, MetaInfo) |

## Colunas — `apolices`

Mesmas colunas de `sinistros`, mais `taxa`. Em `apolices`, `valor_indenizacao` é anulável e vale 0 ou nulo na apólice sem sinistro, `evento` sai vazio (`""`) na apólice sem sinistro e `seguradora` não é nula (faz parte da chave).

Nas apólices, `valor_premio` sai como publicado pelo MAPA, inclusive quando negativo, com `UserWarning` e `meta.validation_warnings`. Nos sinistros, o contrato mantém `valor_premio` ≥ 0.

| Coluna | Tipo | Nullable | Descricao |
|---|---|---|---|
| `taxa` | float | Sim | Taxa do prêmio em fração (0,1369 = 13,69%): prêmio líquido ÷ limite de garantia, como o MAPA publica |

## Pipeline de dados

1. Resolve os períodos pelos filtros de ano: 2006-2015, 2016-2024 e 2025 saem do dicionário fixo; o ano depois de 2025 (e, sem `ano_fim`, até o ano corrente) é procurado no catálogo do pacote no CKAN do MAPA. Ano sem arquivo no catálogo sai com aviso (`UserWarning` e `meta.validation_warnings`), e, sem nenhum arquivo, `meta.source_url` aponta o catálogo consultado. Com o catálogo fora do ar, os anos fixos seguem com aviso; sem nenhum ano fixo no pedido, a falha sobe como `SourceUnavailableError`
2. Baixa um período por vez para arquivo temporário, recebendo o CSV por partes
3. Confere o encoding no arquivo inteiro (UTF-8 com suporte a BOM → Windows-1252 → ISO-8859-1), o separador (`;` ou `,`), as aspas, a unicidade dos cabeçalhos, a largura dos registros e os anos das apólices, e anota a maior `DT_APOLICE` do arquivo (em `source_details["corpos"][i]["ultima_apolice"]`), antes de qualquer filtro
4. Lê blocos de 10 mil linhas e remove colunas PII (NM_SEGURADO, NR_DOCUMENTO_SEGURADO) e geolocalização
5. Normaliza cabeçalhos/textos e aplica filtros de ano, UF, cultura, município e código IBGE em cada bloco
6. Converte os números selecionados (float64 para monetários, int para ano)
7. Para sinistros, filtra VALOR_INDENIZACAO > 0 e EVENTO_PREPONDERANTE não vazio; reúne o resultado e encerra o temporário de cada período

Cabeçalhos com acentos, como `VALOR_INDENIZAÇÃO`, são normalizados antes do
mapeamento. Se a coluna de indenização não puder ser identificada, `sinistros`
levanta `ParseError`; apólices sem valor de indenização não viram sinistros.

## Município e código IBGE

`municipio=` aceita o código IBGE de 7 dígitos (`int` ou `str`) ou o nome inteiro do município, sem
diferenciar caixa e acento, e é resolvido por `normalize.resolver_municipio` antes do download. Pedaço
de nome, nome de outra UF e nome de mais de um município sem `uf` geram `InvalidParameterError` com os
candidatos. O filtro compara o código publicado em `CD_GEOCMU`, então pega também as apólices que o
MAPA rotula com o nome do distrito em `NM_MUNICIPIO_PROPRIEDADE`: em Caxias do Sul (código 4305108),
em 2024, 274 das 693 apólices do código trazem "Caxias do Sul", e as outras 419 saem como Fazenda
Souza (241), Criúva (78), Vila Oliva (48), Vila Seca (32) e Santa Lúcia do Piaí (20). As apólices
publicadas com "-" no lugar do geocódigo (1.516 entre 2006 e 2025, com `cd_ibge` nulo) entram quando
o rótulo é o nome inteiro do município, na mesma UF. Sem a coluna `CD_GEOCMU` no arquivo, o filtro usa
só o nome inteiro e a UF.

## MetaInfo

```python
df, meta = await mapa_psr.sinistros(return_meta=True)
print(meta.source)           # "mapa_psr"
print(meta.source_method)    # "httpx"
print(meta.parser_version)   # 5
print(meta.records_count)    # varia por filtro
```

## Nota de desempenho

Os CSVs do SISSER podem ser grandes. O módulo baixa apenas os períodos
necessários e filtra cada bloco antes de converter todas as colunas numéricas.
Filtros seletivos reduzem a memória usada pelo processamento; a fonte ainda
exige o download integral de cada período selecionado.

O arquivo temporário ocupa espaço em disco e é encerrado em sucesso, erro ou
cancelamento. A API retorna um DataFrame completo: consultas sem filtros ainda
precisam de memória proporcional ao resultado. A validação integral do CSV e a
leitura por partes podem aumentar o tempo de processamento. Timeout de leitura
HTTP: 180 segundos.

O resultado é ordenado por `ano_apolice`. A ordem entre apólices do mesmo ano
não é garantida; use uma ordenação explícita por suas colunas de interesse.

## Datasets

- [`seguro_rural`](../contracts/seguro_rural.md) — wraps `mapa_psr.apolices()` e `mapa_psr.sinistros()` via `tipo=` dispatch

## Fonte

- URL: `https://dados.agricultura.gov.br/dataset/sisser3`
- Formato: CSV (3 arquivos por periodo)
- Atualizacao: anual
- Historico: 2006+
- Licenca: `livre` (CC-BY, dados publicos governo federal)

## Integridade e período das apólices

O CSV inteiro é validado antes da aplicação dos filtros. Cabeçalho duplicado, registro com campos a mais ou a menos e ano de apólice inválido geram `ParseError` com a posição do registro; a leitura não descarta essas linhas silenciosamente. Campos entre aspas podem conter separadores e quebras de linha. O parser é versão 5; o contrato de apólices está em 2.1 e o de sinistros em 1.2.

**Chave e registro publicado em dobro (contrato `mapa_psr_apolices` desde a 2.0).** A chave das apólices é `nr_apolice`, `ano_apolice`, `uf`, `cultura`, `cd_ibge` e `seguradora`: o número da apólice só é único dentro da seguradora (em 2007, 2008, 2009, 2011 e 2012 o MAPA publica o mesmo número em duas seguradoras, com área e prêmio diferentes). Um registro publicado duas vezes e igual em todas as colunas que o agrobr entrega (em 2009, a apólice 1977000249501 da Mapfre, com a proposta reenviada) sai uma vez só, com aviso (`warn_once`) e a contagem em `source_details["duplicatas_colapsadas"]`. Repetição da chave com qualquer valor diferente levanta `ContractViolationError` (no dataset, `SourceUnavailableError`).

`ano_apolice` é o ano de contratação da apólice, conforme o dicionário SISSER; não identifica a data do evento ou do pagamento. `sinistros` seleciona indenização positiva com evento não vazio. Zero publicado continua zero em `apolices`; valores ausentes continuam nulos. Não se arredondam valores monetários a centavos. Números de apólice e códigos geográficos conservam seus zeros iniciais. Fora o registro publicado em dobro e idêntico, descrito acima, nenhuma linha é deduplicada.

Na captura de 18/09/2026, o catálogo disponibilizava três CSVs, até 2025. O arquivo 2025 tinha indenizações ausentes, o que não demonstra ausência de sinistros. O EOF comprova a leitura completa do arquivo publicado, sem garantir cobertura completa do programa ou atualização dos pagamentos.

**Ano publicado pela metade.** O MAPA publica o ano corrente antes de ele terminar e não atualiza o arquivo depois: em 08/10/2026, o CSV 2025 continuava o de 03/09/2025, com 46.137 apólices de 02/01/2025 a 21/08/2025 e nenhuma de soja, milho 1ª safra, arroz ou algodão. A falta não se explica pelo corte em agosto: no arquivo 2016–2024, 43.157 apólices de soja de 2024 têm `DT_APOLICE` de janeiro a agosto. `apolices(produto="soja", ano=2025)` sai vazio porque o arquivo publicado não traz a soja, o que não demonstra ausência de seguro. Quando a última `DT_APOLICE` do arquivo cai antes de 1º de outubro do próprio ano e esse ano está no pedido, `apolices`, `sinistros` e `datasets.seguro_rural` avisam (`UserWarning` e `meta.validation_warnings`): "PSR: o arquivo publicado pelo MAPA tem apólices até 21/08/2025; 2025 pode estar incompleto". O corte vem dos arquivos fechados: de 2006 a 2024, o ano que mais cedo terminou foi 2022, em 24/10. Um arquivo cortado entre outubro e dezembro passa sem aviso, e o ano no meio de um arquivo não é avaliado, porque o arquivo segue depois dele.

**Datas da apólice (contratos `mapa_psr_apolices` 2.1 e `mapa_psr_sinistros` 1.2).** `inicio_vigencia`, `fim_vigencia` e `data_apolice` vêm de `DT_INICIO_VIGENCIA`, `DT_FIM_VIGENCIA` e `DT_APOLICE`, publicadas em `dd/mm/aaaa` nos três arquivos, e saem em `datetime64[ns]`. O ano de `data_apolice` é o `ano_apolice` em todas as 1.712.385 linhas de 2006 a 2025 (conferido em 08/10/2026). A vigência existe a partir de 2016: no arquivo 2006–2015, início e fim são iguais em todas as 617.683 linhas (22/07/2016 ou 23/07/2016, posteriores às apólices), ou seja, a vigência não foi publicada. Onde o início é igual ao fim, as duas colunas de vigência saem nulas, com o aviso "PSR: vigência não publicada (início igual ao fim) em N registro(s); saem nulas. Use data_apolice" (`UserWarning` e `meta.validation_warnings`, uma vez por consulta; N conta os registros publicados que passaram nos filtros, antes de colapsar o registro em dobro). Data ilegível ou com ano fora de 1900–2099 sai `NaT`, com um aviso por coluna na consulta. Arquivo sem a coluna dá a coluna inteira nula. `DT_PROPOSTA` não é publicada: a apólice reenviada de 2009 difere só nela, e o registro em dobro sai uma vez só.

Campos textuais preservam literais como `NULL`, `NA`, `None` e `N/A`, sujeitos apenas às normalizações de espaços e caixa já documentadas; eles não são convertidos em ausência pelo leitor CSV. Campo textual vazio continua vazio, exceto `cd_ibge`, que sai nulo. Campos numéricos mantêm a conversão vigente: valores ausentes ou não interpretáveis ficam nulos, sem transformar tokens textuais em zero.
