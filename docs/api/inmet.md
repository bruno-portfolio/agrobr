# API INMET

O módulo oferece catálogo de estações, API observacional autenticada e arquivos históricos públicos de estações automáticas. As funções da fonte não alternam automaticamente entre API, ZIP e NASA; essa seleção pertence a [datasets.clima](../contracts/clima.md).

## Acesso e retorno

`estacao()` e `clima_uf()` usam a API observacional e requerem `AGROBR_INMET_TOKEN`. `estacoes()` e as três funções históricas abaixo funcionam sem token. Configure o token somente quando usar a API observacional:

```bash
export AGROBR_INMET_TOKEN=seu_token
```

Todas as funções aceitam `as_polars=False` e `return_meta=False`, só por nome. O padrão é pandas; `as_polars=True` requer o extra Polars. Com `return_meta=True`, o retorno é `(frame, MetaInfo)`.

## `historico_periodo`

```python
async def historico_periodo(
    codigo: str,
    inicio: str | date,
    fim: str | date,
    agregacao: str = "horario",
    *,
    as_polars: bool = False,
    return_meta: bool = False,
)
```

Consulta uma estação nos ZIPs de todos os anos do intervalo inclusivo. `codigo` identifica uma estação automática, como `A001`; datas aceitam `date` ou `YYYY-MM-DD`. O intervalo deve ser ordenado, de 2000 ao ano corrente. `agregacao` aceita `"horario"` e `"diario"`.

```python
from agrobr import inmet

df, meta = await inmet.historico_periodo(
    "A001", "2000-12-30", "2001-01-02",
    agregacao="diario", return_meta=True,
)
```

Anos sem membro da estação são diagnosticados em `meta.source_details["coverage"]["missing_station_years"]`; o resultado pode ser parcial ou vazio tipado. Isso não prova inexistência da estação ou ausência de observações fora do arquivo publicado. Uma falha de transporte, ZIP ou layout interrompe a consulta, em vez de apresentar apenas os anos que funcionaram.

## `historico_uf`

```python
async def historico_uf(
    uf: str,
    ano: int,
    *,
    as_polars: bool = False,
    return_meta: bool = False,
)
```

Retorna clima mensal da UF usando os membros do ZIP anual, inclusive estações hoje marcadas `Pane`. Não filtra pelo catálogo atual de estações operantes. O ano deve estar entre 2000 e o corrente.

```python
df, meta = await inmet.historico_uf("GO", 2001, return_meta=True)
```

Colunas: `mes` (data do primeiro dia do mês), `uf`, `precip_acum_mm`, `temp_media`, `temp_max_media`, `temp_min_media`, `num_estacoes`, `estacoes_chuva`, `estacoes_chuva_parciais`, `dias`, `data_inicio` e `data_fim`. Uma UF sem observações no arquivo retorna vazio tipado com diagnóstico na API de fonte; no dataset, isso permite fallback quando a seleção é automática.

## `historico`

```python
async def historico(
    codigo: str,
    ano: int,
    agregacao: str = "horario",
    *,
    as_polars: bool = False,
    return_meta: bool = False,
)
```

A API anual existente permanece disponível, de 2000 ao ano corrente, usando as mesmas observações históricas. O ano corrente e anos passados podem estar incompletos; não há garantia de 8.760 horas ou 365 dias. Se o membro da estação não existe no ZIP, esta função anual mantém `SourceUnavailableError`.

```python
df = await inmet.historico("A001", 2001, agregacao="diario")
```

## Schemas das observações

O retorno horário contém `data`, `hora_utc`, `estacao`, `uf` e 13 medições: `temperatura`, `temperatura_max`, `temperatura_min`, `umidade`, `umidade_max`, `umidade_min`, `precipitacao_mm`, `pressao_hpa`, `vento_ms`, `vento_dir`, `vento_rajada_ms`, `radiacao_kj_m2`, `ponto_orvalho`. `hora_utc` usa `HH00`, de `0000` a `2300`.

O retorno diário contém `data`, `estacao`, `uf`, `temp_media`, `temp_max`, `temp_min`, `precipitacao_mm`, `umidade_media`, `radiacao_total_kj_m2`. Todas as medições admitem ausência. Datas/horas INMET são UTC; temperatura usa °C, chuva mm, pressão hPa, umidade %, vento m/s, direção graus e radiação kJ/m².

## `estacoes`

```python
async def estacoes(
    tipo: str = "T",
    uf: str | None = None,
    apenas_operantes: bool = True,
    *,
    as_polars: bool = False,
    return_meta: bool = False,
)
```

Lista o catálogo atual: `tipo="T"` para automáticas ou `"M"` para convencionais. O padrão mantém somente `Operante`; use `apenas_operantes=False` para incluir outras situações. Retorna `codigo`, `nome`, `uf`, `situacao`, `tipo`, `latitude`, `longitude`, `altitude`, `inicio_operacao`, mais as colunas brutas da fonte (`DT_FIM_OPERACAO`, `CD_OSCAR`, `CD_WSI` e outras). A situação atual não descreve a situação em cada ano histórico. `tipo` fora de `"T"`/`"M"` e `uf` fora das siglas levantam `InvalidParameterError` antes da rede. `inicio_operacao` e `DT_FIM_OPERACAO` saem em `datetime64[ns, UTC]`, porque a fonte publica o instante com fuso: `2026-05-08T21:00:00.000-03:00` é `2026-05-09 00:00 UTC`. Data inexistente vira `NaT`, com aviso em `MetaInfo.validation_warnings`; catálogo vazio, resposta que não é lista JSON ou data fora do formato ISO levantam `ParseError`.

## `estacao`

```python
async def estacao(
    codigo: str,
    inicio: str | date,
    fim: str | date,
    agregacao: str = "horario",
    *,
    as_polars: bool = False,
    return_meta: bool = False,
)
```

Observações autenticadas de uma estação, com intervalo inclusivo e agregação horária ou diária. Períodos longos são divididos em blocos; falha de aquisição em qualquer bloco interrompe a consulta. HTTP 204 autenticado e medições naturalmente ausentes não são convertidos em zero. Período sem nenhuma observação levanta `SourceUnavailableError`, como o `historico()` de estação sem dados no ano.

## `clima_uf`

```python
async def clima_uf(
    uf: str,
    ano: int,
    *,
    as_polars: bool = False,
    return_meta: bool = False,
)
```

Agrega mensalmente a API observacional das estações automáticas atualmente operantes na UF. Retorna as mesmas doze colunas mensais de `historico_uf()`. A seleção de estações pode diferir entre as duas rotas. O ano vai de 2000 ao corrente e é conferido antes da rede; ano sem nenhuma observação levanta `SourceUnavailableError`.

## Agregação e proveniência

Chuva diária soma horas válidas; radiação diária segue a mesma preservação de ausência. A chuva mensal da UF é a média simples dos totais das estações com chuva válida em todos os dias do mês (`estacoes_chuva`). O dia vale com pelo menos 1 hora válida, e a hora faltante conta como sem chuva, então o total de uma estação completa pode sair subestimado (em GO, jan/2026, faltam 830 horas somando as 18 estações completas, 6,2% das horas). A estação com o mês incompleto fica fora e é contada em `estacoes_chuva_parciais`; sem nenhuma estação completa, `precip_acum_mm` sai nulo, com `UserWarning` e a mesma mensagem em `MetaInfo.validation_warnings`. Temperaturas mensais são médias dos registros diários válidos; não são necessariamente médias com pesos iguais por estação. `num_estacoes` conta estações com linhas no mês. Grupos sem medição válida permanecem nulos; nenhum período é completado ou extrapolado. `dias`, `data_inicio` e `data_fim` dão os dias do mês com chuva ou temperatura válida em alguma estação.

Nos retornos históricos, `source_details` inclui acesso, UTC, período, agregação, métodos por variável, recursos com URL/SHA-256/bytes/membros/coleta/cache e metadados das estações da própria edição. A cobertura informa horas observadas e esperadas, valores válidos por variável, primeiro/último dia, anos sem membro e avisos. Membros sem linhas no recorte permanecem explícitos com zero horas. Dados de automáticas são brutos; a validação do parser não equivale a consistência meteorológica.

O cache dos ZIPs é de processo, limitado a 256 MiB, com TTL de 1 hora para o ano corrente e 24 horas para anos anteriores. Anos podem ser reutilizados enquanto presentes e válidos no cache; arquivos maiores que o limite não são retidos. O hash identifica os bytes consultados e não congela a edição no portal.

## Uso via dataset e sync

`datasets.clima` usa API → ZIP → NASA por UF e API → ZIP por estação. `fonte` explícita é exclusiva. O contrato mensal é 3.1; os contratos diário e horário de estação são 1.0. A coluna mensal `fonte` permanece `inmet` no histórico, enquanto `meta.selected_source` é `inmet_historico`.

NASA preserva coordenadas do ponto representativo e usa LST; INMET usa UTC e múltiplas estações no agregado UF. Em contexto `deterministic`, o dataset seleciona o ano do snapshot quando omitido, sem truncar observações nem congelar a edição. Veja o [contrato completo](../contracts/clima.md).

```python
from agrobr.sync import inmet

df = inmet.historico_periodo("A001", "2000-12-30", "2001-01-02", agregacao="diario")
```

Fontes oficiais: [arquivos anuais](https://portal.inmet.gov.br/dadoshistoricos), [catálogo automático](https://portal.inmet.gov.br/paginas/catalogoaut). Consulte [acesso e limites da fonte](../sources/inmet.md).
