# clima

Clima mensal por UF ou observações diárias e horárias por estação. O modo UF aceita anos de 1981 ao corrente (pela data de Brasília); o histórico público INMET começa em 2000 e pode ser incompleto.

## Seleção e rotas

| `fonte` | UF | Estação |
|---|---|---|
| `None` | INMET API → INMET ZIP → NASA POWER | INMET API → INMET ZIP |
| `"inmet"` | API observacional exclusiva | API observacional exclusiva |
| `"inmet_historico"` | ZIP público exclusivo | ZIP público exclusivo |
| `"nasa_power"` | Ponto representativo da UF | Inválido |

Uma fonte explícita nunca aciona outra fonte. A API e o histórico do INMET são omitidos da rota automática por UF para anos anteriores a 2000, que vão direto ao NASA; `fonte="inmet"` ou `"inmet_historico"` com esses anos levanta `InvalidParameterError`. Ausência de observações da UF no ZIP permite avançar ao NASA; linhas com medições nulas continuam válidas.

```python
from agrobr import datasets

mensal, meta = await datasets.clima("GO", 2001, return_meta=True)
diario = await datasets.clima(
    estacao="A001", inicio="2000-12-30", fim="2001-01-02",
    fonte="inmet_historico",
)
horario = await datasets.clima(
    estacao="A001", inicio="2001-01-01", fim="2001-01-02",
    agregacao="horario", fonte="inmet_historico", as_polars=True,
)
```

`uf` e `ano` mantêm suas posições; os demais argumentos são nomeados. `return_meta=True` retorna `(frame, MetaInfo)`. `as_polars=True` converte após validação do contrato e requer o extra Polars.

No modo UF, `agregacao` omitida ou `"mensal"` retornam meses; `"diario"` e `"horario"` levantam `InvalidParameterError` (até a 1.1.0, o padrão era `"diario"`, e o modo UF o aceitava e devolvia meses). No modo estação, o padrão do dataset é diário, com `inicio` e `fim` inclusivos obrigatórios; somente `"diario"` e `"horario"` são aceitos. Não combine estação com UF/ano.

## Contrato mensal `CLIMA_V3` — 3.1

Registro: `clima`. PK: `[mes, uf]`. A versão 2.0 permanece disponível como contrato histórico. A versão 3.0 permite temperaturas nulas, além da precipitação já anulável. A 3.1 acrescenta a contagem das estações de chuva do INMET e a cobertura diária do mês, nas cinco últimas colunas.

| Coluna | Tipo | Nula | Unidade ou significado |
|---|---|---|---|
| `mes` | DATE | Não | Primeiro dia do mês |
| `uf` | STRING | Não | UF consultada |
| `precip_acum_mm` | FLOAT | Sim | mm |
| `temp_media` | FLOAT | Sim | °C |
| `temp_max_media` | FLOAT | Sim | °C |
| `temp_min_media` | FLOAT | Sim | °C |
| `num_estacoes` | INTEGER | Sim | Estações com linhas no mês; nulo no NASA |
| `umidade_media` | FLOAT | Sim | %, disponível no agregado NASA |
| `radiacao_media_mj` | FLOAT | Sim | MJ/m²/dia, média diária NASA |
| `vento_medio_ms` | FLOAT | Sim | m/s, disponível no agregado NASA |
| `fonte` | STRING | Não | `inmet` ou `nasa_power` |
| `lat` | FLOAT | Sim | Latitude do ponto NASA; nula no agregado INMET |
| `lon` | FLOAT | Sim | Longitude do ponto NASA; nula no agregado INMET |
| `agregacao_espacial` | STRING | Sim | `estacoes` ou `ponto_grade` |
| `base_tempo` | STRING | Sim | `UTC` no INMET; `LST` no NASA |
| `estacoes_chuva` | INTEGER | Sim | Estações INMET com chuva válida em todos os dias do mês, as únicas na média; nulo no NASA |
| `estacoes_chuva_parciais` | INTEGER | Sim | Estações INMET com chuva válida só em parte dos dias, fora da média; nulo no NASA |
| `dias` | INTEGER | Sim | Dias do mês com ao menos um valor diário válido |
| `data_inicio` | DATE | Sim | Primeiro desses dias |
| `data_fim` | DATE | Sim | Último desses dias |

As colunas de `lat` em diante são opcionais no contrato e preenchidas pelo dataset. A PK não inclui a fonte: concatenar resultados para a mesma UF/mês exige manter a identidade das consultas fora deste contrato.

## Contrato diário `CLIMA_ESTACAO_V1` — 1.0

Registro: `clima_estacao`. PK: `[data, estacao]`.

| Coluna | Tipo | Nula |
|---|---|---|
| `data` | DATE | Não |
| `estacao` | STRING | Não |
| `uf` | STRING | Sim |
| `temp_media` | FLOAT | Sim |
| `temp_max` | FLOAT | Sim |
| `temp_min` | FLOAT | Sim |
| `precipitacao_mm` | FLOAT | Sim |
| `umidade_media` | FLOAT | Sim |
| `radiacao_total_kj_m2` | FLOAT | Sim |

Temperatura em °C, precipitação em mm, umidade em % e radiação em kJ/m². O dia é UTC.

## Contrato horário `CLIMA_ESTACAO_HORARIA_V1` — 1.0

Registro: `clima_estacao_horaria`. PK: `[data, hora_utc, estacao]`. `data` e `estacao` são obrigatórias; `hora_utc` é uma string entre `0000` e `2300`, em passos de uma hora. `uf` é anulável.

As 13 medições são FLOAT anuláveis: `temperatura`, `temperatura_max`, `temperatura_min`, `ponto_orvalho` (°C); `umidade`, `umidade_max`, `umidade_min` (%); `precipitacao_mm` (mm); `pressao_hpa` (hPa); `vento_ms`, `vento_rajada_ms` (m/s); `vento_dir` (graus); `radiacao_kj_m2` (kJ/m²).

## Agregação, espaço e ausência

No INMET, chuva e radiação diárias somam as horas válidas. A chuva mensal da UF é a média simples dos totais das estações com chuva válida em todos os dias do mês (`estacoes_chuva`). O dia vale com pelo menos 1 hora válida, e a hora faltante conta como sem chuva, então o total de uma estação completa pode sair subestimado (em GO, jan/2026, faltam 830 horas somando as 18 estações completas, 6,2% das horas). A estação com o mês incompleto fica fora e é contada em `estacoes_chuva_parciais`; sem nenhuma estação completa, `precip_acum_mm` sai nulo, com `UserWarning` e a mesma mensagem em `MetaInfo.validation_warnings`. As temperaturas mensais são médias dos registros diários válidos; uma estação com mais dias válidos pode ter maior peso. `num_estacoes` conta estações com linhas, inclusive linhas sem medições válidas.

Grupos inteiramente ausentes permanecem nulos. Horas e dias faltantes não são completados, extrapolados ou convertidos em zero. `dias`, `data_inicio` e `data_fim` dão a cobertura diária de cada mês: no INMET, os dias com chuva ou temperatura válida em alguma estação; no NASA, os dias com algum parâmetro válido. Um mês parcial, como o corrente, sai com `dias` menor que o número de dias do mês e não é extrapolado. Em GO, dezembro de 2001, a A003 tem chuva nos 31 dias (270,8 mm) e a A002 em 28 (170,4 mm): o mensal é 270,8 mm, com `estacoes_chuva=1` e `estacoes_chuva_parciais=1`.

O NASA usa um ponto representativo configurado para a UF, preservado em `lat`/`lon`; isso não representa média territorial, centroide comprovado ou garantia de uma célula espacial comum às variáveis. Seu dia padrão é [LST](https://power.larc.nasa.gov/docs/services/api/temporal/daily/#time-standards), enquanto o INMET usa UTC. As duas rotas têm interpretações espaciais e temporais distintas.

O NASA POWER não é observação de estação: é a reanálise MERRA-2 em ponto de grade até o mês anterior e, no mês corrente, o GEOS-IT, de baixa latência, que a NASA substitui pelo MERRA-2 depois ([fontes da NASA POWER](https://power.larc.nasa.gov/docs/methodology/data/sources/)). O trecho de GEOS-IT sai em `source_details["periodos_baixa_latencia"]` e num aviso em `validation_warnings` (veja a [fonte](../sources/nasa_power.md)). A diferença para as estações do INMET pode ser grande. Em 2025, pelas 2 rotas explícitas: no DF, 1.226,6 mm no INMET (5 estações) × 728,4 mm no NASA (−41%), com a temperatura média mensal de 1,2 a 2,5 °C acima; em MT, 1.383,4 × 1.005,2 mm (−27%), com a temperatura até 2,9 °C acima. Na rota automática, os anos antes de 2000, ou sem observações da UF no ZIP, saem do NASA: a série montada com `fonte=None` pode ter um degrau que não é do clima. A coluna `fonte` e `MetaInfo.selected_source` dizem a rota de cada resultado.

## Proveniência e cobertura

`MetaInfo.selected_source` distingue `inmet`, `inmet_historico` e `nasa_power`; `attempted_sources` registra somente as rotas tentadas. A coluna mensal `fonte` continua `inmet` para ambas as rotas INMET.

`source_details` preserva acesso, base temporal, agregação, período solicitado e métodos por variável. No ZIP, inclui recursos com URL/SHA-256/tamanho/membros/data de coleta/cache, metadados de estação de cada edição e cobertura por estação: primeiro/último dia observado, horas presentes, horas do calendário e contagens válidas por medição. Membros selecionados sem linhas no período aparecem com zero horas e datas nulas. Indicadores de calendário e medições completos, anos sem membro, duplicatas idênticas removidas e avisos tornam as limitações consultáveis; não certificam qualidade científica.

A API agrega estações atualmente `Operante`; a rota ZIP seleciona os membros do ano, inclusive estações hoje `Pane`. Consulte a [documentação da fonte](../sources/inmet.md) para cache e falhas.

Cada item de `source_details.stations` inclui `layout_fingerprint`: assinatura SHA-256 das chaves de metadados e dos cabeçalhos normalizados, com versão do formato da assinatura e do parser. Valores das medições e da estação não entram nessa assinatura. Ela permite comparar layouts; o hash do recurso identifica os bytes completos.

Em contexto `deterministic`, o snapshot seleciona o ano quando omitido. Não trunca observações na data do snapshot nem congela a edição publicada. `source_details.deterministic` explicita esses limites; o hash identifica os bytes recebidos, não um arquivo recuperável por data histórica.

## Acesso

API observacional INMET exige token; [ZIPs anuais oficiais](https://portal.inmet.gov.br/dadoshistoricos) e NASA são públicos. A classificação do repositório é `livre`; consulte [licenças](../licenses.md) para o alcance dessa classificação.
