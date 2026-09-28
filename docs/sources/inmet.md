# INMET — Meteorologia

O Instituto Nacional de Meteorologia publica observações, catálogo de estações e arquivos históricos. O agrobr oferece duas rotas distintas: API observacional e ZIPs anuais de estações automáticas. Os ZIPs não representam todo o acervo ou os produtos do [BDMEP](https://bdmep.inmet.gov.br/).

## Acesso

| Serviço | Autenticação | Funções |
|---|---|---|
| Catálogo atual de estações | Público | `estacoes` |
| API observacional | `AGROBR_INMET_TOKEN` | `estacao`, `clima_uf` |
| ZIPs anuais de automáticas | Público, sem token | `historico`, `historico_periodo`, `historico_uf` |

```bash
export AGROBR_INMET_TOKEN=seu_token
```

A variável é necessária somente na rota observacional. Ausência ou rejeição do token resulta em `SourceUnavailableError`; o segredo não deve ser incluído em URLs publicadas, logs ou relatórios. A API do INMET exige o token no caminho da URL (`/token/<rota>/<token>`): o agrobr o mascara no log do httpx e do httpcore, na resposta, no histórico de redirecionamento e no erro, mas um proxy, firewall ou log de acesso fora do processo vê a URL completa. O catálogo público [estacoes/T](https://apitempo.inmet.gov.br/estacoes/T) não exige esse token.

O [catálogo histórico oficial](https://portal.inmet.gov.br/dadoshistoricos) oferece anos de 2000 ao corrente. Na verificação de 6 de setembro de 2026 havia 27 ZIPs, e 2026 estava rotulado até 31/08/2026. Isso não garante cobertura anual completa, atualização diária do ZIP ou imutabilidade de anos passados. A [orientação oficial](https://portal.inmet.gov.br/noticias/saiba-como-acessar-os-dados-meteorol%25C3%25B3gicos-dispon%25C3%25ADveis-no-site-do-inmet) descreve acesso, horário UTC e dados automáticos brutos, sem consistência meteorológica.

## Consultas públicas

```python
from agrobr import inmet, datasets

horas = await inmet.historico("A001", 2001)
dias, meta = await inmet.historico_periodo(
    "A001", "2000-12-30", "2001-01-02",
    agregacao="diario", return_meta=True,
)
mensal = await inmet.historico_uf("GO", 2001)
mensal, meta = await datasets.clima("GO", 2001, fonte="inmet_historico", return_meta=True)
```

As funções históricas anuais e por intervalo aceitam de 2000 ao ano corrente. O padrão da fonte para estação é horário; `agregacao="diario"` entrega dias. `historico_uf` entrega meses, com `mes` como data do primeiro dia, não número inteiro. Todas aceitam `as_polars` e `return_meta`. Consulte [assinaturas e colunas](../api/inmet.md).

## Seleção espacial e fórmulas

`clima_uf` consulta as estações automáticas atualmente `Operante` na API. `historico_uf` usa membros e metadados do arquivo anual, inclusive estações hoje `Pane`. Por exemplo, A003/Morrinhos tem observações no [ZIP 2001](https://portal.inmet.gov.br/uploads/dadoshistoricos/2001.zip), apesar dessa situação no catálogo atual. Situação atual e ausência de membro não comprovam datas de ativação ou encerramento.

Chuva e radiação diárias somam medições horárias válidas. A chuva mensal da UF é a média simples dos acumulados das estações com chuva válida em todos os dias do mês; as de mês incompleto ficam fora (`estacoes_chuva_parciais`) e, sem nenhuma completa, o valor sai nulo, com aviso. Em GO, dezembro/2001, a A003 tem chuva nos 31 dias (270,8 mm) e a A002 em 28 (170,4 mm): o mensal é 270,8 mm. O dia vale com pelo menos 1 hora válida, e a hora faltante conta como sem chuva, então o total de uma estação completa pode sair subestimado (em GO, jan/2026, faltam 830 horas somando as 18 estações completas, 6,2% das horas). Temperaturas mensais são médias dos registros diários válidos, sem garantir pesos iguais entre estações. `num_estacoes` conta estações com linhas no mês, não sensores com cobertura completa; `dias`, `data_inicio` e `data_fim` dão os dias do mês com chuva ou temperatura válida.

Nenhuma hora/dia ausente é imputada ou extrapolada. Grupos inteiramente sem medições permanecem nulos: A001 em 20/05/2000 possui 24 linhas sem medições válidas no [ZIP 2000](https://portal.inmet.gov.br/uploads/dadoshistoricos/2000.zip), e chuva, temperatura e radiação permanecem nulas. Validação de layout e contrato não transforma observações brutas em uma série meteorológica consistida.

## Aquisição, cache e falhas

Cada consulta histórica baixa os ZIPs anuais necessários e seleciona os CSVs pertinentes. Tamanho varia por ano; os arquivos modernos podem ter dezenas de MB. O cache em memória do processo tem limite total de 256 MiB, TTL de 1 hora para o ano corrente e 24 horas para anos anteriores, com descarte dos menos recentemente usados. ZIPs acima do limite não são retidos. Consultas simultâneas do mesmo ano no mesmo loop compartilham a aquisição; não há cache persistente de revisões nem consulta HTTP condicional por ETag nesta implementação.

Falhas HTTP/transporte, ZIP inválido, CRC ou layout abortam a consulta histórica; anos bem-sucedidos não são apresentados como uma aquisição completa após erro. Observações idênticas repetidas são contadas uma vez; registros conflitantes para estação/data/hora causam erro.

`historico_periodo` diagnostica anos sem membro e pode retornar observações parciais ou vazio tipado. `historico` anual mantém erro quando o membro está ausente. `historico_uf` pode retornar vazio tipado quando não há observações da UF. Nenhum desses resultados prova ausência de dados em outros serviços do INMET.

A API observacional divide intervalos longos em blocos de até 365 dias; falha de aquisição interrompe a consulta. HTTP 204 autenticado e valores naturalmente ausentes são tratados como ausência de observações, sem criar chuva zero.

## Proveniência e edição

Com `return_meta=True`, `source_details` registra acesso, UTC, período solicitado, agregação e métodos espaciais por variável. Na rota ZIP, também preserva URL, SHA-256, bytes, membros, instante de coleta e origem de cache de cada recurso; os metadados de estação pertencem à edição consultada. Altitude e coordenadas atuais do catálogo não substituem silenciosamente metadados antigos.

`stations[].layout_fingerprint` identifica o layout de cada CSV por SHA-256 de chaves de metadados e cabeçalhos normalizados, sem incluir valores. A assinatura registra sua versão e a do parser e é distinta do hash integral do ZIP.

`coverage` expõe horas presentes, calendário solicitado, contagens válidas por medição, primeiro/último dia observado e anos sem membro. Estações selecionadas sem linhas no recorte permanecem explícitas com zero horas. `complete_calendar` e `complete_measurements` distinguem presença de linhas de validade das medições; avisos e contagem de duplicatas removidas complementam o diagnóstico. Esses campos não certificam qualidade científica ou completude de todo o acervo.

## Camada datasets

Sem `fonte`, `datasets.clima` tenta API → ZIP → NASA por UF e API → ZIP por estação. Uma fonte explícita é exclusiva; NASA não atende modo estação. Uma UF sem observações no ZIP permite fallback automático, enquanto medições nulas em linhas existentes permanecem válidas. Histórico anterior a 2000 não é uma rota elegível.

O contrato mensal `clima` é 3.1; `clima_estacao` diário e `clima_estacao_horaria` são 1.0. A coluna mensal `fonte` continua `inmet`; `selected_source="inmet_historico"` identifica o acesso por ZIP. No NASA, `lat`/`lon` preservam um ponto representativo fixo da UF, sem garantir centroide ou célula comum às variáveis. `agregacao_espacial` distingue `estacoes` e `ponto_grade`; `base_tempo` distingue UTC INMET de [LST NASA](https://power.larc.nasa.gov/docs/services/api/temporal/daily/#time-standards).

Um snapshot no dataset seleciona o ano quando omitido, mas não trunca o resultado na data nem congela a edição da fonte. O hash permite identificar os bytes recebidos; não oferece recuperação as-of. Consulte [contratos e exemplos](../contracts/clima.md).

## Licença e limites da verificação

O repositório classifica INMET como `livre` em [licenças](../licenses.md). A evidência oficial de acesso público e gratuito não foi tratada como uma licença específica de redistribuição adicional.

A verificação examinou os ZIPs completos de 2000/2001 e um CSV de 2026, além do catálogo e de uma resposta NASA. Não validou todas as edições modernas, dados convencionais do BDMEP ou observações autenticadas. As informações de catálogo acima são datadas, não contagens permanentes.
