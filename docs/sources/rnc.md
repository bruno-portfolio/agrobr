# RNC/SNPC — CultivarWeb/MAPA

O CultivarWeb publica exportações de cultivares registradas no RNC e de registros de proteção do SNPC. O agrobr mantém essas famílias separadas, com identidade, contrato e proveniência próprios. A classificação do projeto é `livre`; os fundamentos e limites estão na [documentação de licenças](../licenses.md). Não se atribui uma licença específica aos CSVs com base apenas no acesso público.

## Visão geral

| Campo | Valor |
|---|---|
| Operador | MAPA |
| Portal | [CultivarWeb](https://sistemas.agricultura.gov.br/snpc/cultivarweb) |
| Recursos | Exportação CSV de registradas; exportação CSV de protegidas |
| Formato observado | CSV separado por vírgula, UTF-8, campos entre aspas quando necessário |
| Temporalidade | Exportação corrente; sem seletor de revisões históricas |
| Cache do SDK | CSV bruto e manifesto, TTL de 24 horas desde a aquisição |

Na captura de **07/09/2026**, as exportações continham 38.335 linhas RNC e 5.424 linhas SNPC. São contagens daquela aquisição, não tamanhos permanentes nem garantia de todos os registros históricos. A exportação RNC observada apresentava somente a situação `REGISTRADA`; não se afirma cobertura de registros encerrados a partir dela. O SNPC incluía seis situações, com proteção definitiva, provisória, cancelada, expirada por prazo, extinta por renúncia e nula.

## Dados entregues

### Cultivares registradas

Dez colunas: `cultivar`, `nome_comum`, `nome_cientifico`, `grupo`, `situacao`, `nr_formulario`, `nr_registro`, `data_registro`, `data_validade`, `mantenedor`.

O número de registro identifica a linha dentro da exportação. O formulário não é chave: há números repetidos e vazios. Nomes de cultivar e mantenedor vazios publicados são preservados, assim como identificadores textuais e suas eventuais pontuações/zeros iniciais. Não se divide automaticamente espécie ou mantenedor composto.

### Cultivares protegidas

Doze colunas: `cultivar`, `nome_cientifico`, `nome_comum`, `nr_processo`, `situacao`, `nr_certificado`, `inicio_protecao`, `termino_protecao`, `titular`, `representante_legal`, `melhoristas`, `termino_protecao_texto`.

A chave é o número de processo, com escopo da exportação. Certificados repetidos entre processos não são deduplicados. Os campos de titulares e melhoristas podem conter várias pessoas; seu texto composto é preservado, sem inferir cardinalidade pelo delimitador.

`termino_protecao_texto` conserva a célula de término após remover espaços externos. A coluna de data distingue datas válidas de valores sem uma data determinada: vazio e `até a emissão do certificado definitivo` resultam em `NaT`, mantendo o texto ao lado. Na captura mencionada, havia 5.251 datas, 171 condições e dois vazios. A condição ocorre em mais de uma situação administrativa e não equivale automaticamente a proteção provisória.

As quatro colunas de datas são civis `datetime64[ns]`, sem fuso, e permitem ausência publicada. Qualquer outro texto não vazio inválido causa erro explícito. As demais colunas são texto; o SDK remove apenas espaços externos e mantém vazios legítimos.

## Acesso e validação

Uma sessão pública segue os formulários de pesquisa e exportação. Um GET obtém um token CSRF antes de cada POST, inclusive em novas tentativas; não há credencial pessoal. O client usa timeout, retry e User-Agent rotativo. Tokens/cookies não integram os metadados públicos.

O CSV completo recebido é validado antes dos filtros: layout integral, largura das linhas, modelos Pydantic, datas, chave e contrato. Mudança de layout, erro de conteúdo ou divergência entre total da pesquisa e quantidade de registros aborta a coleta. O fingerprint SHA256 descreve a estrutura do cabeçalho; o hash do CSV identifica os bytes da aquisição. Nenhum deles certifica exatidão geográfica ou situação jurídica.

Os filtros existentes são substrings literais sem distinção de caixa. `nr_registro`/`nr_formulario` na família RNC e `nr_processo`/`nr_certificado` no SNPC usam igualdade textual. Todos removem espaços externos. O recorte não reduz a validação da população nem o volume transferido na aquisição corrente.

## Cache, metadados e cobertura

O cache guarda CSV bruto e manifesto em pacote local por família. O TTL de 24 horas usa o horário UTC de recebimento, não a última leitura ou mtime do arquivo. Pacotes expirados, incompatíveis ou corrompidos são ignorados; a escrita substitui o pacote de forma atômica após a validação. Em cache, o corpo é novamente parseado e contratado, preservando os metadados da aquisição original. `use_cache=False` não lê nem grava o cache. Onde fica e como limpar: [O que o agrobr grava no disco](../advanced/disco.md).

`MetaInfo` distingue cache/rede, hash/tamanho bruto, horário original, expiração, tempos de fetch/parsing/filtro e fonte selecionada. `source_details` registra a pesquisa e a exportação, estatísticas anteriores aos filtros e a seleção local.

Cobertura `count_matched` significa apenas que a população validada coincide com o total declarado pela pesquisa precedente. Sem total verificável, o estado é `unknown`. Pesquisa e exportação são requisições distintas; nem contagem igual nem cache estabelecem um snapshot transacional ou uma revisão histórica imutável.

## Uso

```python
from agrobr import rnc

registradas = await rnc.registradas(especie="soja")
protegidas, meta = await rnc.protegidas(
    nr_processo="21806.000132/2019",
    use_cache=False,
    return_meta=True,
)
```

Consulte as assinaturas e os tipos na [API RNC/SNPC](../api/rnc.md). Os datasets `cultivares_registradas` e `cultivares_protegidas` reutilizam essas capacidades e contratos 1.0, sem fallback para outra fonte. O contexto `deterministic` é recusado pelos datasets antes de I/O, pois não existe seleção de revisão histórica nessa rota.

## Limites

A entrega cobre os campos das duas exportações consultadas. Não une registro a proteção por nomes, não calcula vigência jurídica, não reproduz uma edição passada por data e não amplia automaticamente o catálogo para outros documentos ou serviços do MAPA. Quantidades e situações variam com a publicação corrente.

## Exportação de 18/09/2026

Os CSVs da exportação de 18/09/2026 têm 38.325 registros RNC e 5.424 registros SNPC. A contagem HTML da pesquisa que precede cada CSV coincide com ele; essa concordância não garante snapshot transacional nem cobertura histórica.

No RNC, os 22.946 formulários, 4.256 nomes de cultivar e 4.271 mantenedores vazios publicados permanecem texto vazio; ambas as datas estão preenchidas nessa exportação. Há 924 grupos de formulários repetidos, com 15.142 ocorrências. No SNPC, o término tem 5.267 datas, 155 condições e dois vazios; as 155 condições e os dois vazios produzem data nula, preservando o texto. Há 102 certificados repetidos entre processos distintos, com 204 ocorrências. Nenhuma dessas repetições de identificadores secundários é deduplicada.

A aquisição original em `fetched_at` conserva UTC com fuso explícito, inclusive no cache e no dataset. Na fonte e no dataset, `fetch_timestamp` coincide com a aquisição. As datas civis da tabela são independentes desses instantes.
