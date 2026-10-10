# MapBiomas

## Visao Geral

| Campo | Valor |
|-------|-------|
| **Provedor** | Projeto MapBiomas — Rede multi-institucional |
| **Dados** | Cobertura e uso da terra, transições entre classes |
| **Acesso** | Download XLSX público nos repositórios oficiais do MapBiomas |
| **Formato** | XLSX direto ou dentro de ZIP; cobertura municipal lida em fluxo com openpyxl |
| **Autenticação** | Nenhuma |
| **Licença** | CC BY 4.0, com atribuição ao MapBiomas; classificação `livre` |
| **Série histórica** | 1985-2025 (coleção 11); 1985-2024 (coleção 10) |

## Origem dos Dados

O MapBiomas e um projeto colaborativo multi-institucional que produz mapas anuais de cobertura e uso da terra do Brasil a partir de imagens de satelite Landsat (30m de resolução). Os dados são gerados via classificação automática usando Google Earth Engine.

O agrobr acessa as **estatísticas tabulares** de áreas em hectares por classe, bioma, estado e cruzamento municipal, disponibilizadas como planilhas XLSX. O `READ_ME` da publicação municipal define o cruzamento bioma × estado × município, de 1985 a 2025 na Coleção 11. Os rasters não são retornados por estas APIs. [Publicação oficial de cobertura](https://brasil.mapbiomas.org/iniciativas-e-produtos/cobertura-e-uso-da-terra/cobertura-30m/cobertura/).

O uso exige referência à fonte, coleção, data de acesso e link, conforme o formato de citação do projeto. A FAQ oficial declara CC BY 4.0. [Acesso, licença e citação](https://brasil.mapbiomas.org/faq/?tema=dados).

## Coleções

O MapBiomas publica coleções anuais com melhorias metodologicas:

| Coleção | Data | Período |
|---------|------|---------|
| 11 (atual) | Agosto 2026 | 1985-2025 |
| 10 | Agosto 2025 | 1985-2024 |

O parâmetro `colecao` aceita `10` e `11`. O padrão `None` usa a coleção 11. A escolha determina o arquivo, a aba, a legenda e o limite de anos; coleções não suportadas levantam `InvalidParameterError` antes do download. Cada lançamento revisa a série histórica. Fixar a coleção identifica a edição, mas não congela o arquivo: preserve também bytes e hashes para reprodução. A coleção nova que o MapBiomas lançar pede uma versão nova do agrobr; até lá, o padrão segue na 11.

As duas APIs (`cobertura` e `transicao`) e o dataset `datasets.uso_do_solo` preservam essa seleção. Com `return_meta=True`, `meta.data_sources` identifica `mapbiomas_colecao_10` ou `mapbiomas_colecao_11`, e `meta.source_url` registra o arquivo consultado.

## Estrutura dos Dados

### Cobertura

Dados em formato wide: uma coluna por ano com área em hectares para cada combinação bioma x estado x classe. A série começa em 1985 e termina em 2025 na coleção 11 ou em 2024 na coleção 10.

Após parsing, o agrobr converte para formato long: uma linha por combinacao bioma x estado x classe x ano.

### Cobertura municipal e identidade territorial

`cobertura(nivel="municipio")` preserva `municipio` e `geocodigo`, proveniente da coluna publicada `geocode`. O código tem sete dígitos ASCII como texto; não é uma garantia de pertencimento ao catálogo municipal atual do IBGE. Lagoa Mirim e Lagoa dos Patos constam como entidades territoriais. Um mesmo geocódigo pode aparecer em mais de uma UF, e essas interseções permanecem separadas. O código não autoriza corrigir UF nem agregar áreas automaticamente.

Em 5 UFs amazônicas de fronteira, a soma dos municípios publicada passa do estadual publicado, na mesma coleção, ano e classe, com a mesma diferença em 1985, 2000 e 2025 (Coleção 11):

| UF | Σ municípios − estadual, todas as classes | Formação Florestal (classe 3), 2025 |
|---|---:|---:|
| RR | +20.059 ha | +19.589 ha (0,14%) |
| AM | +20.316 ha | +18.615 ha (0,015%) |
| PA | +9.671 ha | +8.976 ha (0,011%) |
| AP | +5.851 ha | +5.695 ha (0,057%) |
| AC | +4.078 ha | +3.996 ha (0,029%) |

Nas outras 22 UFs, a diferença fica abaixo de 1 ha (no RS, com as lagoas, +0,985 ha). O agrobr repassa as 2 publicações como vêm, e o `READ_ME` delas não trata da diferença. Para o total da UF, use `nivel="estado"`, e não a soma dos municípios.

O filtro `municipio` aceita o nome inteiro (sem diferenciar caixa e acento, com `uf` para desambiguar, por `normalize.resolver_municipio`) ou o geocódigo de sete dígitos, e sempre seleciona pelo geocódigo; pedaço de nome é recusado com os candidatos. Na Coleção 11, o parser valida todas as linhas e todos os 41 anos antes de concluir, mesmo quando a consulta pede apenas um município, classe ou ano. Áreas ausentes, não finitas, negativas ou de tipo incompatível interrompem a leitura; zeros permanecem. Linhas inteiramente vazias são contabilizadas, sem fabricar observações.

O parser municipal usa leitura em fluxo e só acumula o retorno selecionado, preservando a ordem ano→linha da fonte. Isso reduz memória de consultas filtradas, mas não o download nem a validação da população. Uma consulta sem filtros ainda materializa todo o resultado longo. Fingerprint e estatísticas descrevem estrutura e valores publicados; não medem acurácia científica dos mapas.

Na Coleção 10, o recurso tem 40 anos de 1985–2024 e linhas territoriais repetidas com áreas distintas. O retorno municipal de ambas as coleções preserva `id_registro`, o `ID` numérico publicado, para manter cada linha sem agregação nem deduplicação. O ID é local à publicação/coleção, aceita zero e não deve ser usado como ligação estável entre revisões. A chave inclui bioma, UF, geocódigo, classe, ID e ano. O parser verifica unicidade do ID em toda a população; repetições da combinação territorial com IDs distintos permanecem válidas e são contabilizadas em `source_details["territorial_keys"]`.

O código da classe não basta para comparar coleções. Nos campos da tabela municipal 10, classe 13 é `Other non Forest Formations`, devolvida como **Outras Formações não Florestais**; na 11, é **Mosaico Herbáceo-Arbustivo**. A classe 0 de ambas representa não observado. Cada recorte usa a legenda da sua própria coleção; estadual e municipal compartilham essa mesma legenda.

### Transição

Área em hectares de transição entre pares de classes para cada período temporal. Inclui períodos anuais, quinquenais e outros intervalos publicados. O período total é 1985-2025 na coleção 11 e 1985-2024 na coleção 10.

## Exemplo de Uso

```python
import agrobr

# Cobertura do Cerrado em 2020
df = await agrobr.mapbiomas.cobertura(bioma="Cerrado", ano=2020)

# Pastagem (classe 15) em Goias
df = await agrobr.mapbiomas.cobertura(bioma="Cerrado", uf="Goiás", classe_id=15)

# Transicao floresta→pastagem no Cerrado
df = await agrobr.mapbiomas.transicao(
    bioma="Cerrado",
    classe_de_id=3,   # Formacao Florestal
    classe_para_id=15, # Pastagem
    periodo="2019-2020",
)

# Com metadados
df, meta = await agrobr.mapbiomas.cobertura(
    bioma="Cerrado", ano=2020, return_meta=True
)
print(meta.records_count, meta.fetch_duration_ms)
```

## Limitacoes

- Apenas dados tabulares (estatísticas). Dados geoespaciais (rasters/GEE) ficam para versão futura
- O XLSX selecionado é baixado inteiro em cada chamada. Os filtros reduzem o DataFrame retornado, sem reduzir o download.
- `nivel` aceita `"estado"` (padrão), `"uf"` (sinônimo de `"estado"`, mesmo resultado) e `"municipio"`.
- Nível municipal disponível via `cobertura(nivel="municipio")`, com arquivo significativamente maior que o estadual. Sem cache local integrado.
- `classe` usa o rótulo em português da aba `LEGEND_CODE` da própria coleção, sem a numeração hierárquica e com a grafia e os qualificadores publicados (por exemplo, `Algodão (beta)` e `Parque eólico (beta)`). As classes que aparecem nos dados sem rótulo em português na legenda usam tradução do agrobr do rótulo inglês das linhas: 0 → `Não observado` (`Not Observed`) nas duas coleções, 75 → `Usina Fotovoltaica` (`Photovoltaic Project`) e 13 → `Outras Formações não Florestais` (`Other non Forest Formations`) na 10, 13 → `Mosaico Herbáceo-Arbustivo` (`Herbaceous-Shrub Mosaic`) na 11. `nivel_0` preserva o texto publicado. Um código publicado fora da legenda conhecida sai com o rótulo nulo (`classe`, ou `classe_de`/`classe_para` na transição), o `classe_id` publicado e um aviso (`UserWarning` e `meta.validation_warnings`) com os códigos. Até a 1.1.0, o estadual devolvia `Classe {id}`. Célula de classe sem código inteiro é defeito da planilha: a leitura falha com `ParseError`, com a linha e o valor publicado.
- A leitura tabular não infere equivalência territorial entre coleções, correção cadastral nem vigência de nomes/códigos. Classes hierárquicas não devem ser somadas indiscriminadamente.

## Proveniência do arquivo municipal

`meta.raw_content_hash` e `meta.raw_content_size` identificam o corpo HTTP efetivamente baixado. `meta.source_details["acquisition"]` contém URL solicitada e final, horário de coleta, status e cabeçalhos técnicos, além da confirmação de download quando necessária. Quando o corpo é ZIP, `member` identifica nome, hash, CRC e tamanho do XLSX extraído, separadamente do arquivo externo. Um XLSX direto não recebe um membro fictício.

O restante de `source_details` inclui coleção, aba, fingerprint de layout, contagens da população e saída, estatísticas por ano e cruzamentos de geocódigos em múltiplas UFs. Consulte o [contrato municipal de uso_do_solo](../contracts/uso_do_solo.md#nivel-municipal) para a identidade de cada linha.

## Cache e Atualização

- Não há cache local: cada chamada baixa a planilha correspondente da fonte.
- O MapBiomas publica uma nova coleção por ano, com dados retroativos recalculados.
- Recomenda-se especificar filtros para reduzir o volume de dados no DataFrame.

## Links

- [MapBiomas Brasil](https://brasil.mapbiomas.org)
- [Estatísticas](https://brasil.mapbiomas.org/estatisticas/)
- [Legenda](https://brasil.mapbiomas.org/codigos-de-legenda/)
- [Citacao (FAQ)](https://brasil.mapbiomas.org/faq/?tema=dados)
