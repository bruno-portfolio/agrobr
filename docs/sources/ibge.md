# IBGE - Instituto Brasileiro de Geografia e Estatistica

## Visao Geral

| Campo | Valor |
|-------|-------|
| **Instituicao** | Governo Federal |
| **Website** | [ibge.gov.br](https://www.ibge.gov.br) |
| **API** | [SIDRA](https://sidra.ibge.gov.br) |
| **Acesso agrobr** | Via API SIDRA (JSON); malha municipal e áreas urbanizadas via WFS (GeoJSON) |

## Origem dos Dados

### Fonte

- **API**: `https://sidra.ibge.gov.br/`
- **Formato**: JSON
- **Acesso**: Publico, sem autenticacao

### Canal de acesso e fallback

Todas as consultas tabulares (PAM, LSPA, PPM, abate, PEVS, leite, PIB agropecuario e censos) passam por
`agrobr.ibge.client.fetch_sidra`. A API SIDRA (`apisidra.ibge.gov.br`) e o canal principal; desde setembro
de 2026 ela responde 403 com desafio Cloudflare a clientes programaticos. Quando a SIDRA falha (403, HTML,
5xx; falha de rede ou timeout não aciona a troca), a mesma tabela e consultada na API de agregados do IBGE
(`servicodados.ibge.gov.br/api/v3/agregados`), com os mesmos seletores traduzidos
(`t/p/v/n/c` → `agregados/{tabela}/periodos/{p}/variaveis/{v}?localidades=N{n}[...]&classificacao=c[...]`)
e a resposta convertida para o mesmo formato de colunas da SIDRA (`NC`, `NN`, `MC`, `MN`, `V`, `D1C`…), de
modo que parsers e contratos nao mudam. Diferencas conhecidas do canal de fallback: `MC` (codigo da unidade)
vem vazio, porque a API de agregados publica apenas o nome da unidade; `allxp` vira `all`; o nome do periodo
(`D2N`) vem do endpoint `/periodos` da propria tabela. O canal usado fica em
`MetaInfo.source_details["canal"]` (`sidra`, `servicodados` ou `misto`), `source_details["consultas"]` lista canal e URL
de cada consulta, `attempted_sources` ganha `ibge_servicodados` e `selected_source` passa a ser `ibge_servicodados`
quando o fallback foi usado (o dataset herda essa proveniencia); `source_url` aponta para a URL consultada;
a troca de canal emite `SourceFallbackWarning` (detalhe abaixo). No LSPA de julho/2026 (soja), os dois canais
dao os mesmos valores. O probe de saude do IBGE consulta a API de agregados.

Quando uma resposta da SIDRA exige a troca para a API de agregados, cada aquisição emite `SourceFallbackWarning` com o motivo. Com `return_meta=True`, a mensagem também aparece em `meta.validation_warnings`, e `selected_source="ibge_servicodados"` identifica o canal usado. Consultas sucessivas continuam avisando; uma resposta vazia preserva tanto o aviso de troca de canal quanto o de ausência de observações. Os datasets preservam esses metadados. Falhas de rede ou timeout mantêm o tratamento anterior e não acionam essa troca de canal.

Cada consulta também pede `/agregados/{tabela}/periodos` e registra a data de modificação dos períodos devolvidos, que
diz de qual edição veio o número: `source_details["periodos_modificacao"]` sai como `{tabela: {período: data ISO}}` (a
PAM 2024 foi revista em 17/09/2026), e cada item de `consultas` traz `tabela` e `periodos_modificacao`. O `01/01/0001` que
o IBGE publica para período sem data sai nulo. Se o pedido de metadado falhar, a consulta segue, e o motivo vai em
`periodos_modificacao_erro`.

Resposta sem observações (`[]`, por exemplo período ainda não publicado), em qualquer um dos dois canais, devolve
DataFrame vazio com as mesmas colunas de uma resposta com dados e emite um aviso (`warnings.warn`, a cada consulta, também registrado em `MetaInfo.validation_warnings`).

Na API 2.0, os filtros territoriais e as flags são passados por nome. PAM, LSPA, PPM, silvicultura e extração vegetal aceitam apenas produto/espécie e ano por posição; abate aceita espécie e trimestre; censos aceitam apenas tema. No PIB, apenas `setor` é posicional: use `ibge.pib_agro(trimestre="202401")`. Leite aceita apenas `trimestre` por posição.

Domínios fechados normalizam caixa e acentos; parâmetros inválidos levantam `InvalidParameterError` antes da consulta. `variaveis=[]`, variáveis desconhecidas e listas de anos vazias são recusadas. Silvicultura e extração vegetal validam o ano entre 1974 e o ano corrente. O censo histórico aceita ano inteiro ou lista não vazia de inteiros publicados para o tema.

Cada consulta SIDRA vazia emite um aviso e registra o mesmo texto em `MetaInfo.validation_warnings`, inclusive quando outra consulta da mesma tabela e período já veio vazia. Colunas e dtypes são preservados no vazio: anos/códigos em `Int64`, medidas em `float64`, rótulos de trimestre em texto e texto no padrão do pandas instalado. `animais_abatidos` usa `Int64` (contrato 2.0); quantidade fracionária gera `ParseError`. A PAM preserva suas 14 colunas de saída, com medidas não solicitadas nulas.

## Pesquisas Disponiveis

### PAM - Producao Agricola Municipal

- **Tabela SIDRA**: 5457 (serie desde 1974)
- **Cobertura**: Todos os municipios
- **Frequencia**: Anual

### LSPA - Levantamento Sistematico da Producao Agricola

- **Tabela SIDRA**: 6588
- **Cobertura**: Nacional/UF
- **Frequencia**: Mensal
- **Contrato**: [LSPA 2.0](../contracts/lspa.md), uma linha por ano/mês/localidade/produto/variável, com unidade explícita; sem `mes`, preserva os meses disponíveis do ano

`ibge.lspa("soja", ano=2025, mes="01", uf="MT")` aceita mês inteiro ou string inteira de 1 a 12. `uf=None` consulta o agregado Brasil. Um período não publicado pode retornar DataFrame vazio; HTTP 200 não comprova disponibilidade de observações.

O [dataset `estimativa_safra` 3.1](../contracts/estimativa_safra.md) pode selecionar essa fonte com `fonte="ibge_lspa"` ou `mes`, preservando `ano_lspa` e `mes_lspa`. Seu parâmetro `safra="2024/25"` seleciona o ano civil final, 2025; esse rótulo de safra não é uma dimensão nativa LSPA. No dataset, sem mês seleciona-se o último período com observações, enquanto a API de fonte sem mês preserva a série mensal disponível.

O dataset agrega os componentes esperados de milho e feijão, converte hectares/toneladas para mil ha/mil toneladas e recalcula produtividade pelos totais. Componentes ausentes, duplicatas ou unidades/localidades incompatíveis são rejeitados; valores NA continuam ausentes. Não use `levantamento` CONAB como mês LSPA nem some estimativas mensais como fluxos de produção.

### PPM - Pesquisa da Pecuaria Municipal

- **Tabelas SIDRA**: 3939 (rebanhos), 74 (producao de origem animal)
- **Cobertura**: Todos os municipios
- **Frequencia**: Anual
- **Serie**: 1974-presente (51 anos)

### Abate - Pesquisa Trimestral do Abate de Animais

- **Tabelas SIDRA**: 1092 (bovinos), 1093 (suinos), 1094 (frangos)
- **Cobertura**: 27 UFs (sem linha Brasil; para o total nacional, use a tabela da espécie no SIDRA, no nível Brasil)
- **Frequencia**: Trimestral
- **Serie**: 1997-presente
- **Especies**: bovino, suino, frango
- **Variaveis**: animais abatidos (cabecas), peso das carcacas (kg)

### Censo Agropecuario 1995/2006/2017

- **Tabelas SIDRA 2017**: 6907 (efetivo rebanho), 6881 (uso terra), 6957 (lavoura temporaria), 6956 (lavoura permanente), 6855 (preparo solo), 6848 (adubacao), 6849 (calagem), 6851 (agrotoxicos), 8561 (praticas agricolas), 6857 (irrigacao), 6899 (despesa com adubos)
- **Tabelas SIDRA 2006**: 791 (preparo solo), 1249 (adubacao), 1245 (calagem), 1459 (agrotoxicos), 837 (praticas agricolas), 855 (irrigacao)
- **Tabelas SIDRA 1995**: 323 (efetivo rebanho), 316/311 (uso terra), 497/492/503 (lavoura temporaria), 509/504/510 (lavoura permanente)
- **Cobertura**: Brasil + UF + municipio
- **Frequencia**: Decenial
- **Periodos**: 1995, 2006 e 2017 (conforme tema)
- **Temas**: efetivo_rebanho, uso_terra, lavoura_temporaria, lavoura_permanente, preparo_solo, adubacao, calagem, agrotoxicos, praticas_agricolas, irrigacao, despesa_adubos
- **Formato**: Long format (variavel/valor por linha)
- **Linha Total**: publicada como a fonte (`categoria = "Total"`), sem se somar às demais; `estabelecimentos`
  não soma entre categorias

### Censo Agropecuario — Serie Historica (1920-2006)

- **Tabelas SIDRA**: 263 (estabelecimentos/area), 264 (uso terra), 265 (pessoal/tratores), 280 (condicao produtor), 281 (efetivo animais), 282 (producao animal), 283 (producao vegetal), 1730 (lavoura permanente), 1731 (lavoura temporaria)
- **Cobertura**: Brasil + Regiao + UF (municipal NAO disponivel no SIDRA)
- **Frequencia**: Censos decenais (1920-2006, conforme tabela)
- **Periodos**: ate 10 censos por tema (1920, 1940, 1950, 1960, 1970, 1975, 1980, 1985, 1995, 2006)
- **Temas**: 9 temas com serie historica longa
- **Quirks**: Aves em mil cabecas (tab 281), unidades mistas por categoria (tabs 282/283/1730/1731), classificacoes sem Total (tabs 281/282/283/1730/1731)

### Censo Agropecuario 1985 — Dados Municipais (PDFs do IBGE)

- **Fonte**: os 28 PDFs estaduais da Biblioteca do IBGE (27 UFs; Minas Gerais em 2 volumes). MA, PI, CE e RN usam a versão que o
  IBGE republicou em 03/09/2018, com camada de texto.
- **Formato**: pacote do agrobr em Parquet (`agrobr/data/censo_1985/`), 1 linha por casa do PDF, extraído pela camada
  de texto, com o RapidOCR como 2ª leitura; o manifesto guarda o SHA-256 de cada PDF.
- **Cobertura**: 27 UFs, até município (mesorregião, microrregião, município); 85,8 % das células lidas têm coluna
  identificada (a lista por volume está no contrato).
- **Frequencia**: Unica (Censo 1985)
- **Temas**: 53 temas, 1 por tabela (67 a 119), pelo título impresso
- **Confiança**: `valor` só na casa confirmada pelas somas impressas (0 erro na precisão medida); `valor_lido` e o `status` para o
  resto, com a precisão medida no [contrato](../contracts/censo_agropecuario_municipal_1985.md)
- **Acesso**: local, sem rede
- **URL catalogo**: https://biblioteca.ibge.gov.br/index.php/biblioteca-catalogo?view=detalhes&id=747

### Censo Agropecuario 1995/96 — Temas Legados (FTP)

- **Fonte**: FTP IBGE (`ftp.ibge.gov.br`)
- **Formato**: ZIPs com XLS legado (BIFF5/BIFF8) ou HTML
- **Cobertura**: Brasil, totais estaduais e municípios; `uf` distingue municípios homônimos
- **Contrato**: [Censo legado 2.1](../contracts/censo_agropecuario_legado.md), com categorias, variáveis e unidades dos cabeçalhos oficiais
- **Frequencia**: Unica (Censo 1995/96)
- **Temas**: tecnologia, pessoal_ocupado, maquinas, producao_animal, valor_producao, financeiro
- **Acesso**: Publico, sem autenticacao

Dos seis temas nas 27 UFs, 161 combinações têm dados e uma, máquinas no PA, é recusada: o arquivo
[Pará/Tab_7Mn.zip](https://ftp.ibge.gov.br/Censo_Agropecuario/Censo_Agropecuario_1995_96/Para/Tab_7Mn.zip)
de máquinas contém os mesmos bytes da tabela de pessoal ocupado, e nenhum dos 11
`Tab_*Mn` do Pará traz a Tabela 7. A consulta de
máquinas com `uf='PA'` levanta `SourceUnavailableError`, e a consulta sem `uf`
devolve as outras 26 UFs, com o aviso no `MetaInfo` e um `UserWarning`; o tema não
é substituído por dados de pessoal ou por uma tabela de outra granularidade.

Os cabeçalhos BIFF8 de máquinas em Sergipe distinguem plantio, colheita,
caminhões e utilitários, mesmo quando as caixas de texto se sobrepõem. Os
valores estaduais não são reconstruídos pela soma dos municípios.

### PEVS — Silvicultura

- **Tabelas SIDRA**: 291 (producao, classificacao c194) + 5930 (area plantada, classificacao c734)
- **Cobertura**: Todos os municipios
- **Frequencia**: Anual
- **Serie**: 1986-presente
- **Produtos**: carvao, lenha, madeira_tora, madeira_celulose, acacia_negra, eucalipto_folha, resina (14 total)
- **Especies area**: eucalipto, pinus, outras
- **Variaveis**: quantidade_produzida (var 142), valor_producao (var 143), area (var 6549)
- **Unidades**: campo `MN` da resposta SIDRA, conforme variável e período; produção física em toneladas ou metros cúbicos, área em hectares e valor da produção em moeda (por exemplo, `Mil Reais` em 2023)

### PEVS — Extracao Vegetal

- **Tabela SIDRA**: 289 (classificacao c193)
- **Cobertura**: Todos os municipios
- **Frequencia**: Anual
- **Serie**: 1986-presente
- **Produtos**: acai, castanha_caju, castanha_para, erva_mate, mangaba, palmito, pequi_fruto, pinhao, umbu, hevea_coagulado, hevea_liquido, carnauba_cera, carnauba_po, piacava, carvao, lenha, madeira_tora, babacu, copaiba, cumaru, pequi_amendoa (21 total)
- **Variaveis**: quantidade_produzida (var 144), valor_producao (var 145)
- **Unidades**: campo `MN` da resposta SIDRA; quantidade em toneladas ou metros cúbicos e valor da produção em moeda (por exemplo, `Mil Reais` em 2023)

### Leite Trimestral — Pesquisa Trimestral do Leite

- **Tabela SIDRA**: 1086
- **Cobertura**: 27 UFs (sem linha Brasil; para o total nacional, use a tabela 1086 do SIDRA no nível Brasil)
- **Frequencia**: Trimestral
- **Serie**: 1997-presente
- **Variaveis**: leite adquirido (var 282, mil litros), leite industrializado (var 283, mil litros), preco medio (var 2522, R$/litro)
- **Output**: Formato wide (3 variaveis como colunas)

### PIB Agropecuario — Contas Nacionais Trimestrais

- **Tabelas SIDRA**: 1846 (precos correntes, var 585) + 6612 (precos reais base 1995, var 9318)
- **Cobertura**: Brasil (nivel nacional)
- **Frequencia**: Trimestral
- **Serie**: 1996-presente
- **Setores**: agropecuaria (90687), industria (90691), servicos (90696), pib_total (90707)
- **Classificacao**: c11255
- **Unidade**: Milhoes de Reais

## Variaveis

| Codigo | Nome | Unidade |
|--------|------|---------|
| 214 | Quantidade produzida | toneladas |
| 215 | Valor da produção | mil R$ |
| 216 | Área colhida | hectares |
| 8331 | Área plantada | hectares |
| 112 | Rendimento médio | kg/ha |

## Uso - PAM

### Basico

```python
import asyncio
from agrobr import ibge

async def main():
    # Dados de soja por UF
    df = await ibge.pam('soja', ano=2023)

    # Multiplos anos
    df = await ibge.pam('milho', ano=[2020, 2021, 2022, 2023])

    # Filtrar por UF
    df = await ibge.pam('soja', ano=2023, uf='MT')

    # Nivel municipal
    df = await ibge.pam('arroz', ano=2023, nivel='municipio', uf='RS')

    # Com metadados
    df, meta = await ibge.pam('soja', ano=2023, return_meta=True)

asyncio.run(main())
```

### Niveis Territoriais

| Nivel | Descricao |
|-------|-----------|
| `brasil` | Total nacional |
| `uf` | Por Unidade Federativa |
| `municipio` | Por municipio |

## Uso - LSPA

### Basico

```python
# Estimativas do ano
df = await ibge.lspa('soja', ano=2024)

# Mes especifico
df = await ibge.lspa('milho_1', ano=2024, mes=6)

# Filtrar por UF
df = await ibge.lspa('soja', ano=2024, uf='PR')

# Com metadados
df, meta = await ibge.lspa('soja', ano=2024, return_meta=True)
```

## Schema - PAM

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `ano` | int | Ano de referencia |
| `localidade` | str | Nome da localidade |
| `localidade_cod` | int | Código IBGE da localidade (D1C do SIDRA) |
| `produto` | str | Nome do produto |
| `area_plantada` | float | Hectares |
| `area_colhida` | float | Hectares |
| `producao` | float | Ver `unidade_producao` |
| `rendimento` | float | Ver `unidade_rendimento` |
| `valor_producao` | float | Ver `unidade_valor_producao` |
| `fonte` | str | "ibge_pam" |
| `unidade_producao` | str | `ton`; laranja antes de 2001, `mil_frutos` |
| `unidade_rendimento` | str | `kg/ha`; laranja antes de 2001, `frutos/ha` |
| `unidade_valor_producao` | str | `mil_reais`; antes de 1994, a moeda da época (`mil_cruzeiros`, `mil_cruzados` etc.) |
| `condicao_produto` | str | Café: `em_coco` até 2001, `beneficiado` desde 2002; nulo nos demais produtos |

## Schema - LSPA

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `ano` | int | Ano de referencia |
| `mes` | int | Mes de referencia |
| `localidade` | str | Nome da localidade |
| `localidade_cod` | int | Código IBGE da localidade (D1C do SIDRA) |
| `variavel` | str | Nome da variavel |
| `variavel_cod` | int | Código SIDRA da variável |
| `valor` | float | Valor da variavel |
| `unidade` | str | Unidade publicada pelo SIDRA |
| `produto` | str | Nome do produto |
| `fonte` | str | "ibge_lspa" |

## Produtos PAM

```python
produtos = await ibge.produtos_pam()
# ['soja', 'milho', 'arroz', 'feijao', 'trigo', 'cafe', ...]
```

## Produtos LSPA

```python
produtos = await ibge.produtos_lspa()
# ['soja', 'milho_1', 'milho_2', 'arroz', 'feijao_1', 'feijao_2', ...]
```

Nota: No LSPA, `milho_1` e `milho_2` referem-se à primeira e segunda safras de milho do mesmo ano civil. O alias `milho` da API de fonte devolve os componentes separados; sua agregação ocorre no dataset.

## UFs Disponiveis

```python
ufs = await ibge.ufs()
# ['AC', 'AL', 'AM', 'AP', 'BA', 'CE', 'DF', ...]
```

## Uso - PPM

### Basico

```python
import asyncio
from agrobr import ibge

async def main():
    # Rebanho bovino por UF
    df = await ibge.ppm('bovino', ano=2023)

    # Producao de leite
    df = await ibge.ppm('leite', ano=2023)

    # Multiplos anos
    df = await ibge.ppm('bovino', ano=[2020, 2021, 2022, 2023])

    # Filtrar por UF
    df = await ibge.ppm('bovino', ano=2023, uf='MT')

    # Nivel municipal
    df = await ibge.ppm('bovino', ano=2023, nivel='municipio', uf='MS')

    # Com metadados
    df, meta = await ibge.ppm('bovino', ano=2023, return_meta=True)

asyncio.run(main())
```

## Schema - PPM

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `ano` | int | Ano de referencia |
| `localidade` | str | Nome da localidade |
| `localidade_cod` | int | Codigo IBGE da localidade |
| `especie` | str | Nome da especie/produto |
| `valor` | float | Valor (cabecas, mil litros, etc) |
| `unidade` | str | Unidade de medida |
| `fonte` | str | "ibge_ppm" |

## Especies/Produtos PPM

### Rebanhos (tabela 3939)

| Codigo | Especie | Unidade |
|--------|---------|---------|
| `bovino` | Bovino | cabecas |
| `bubalino` | Bubalino | cabecas |
| `equino` | Equino | cabecas |
| `suino_total` | Suino (total) | cabecas |
| `suino_matrizes` | Suino matrizes | cabecas |
| `caprino` | Caprino | cabecas |
| `ovino` | Ovino | cabecas |
| `galinaceos_total` | Galinaceos (total) | cabecas |
| `galinhas` | Galinhas (inclui poedeiras e matrizeiras) | cabecas |
| `codornas` | Codornas | cabecas |

`galinhas_poedeiras`: alias depreciado de `galinhas` (`FutureWarning`).

### Producao de origem animal (tabela 74)

| Codigo | Produto | Unidade |
|--------|---------|---------|
| `leite` | Leite | mil litros |
| `ovos_galinha` | Ovos de galinha | mil duzias |
| `ovos_codorna` | Ovos de codorna | mil duzias |
| `mel` | Mel de abelha | kg |
| `casulos` | Casulos de bicho-da-seda | kg |
| `la` | La | kg |

```python
especies = await ibge.especies_ppm()
# ['bovino', 'bubalino', 'caprino', 'casulos', 'codornas', ...]
```

## Uso - Abate Trimestral

### Basico

```python
import asyncio
from agrobr import ibge

async def main():
    # Abate bovino por UF
    df = await ibge.abate('bovino', trimestre='202303')

    # Abate de frango no Parana
    df = await ibge.abate('frango', trimestre='202303', uf='PR')

    # Abate de suinos — todas as UFs
    df = await ibge.abate('suino', trimestre='202304')

    # Com metadados
    df, meta = await ibge.abate('bovino', trimestre='202303', return_meta=True)

asyncio.run(main())
```

## Schema - Abate Trimestral

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `trimestre` | str | Trimestre no formato YYYYQQ |
| `localidade` | str | UF |
| `localidade_cod` | int | Codigo IBGE da localidade |
| `especie` | str | bovino, suino ou frango |
| `animais_abatidos` | Int64 | Quantidade abatida (cabecas) |
| `peso_carcacas` | float | Peso total das carcacas (kg) |
| `fonte` | str | "ibge_abate" |

## Especies Abate

| Codigo | Especie | Tabela SIDRA |
|--------|---------|--------------|
| `bovino` | Bovino | 1092 |
| `suino` | Suino | 1093 |
| `frango` | Frango | 1094 |

```python
especies = await ibge.especies_abate()
# ['bovino', 'suino', 'frango']
```

## Uso - Censo Agropecuario

### Basico

```python
import asyncio
from agrobr import ibge

async def main():
    # Efetivo de rebanho por UF
    df = await ibge.censo_agro('efetivo_rebanho')

    # Uso da terra em Mato Grosso
    df = await ibge.censo_agro('uso_terra', uf='MT')

    # Lavoura temporaria por municipio
    df = await ibge.censo_agro('lavoura_temporaria', nivel='municipio', uf='PR')

    # Lavoura permanente — Brasil
    df = await ibge.censo_agro('lavoura_permanente', nivel='brasil')

    # Com metadados
    df, meta = await ibge.censo_agro('efetivo_rebanho', return_meta=True)

asyncio.run(main())
```

## Schema - Censo Agropecuario

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `ano` | int | Ano de referencia (1995, 2006 ou 2017) |
| `localidade` | str | Nome da localidade |
| `localidade_cod` | int | Codigo IBGE da localidade |
| `tema` | str | Tema do censo |
| `categoria` | str | Categoria dentro do tema |
| `variavel` | str | Nome da variavel |
| `valor` | float | Valor da variavel |
| `unidade` | str | Unidade de medida |
| `fonte` | str | "ibge_censo_agro" |

## Temas Censo Agropecuario

| Codigo | Tema | Tabela SIDRA 1995 | Tabela SIDRA 2006 | Tabela SIDRA 2017 |
|--------|------|-------------------|-------------------|-------------------|
| `efetivo_rebanho` | Efetivo de rebanho | 323 | — | 6907 |
| `uso_terra` | Uso da terra | 316/311 | — | 6881 |
| `lavoura_temporaria` | Lavoura temporaria | 497/492/503 | — | 6957 |
| `lavoura_permanente` | Lavoura permanente | 509/504/510 | — | 6956 |
| `preparo_solo` | Preparo do solo | — | 791 | 6855 |
| `adubacao` | Adubacao | — | 1249 | 6848 |
| `calagem` | Calagem | — | 1245 | 6849 |
| `agrotoxicos` | Uso de agrotoxicos | — | 1459 | 6851 |
| `praticas_agricolas` | Praticas agricolas | — | 837 | 8561 |
| `irrigacao` | Irrigacao | — | 855 | 6857 |

```python
temas = await ibge.temas_censo_agro()
# ['efetivo_rebanho', 'uso_terra', 'lavoura_temporaria', 'lavoura_permanente',
#  'preparo_solo', 'adubacao', 'calagem', 'agrotoxicos', 'praticas_agricolas', 'irrigacao',
#  'despesa_adubos']
```

## Cache

Não há cache local: cada chamada consulta o IBGE, e o `MetaInfo` sai com `from_cache=False` e `cache_expires_at` nulo.

## Atualizacao

| Pesquisa | Frequencia |
|----------|------------|
| PAM | Anual (agosto-setembro) |
| LSPA | Mensal |
| PPM | Anual (setembro) |
| Abate | Trimestral (T+2 meses) |
| Censo Agro | Decenial (ultimo: 2017) |
| Silvicultura (PEVS) | Anual (agosto-setembro) |
| Extracao Vegetal (PEVS) | Anual (agosto-setembro) |
| Leite Trimestral | Trimestral (T+2 meses) |
| PIB Agro | Trimestral (T+2 meses) |

## Uso - Silvicultura (PEVS)

### Basico

```python
import asyncio
from agrobr import ibge

async def main():
    # Producao de madeira em tora por UF
    df = await ibge.silvicultura('madeira_tora', ano=2023)

    # Area plantada de eucalipto
    df = await ibge.silvicultura('eucalipto', variavel='area')

    # Carvao vegetal em MG
    df = await ibge.silvicultura('carvao', ano=2023, uf='MG')

    # Com metadados
    df, meta = await ibge.silvicultura('madeira_tora', return_meta=True)

asyncio.run(main())
```

## Schema - Silvicultura

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `ano` | int | Ano de referencia |
| `localidade` | str | Nome da localidade |
| `localidade_cod` | int | Codigo IBGE da localidade |
| `produto` | str | Nome do produto |
| `valor` | float | Valor (Toneladas, Metros cubicos ou Hectares) |
| `unidade` | str | Unidade de medida |
| `fonte` | str | "ibge_silvicultura" |

## Uso - Extracao Vegetal (PEVS)

### Basico

```python
import asyncio
from agrobr import ibge

async def main():
    # Producao de acai por UF
    df = await ibge.extracao_vegetal('acai', ano=2023)

    # Castanha-do-Para no Amazonas
    df = await ibge.extracao_vegetal('castanha_para', ano=2023, uf='AM')

    # Valor da producao
    df = await ibge.extracao_vegetal('acai', variavel='valor_producao')

    # Com metadados
    df, meta = await ibge.extracao_vegetal('acai', return_meta=True)

asyncio.run(main())
```

## Schema - Extracao Vegetal

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `ano` | int | Ano de referencia |
| `localidade` | str | Nome da localidade |
| `localidade_cod` | int | Codigo IBGE da localidade |
| `produto` | str | Nome do produto |
| `valor` | float | Valor (Toneladas ou Metros cubicos) |
| `unidade` | str | Unidade de medida |
| `fonte` | str | "ibge_extracao_vegetal" |

## Uso - Leite Trimestral

### Basico

```python
import asyncio
from agrobr import ibge

async def main():
    # Leite trimestral por UF
    df = await ibge.leite_trimestral(trimestre='202303')

    # Filtrar por UF
    df = await ibge.leite_trimestral(trimestre='202303', uf='MG')

    # Multiplos trimestres
    df = await ibge.leite_trimestral(trimestre=['202301', '202302', '202303'])

    # Com metadados
    df, meta = await ibge.leite_trimestral(return_meta=True)

asyncio.run(main())
```

## Schema - Leite Trimestral

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `trimestre` | str | Trimestre YYYYQQ |
| `localidade` | str | UF |
| `localidade_cod` | int | Codigo IBGE da localidade |
| `leite_adquirido` | float | Leite cru adquirido (mil litros) |
| `leite_industrializado` | float | Leite cru industrializado (mil litros) |
| `preco_medio` | float | Preco medio pago ao produtor (R$/litro) |
| `fonte` | str | "ibge_leite_trimestral" |

## Uso - PIB Agropecuario

### Basico

```python
import asyncio
from agrobr import ibge

async def main():
    # PIB agropecuario a precos correntes
    df = await ibge.pib_agro(trimestre='202501')

    # PIB a precos reais (base 1995)
    df = await ibge.pib_agro(trimestre='202501', precos='real_1995')

    # PIB total
    df = await ibge.pib_agro(trimestre='202501', setor='pib_total')

    # Com metadados
    df, meta = await ibge.pib_agro(return_meta=True)

asyncio.run(main())
```

## Schema - PIB Agropecuario

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `trimestre` | str | Trimestre YYYYQQ |
| `valor` | float | Valor (Milhoes de Reais) |
| `unidade` | str | Unidade de medida |
| `setor` | str | Setor economico |
| `fonte` | str | "ibge_pib" |

## Malha municipal e áreas urbanizadas (geoserviços)

Fora do SIDRA, o módulo lê 2 camadas do WFS dos geoserviços do IBGE (GeoServer, WFS 2.0.0, saída em GeoJSON):

| Função | Camada | Edição | Feições |
|--------|--------|--------|---------|
| `malha_municipal` / `malha_municipal_geo` | `CGMAT:qg_2025_030_munic`, em `https://geoservicos.ibge.gov.br/geoserverIBGE/wfs` | malha municipal 2025 | 5.573: os 5.571 municípios e 2 áreas operacionais de lagoas do RS |
| `areas_urbanizadas` / `areas_urbanizadas_geo` | `CGEO:AU_2026_AreasUrbanizadas2022_Brasil`, em `https://geoservicos.ibge.gov.br/geoserverCGEO/wfs` | Áreas Urbanizadas do Brasil 2022 | 190.172 polígonos, sem UF nem município |

- A contagem (`resultType=hits`) sai antes do download; acima do teto da consulta, `ResourceLimitError` sem baixar nada, e o
  recebido é conciliado com a contagem, como no CNUC e no ICMBio.
- Os filtros vão ao servidor só por igualdade (`sigla_uf`, `cd_mun`) e `BBOX`. O firewall do IBGE recusa CQL com `OR` ou
  `NOT LIKE` com uma página HTML "Request Rejected" e status 200, que o agrobr levanta como `SourceUnavailableError`.
- A geometria sai em EPSG:4326, pedida ao servidor (`srsName`), na resolução original da camada.
- A API de malhas do IBGE (`servicodados.ibge.gov.br/api/v3/malhas`) não é usada: a geometria dela é generalizada para web
  (Brasília com 840 vértices, contra 9.896 no WFS) e só traz o código da área.
- Em arquivo, os ZIPs da malha por UF e do Brasil ficam no
  [geoftp do IBGE](https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_municipais/municipio_2025/),
  e o shapefile das áreas urbanizadas, em `organizacao_do_territorio/tipologias_do_territorio/areas_urbanizadas_do_brasil/2022/Shapefile/`
  no mesmo servidor.
- Licença: IBGE, `livre` (ver [licenças](../licenses.md)).
- Colunas, limites e exemplos: [API IBGE](../api/ibge.md#malha_municipal-malha_municipal_geo).

## Limites e erros

Rajadas de consultas ao SIDRA podem provocar uma verificação antibot do Cloudflare (`challenge`). Quando a resposta 403 contém `cf-mitigated: challenge` ou a página “Just a moment”, a consulta levanta `SourceUnavailableError` com a indicação `Cloudflare challenge` e orientação para reduzir a taxa de requisições. Aguarde antes de tentar novamente; esse 403 não é repetido automaticamente.

## Notas

- PEVS Silvicultura: 14 produtos, dados anuais desde 1986. Area plantada (tab 5930) com 3 especies
- PEVS Extracao Vegetal: 21 produtos, dados anuais desde 1986. Unidades mistas (Toneladas vs Metros cubicos)
- Leite Trimestral: tabela 1086, 3 variaveis pivotadas em colunas wide. Serie desde 1997
- PIB Agropecuario: tabs 1846/6612, 4 setores, nivel Brasil. Serie desde 1996. Contrato `pib_agro` 1.0 (dataset `datasets.pib_agro`)

## Períodos e cobertura histórica

PAM anterior a 1988 pode não publicar área plantada. `datasets.producao_anual` representa a ausência por `Float64` nulo; também mantém nulo o valor da produção quando não solicitado. As demais medidas não são preenchidas automaticamente, para não esconder mudanças na fonte.

PPM rejeita anos futuros com `InvalidParameterError`, também em `datasets.pecuaria_municipal`. Abate, leite trimestral e PIB agro aceitam `2025-4`, `2025-T4`, `2025T4`, `2025/4` e `2025Q4`, normalizados para `202504`. Formatos inválidos são rejeitados antes da rede.

## Unidades e quebras históricas da PAM

Os valores publicados não são convertidos implicitamente. `unidade_producao`, `unidade_rendimento` e `unidade_valor_producao` identificam a escala de cada linha. Laranja anterior a 2001 usa `mil_frutos` e `frutos/ha`; desde 2001, `ton` e `kg/ha`. `condicao_produto` distingue café `em_coco` até 2001 e `beneficiado` desde 2002. As moedas históricas permanecem identificadas, sem conversão para reais nem correção de inflação. Consulte as [notas metodológicas do IBGE](https://sidra.ibge.gov.br/pesquisa/pam/tabelas/).

`localidade_cod` (contrato `producao_anual` 2.2) traz o código IBGE da localidade como o SIDRA publica (D1C): 7 dígitos no município, 2 na UF e 1 no Brasil. Use-o para juntar municípios entre anos, porque o nome publicado muda (e o do DF sai como "Brasília (DF)", sem o " - UF" dos demais). No `producao_anual`, só as linhas do IBGE trazem o código; o fallback da CONAB não.

O símbolo SIDRA `-` significa zero numérico e é preservado como zero; `..`, `...` e `X` permanecem ausentes. Municípios com produção zero não são eliminados. O contrato `producao_anual` é 2.2; as quatro colunas descritivas e o `localidade_cod` são opcionais no contrato e entregues pela API PAM.

## Coleta bruta (malhas)

`agrobr.bruto.coletar("ibge", "malha_municipal", ...)` e `("ibge", "areas_urbanizadas", ...)` guardam as páginas GeoJSON originais das camadas `CGMAT:qg_2025_030_munic` (edição 2025) e `CGEO:AU_2026_AreasUrbanizadas2022_Brasil` (edição 2022), no CRS nativo (`EPSG:4674`) e com todos os atributos; `ibge.malha_municipal` e `ibge.areas_urbanizadas` (e os `_geo`) seguem em `EPSG:4326`. A chave da cobertura é `cd_mun` na malha e o atributo `fid` nas áreas urbanizadas, não o ID da feição do GeoServer (a feição `AU_2026_AreasUrbanizadas2022_Brasil.43300` tem `fid="45886"`); as duas são texto, e as páginas têm de vir em ordem estritamente crescente. Áreas urbanizadas recusam UF. Veja a [API da coleta bruta](../api/bruto.md) e o [contrato do manifesto](../contracts/bruto.md).

`agrobr.bruto.coletar("ibge", "malha_municipal_zip", ...)` e `("ibge", "areas_urbanizadas_zip", ...)` guardam os ZIPs nacionais do geoftp do IBGE como o IBGE os publica, sem abrir o shapefile: `BR_Municipios_2025.zip` (237 MB, edição 2025) e `AreasUrbanizadas2022_Brasil.zip` (54 MB, edição 2022). São a mesma publicação das camadas WFS: em 04/10/2026, o DBF de cada ZIP tinha o mesmo número de registros que o `numberMatched` da camada (5.573 na malha e 190.172 nas áreas urbanizadas). UF e bbox são recusadas; para um recorte, use os recursos paginados.
