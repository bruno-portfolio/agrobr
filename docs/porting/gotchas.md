# Armadilhas por Fonte

Tudo que vai quebrar se você portar o agrobr olhando só o README.
Estas armadilhas são **independentes de linguagem** — valem para R, Julia,
JavaScript ou qualquer outra implementação.

!!! warning "Licenças dos Dados"
    Este guia documenta armadilhas **técnicas**. Antes de implementar acesso
    a qualquer fonte, verifique a [página de licenças](../licenses.md).

---

## Infraestrutura HTTP Transversal

Antes das fontes individuais: problemas que afetam **todas**.

### Encoding

Fontes governamentais BR misturam encodings sem declarar corretamente
no header `Content-Type`.

| Fonte | Encoding real | O que declara |
|-------|--------------|---------------|
| CEPEA | Windows-1252 ou ISO-8859-1 | UTF-8 (errado) |
| CONAB | UTF-8 (Excel, fallback calamine) | -- |
| IBGE/SIDRA | UTF-8 | Correto |
| B3 | ISO-8859-1 | -- |
| DERAL | ISO-8859-1 (XLS antigo) | Nada |
| INMET | UTF-8 | Correto |
| Notícias Agrícolas | ISO-8859-1 | Variável |
| ComexStat | UTF-8 (CSV) | Correto |

O agrobr usa uma fallback chain de 3 encodings. O ISO-8859-1 decodifica qualquer byte, então a chain termina nele, e
não há passo depois (nem `chardet`, nem replacement).

```
ENCODING_CHAIN = ("utf-8", "windows-1252", "iso-8859-1")
```

!!! warning "Sem tratamento, nomes quebram"
    "Feijão", "Açúcar", "São Paulo", "Paraná" viram caracteres ilegíveis
    se o encoding for interpretado errado.

### Engine Excel (openpyxl → calamine)

Planilhas XLSX de fontes governamentais (CONAB, ANP, etc.) podem conter
estilos/fills malformados que crasham openpyxl (bug conhecido desde 2021,
pandas#40499, sem fix upstream). O agrobr usa `python-calamine` (Rust, MIT)
como fallback automatico: ignora estilos, extrai apenas dados. Arquivos
OLE2/BIFF (.xls, ex: DERAL, serie historica legacy) usam xlrd direto.

Se voce portar para outra linguagem, garanta que sua lib Excel tolere
estilos invalidos ou tenha fallback equivalente.

### Rate Limiting

Fontes governamentais bloqueiam IP se receberem requests muito rápidos.
O intervalo é **por fonte**, não global.

| Fonte | Intervalo mínimo | Sensibilidade |
|-------|-----------------|---------------|
| IBGE/SIDRA | 1.0s | Alta |
| INMET | 0.5s | Média |
| BCB/SICOR | 1.0s | Média |
| CEPEA | 5.0s | Alta (Cloudflare) |
| CONAB | 3.0s | Média |
| CONAB CEASA | 2.0s | Média |
| ANDA | 3.0s | Baixa |
| ABIOVE | 3.0s | Baixa |
| NASA POWER | 1.0s | Baixa |
| USDA | 1.0s | Baixa |
| IMEA | 1.0s | Média |
| ComexStat | 2.0s | Média |
| DERAL | 3.0s | Baixa |
| Notícias Agrícolas | 2.0s | Média |
| B3 | 1.0s | Média |
| Desmatamento | 2.0s | Baixa |
| MapBiomas | 2.0s | Baixa |
| Queimadas | 1.0s | Baixa |
| ZARC | 2.0s | Baixa |

### Retry e Backoff

- **Status codes retriáveis:** `408, 429, 500, 502, 503, 504`
- **Backoff:** `delay = min(base * 2^attempt, max_delay)`
- **Retry-After:** respeitar header quando presente (geralmente em 429)
- **Máximo:** 3 tentativas
- **Exceções retriáveis:** timeout, erro de rede, protocolo remoto

### User-Agent

CEPEA e Notícias Agrícolas bloqueiam User-Agents genéricos.
O agrobr mantém pool de **10 User-Agents** (Chrome, Firefox, Edge, Safari)
com rotação round-robin por fonte. Headers obrigatórios:

- `Accept-Language: pt-BR,pt;q=0.9`
- `Sec-Fetch-Dest: document`
- `Sec-Fetch-Mode: navigate`

ComexStat também exige User-Agent de browser (Mozilla).

### Timeouts

| Configuração | Valor padrão |
|-------------|-------------|
| Conexão | 10s |
| Leitura | 30s |
| Escrita | 10s |
| Pool | 10s |
| ComexStat (leitura) | 120s (CSVs grandes) |
| USDA (leitura) | 60s |
| ABIOVE (leitura) | 60s |
| ANDA (leitura) | 60s |

---

## CEPEA (Preços)

!!! info "Licença: CC BY-NC 4.0"
    Uso não-comercial livre com atribuição ao CEPEA.
    Uso comercial requer autorização: cepea@usp.br

!!! danger "Cloudflare"
    O site CEPEA fica atrás do Cloudflare, que pode bloquear requests HTTP
    diretos com status 403.

**Cadeia de fallback do agrobr:**

```
httpx direto → Notícias Agrícolas (restrito)
```

Playwright headless é um fallback **opcional/interno** (`_use_browser=False` por padrão); quando ativado, entra entre o httpx direto e o Notícias Agrícolas. Não faz parte da cadeia padrão.

**Anti-detecção no headless browser:**

- `--disable-blink-features=AutomationControlled`
- Property masking: `navigator.webdriver → undefined`
- Viewport: 1920x1080
- Locale: pt-BR, timezone America/Sao_Paulo

**URLs dos indicadores:**

Base: `https://www.cepea.org.br/br/indicador/{slug}.aspx`

| agrobr | Slug URL |
|--------|---------|
| `soja` | soja |
| `milho` | milho |
| `boi` / `boi_gordo` | boi-gordo |
| `cafe` / `cafe_arabica` | cafe |
| `algodao` | algodao |
| `trigo` | trigo |
| `arroz` | arroz |
| `acucar` | acucar |
| `frango_congelado` / `frango_resfriado` | frango |
| `suino` | suino |
| `etanol_hidratado` / `etanol_anidro` | etanol |
| `leite` | leite |

Lista parcial: o mapeamento completo (22 chaves → 15 páginas de indicador) está em `CEPEA_PRODUTOS`, em `agrobr/constants.py`.

**Fingerprinting de layout:**

O agrobr gera hash MD5 da estrutura DOM e compara com baseline.
Score ponderado:

| Componente | Peso |
|-----------|------|
| Headers de tabelas | 30% |
| Estrutura geral | 25% |
| Classes de tabelas | 20% |
| IDs-chave | 15% |
| Contagem de elementos | 10% |

Thresholds: > 85% OK, 70-85% warning, < 70% layout mudou.

---

## CONAB — Boletim de Safras

!!! info "Licença: Dados públicos"

!!! tip "HTTP primeiro, browser opcional"
    A página do boletim e o XLSX saem por HTTP direto (httpx).
    O Playwright só entra como fallback, quando o HTTP falha.

**Fluxo:**

1. GET da página `gov.br/conab/.../boletim-da-safra-de-graos` (httpx)
2. Página curta ou sem a palavra "levantamento" conta como falha e cai no browser
3. Extrair links XLSX via regex: `{N}o-levantamento-safra-{YYYY}-{YY}/...\.xlsx`
4. Baixar o XLSX por HTTP; Playwright só se o HTTP falhar

**Parsing do Excel — posição dinâmica:**

- Cada produto tem uma **aba** com nome específico
- O header row **não está numa posição fixa** — buscar dinamicamente
- Dados começam 3 linhas abaixo do header encontrado
- A CONAB adiciona/remove colunas de safras anteriores sem aviso

**Mapeamento produto → aba Excel (25 produtos):**

| agrobr | Aba Excel |
|--------|----------|
| `soja` | Soja |
| `milho` | Milho Total |
| `milho_1` | Milho 1a |
| `milho_2` | Milho 2a |
| `milho_3` | Milho 3a |
| `arroz` | Arroz Total |
| `arroz_irrigado` | Arroz Irrigado |
| `arroz_sequeiro` | Arroz Sequeiro |
| `feijao` | Feijão Total |
| `feijao_1` | Feijão 1a Total |
| `feijao_2` | Feijão 2a Total |
| `feijao_3` | Feijão 3a Total |
| `algodao` | Algodao Total |
| `algodao_pluma` | Algodao em Pluma |
| `trigo` | Trigo |
| `sorgo` | Sorgo |
| `aveia` | Aveia |
| `cevada` | Cevada |
| `canola` | Canola |
| `girassol` | Girassol |
| `mamona` | Mamona |
| `amendoim` | Amendoim Total |
| `centeio` | Centeio |
| `triticale` | Triticale |
| `gergelim` | Gergelim |

---

## CONAB — CEASA/PROHORT (Preços Atacado Hortifruti)

!!! warning "Licença: zona cinza (API não documentada, credenciais públicas do frontend)"

!!! tip "HTTP puro"
    Acesso direto via API REST do Pentaho — **não precisa de headless browser**.

**Endpoint:** `https://pentahoportaldeinformacoes.conab.gov.br/pentaho/plugin/cda/api/doQuery`

- 48 produtos (frutas, hortaliças, ovos)
- 43 CEASAs com mapeamento por UF
- Retorna JSON
- Rate limit: 2.0s

---

## CONAB — Custo de Produção

!!! info "Licença: Dados públicos"

!!! tip "HTTP puro"
    Scraping de HTML no gov.br + download de XLSX — sem browser.

- Custos detalhados por hectare: COE, COT, CT
- Por cultura, UF, safra e nível tecnológico
- Planilhas Excel com layout específico

---

## CONAB — Progresso Semanal de Safra

!!! info "Licença: Dados públicos"

!!! tip "HTTP puro"
    Scraping de portal Plone (gov.br) + download de XLSX — sem browser.

- Percentual de semeadura e colheita semanal
- Por cultura, estado e semana
- Comparação com ano anterior e média de 5 anos
- Paginação no portal para listar semanas disponíveis

---

## CONAB — Série Histórica

!!! info "Licença: Dados públicos"

!!! tip "HTTP puro"
    Download direto de XLS via URLs pré-mapeadas — sem browser.

- Dados desde ~1976 até safra corrente
- 45 produtos com mapeamento produto → URL
- Área plantada, produção e produtividade por UF e região
- Formato XLS (Excel 97-2003) ou XLSX (com fallback calamine)

---

## IBGE/SIDRA (PAM, LSPA, PPM, Abate, Censo)

!!! info "Licença: Dados públicos"

A API SIDRA é a fonte mais estável, mas tem particularidades:

- **Rate limit rigoroso** — 1s entre requests, bloqueia rápido
- Formato de resposta muda conforme combinação de parâmetros
- Respostas em CSV com header em linha 1

**Códigos de tabela:**

| Dataset | Tabela | Nível geográfico |
|---------|--------|-----------------|
| PAM | 5457 | N1, N2, N3, N6 |
| LSPA | 6588 | N1, N2, N3 |
| PPM | 3939 | N1, N2, N3, N6 |
| Abate | 1092 (bovinos), 1093 (suínos), 1094 (frangos) | N1, N3 |
| Censo Agro | uma tabela por tema e ano (ex.: 6907 efetivo 2017, 323 efetivo 1995) | N1, N2, N3, N6 |
| PEVS Silvicultura | 291 | N1, N2, N3, N6 |
| PEVS Silvicultura Area | 5930 | N1, N2, N3, N6 |
| PEVS Extracao Vegetal | 289 | N1, N2, N3, N6 |
| Leite Trimestral | 1086 | N1, N3 |
| PIB Agro (corrente) | 1846 | N1 |
| PIB Agro (real) | 6612 | N1 |

Níveis: `N1` (Brasil), `N2` (Região), `N3` (UF), `N6` (Município)

### Série Histórica Censo Agropecuário (tabelas 263-283, 1730, 1731)

**Nível territorial máximo:** UF (N3). Municipal **NÃO existe** no SIDRA para
estas tabelas — retorna erro ou dados vazios.

**Unidades mistas por categoria:**

- Tabela 281 (efetivo animais): Aves = "Mil cabeças", demais = "Cabeças"
- Tabela 282 (produção animal): Leite = "Mil litros", Ovos = "Mil dúzias", Lã = "Toneladas"
- Tabelas 283, 1730, 1731: unidade depende do produto (Toneladas, Mil frutos, Mil cachos)

**Classificações sem Total (`sumarizacao=false`):**
Tabelas 281, 282, 283, 1730, 1731 não têm categoria "Total" na classificação.
Não é possível pedir agregação via SIDRA — somar manualmente se necessário.

**Missing values:** `".."` = indisponível, `"..."` = suprimido, `"-"` = não aplicável,
`"X"` = dado sigiloso. Todos devem virar NaN.

**Parâmetro `v/all` vs `v/allxp`:** usar `v/all` para incluir variáveis percentuais.
`v/allxp` exclui percentuais silenciosamente.

**Parâmetro `c/all` inválido:** usar `c{ID}/all` (ex: `c220/all`), não `c/all`.

---

## NASA POWER (Clima)

!!! info "Licença: CC BY 4.0"

API REST limpa, sem autenticação. A fonte mais fácil.

- **Limite por request:** 366 dias — paginar para séries longas
- **Coordenadas:** ponto (lat/lon), não polígono
- Rate limit: 1s

**Parâmetros disponíveis:**

| Parâmetro | Descrição |
|-----------|-----------|
| `T2M` | Temperatura média 2m |
| `T2M_MAX` | Temperatura máxima |
| `T2M_MIN` | Temperatura mínima |
| `PRECTOTCORR` | Precipitação |
| `ALLSKY_SFC_SW_DWN` | Radiação solar |
| `RH2M` | Umidade relativa |
| `WS2M` | Velocidade do vento |

---

## BCB/SICOR (Crédito Rural)

!!! info "Licença: Dados públicos"

- API pública via OData (`olinda.bcb.gov.br`) — funcional mas lenta
- **Fallback BigQuery:** dataset público quando OData falha
- Rate limit: 1s

---

## ComexStat (Exportações)

!!! info "Licença: Dados públicos"

**Acesso:** download de CSV bulk anual.

**URL:** `https://balanca.mdic.gov.br/balanca/bd/comexstat-bd/ncm/EXP_{YYYY}.csv`

- **Separador:** ponto e vírgula (`;`), não vírgula
- **Timeout estendido:** 120s (arquivos podem ser grandes)
- **User-Agent obrigatório:** Mozilla (site filtra agentes genéricos)
- **Filtro por NCM:** código de 8 dígitos para produto específico
- HTTP 404 (ano ainda não publicado) sai como `SourceUnavailableError`; ano fora de 1997–9999 é recusado antes da rede

**Produtos mapeados via NCM:** soja (grão, óleo, farelo), milho, arroz,
trigo, algodão, café (arábica, conilon), açúcar, etanol, carnes
(bovina, frango, suína).

---

## USDA PSD (Estimativas Internacionais)

!!! info "Licença: Dados públicos"

!!! danger "API key obrigatória"
    Requer chave gratuita do [api.data.gov/signup](https://api.data.gov/signup).
    Sem chave, o gateway responde HTTP 403 `API_KEY_MISSING`; o agrobr levanta `SourceUnavailableError` antes da rede.

**API REST:** `https://api.fas.usda.gov/api/psd` (a antiga `apps.fas.usda.gov/OpenData/api` responde 500)

- Header: `X-Api-Key: {chave}` (nunca `?api_key=` na URL)
- Env var agrobr: `AGROBR_USDA_API_KEY`
- Usa USDA commodity codes (ex: `"2222000"` para soja, `"0813100"` para farelo de soja)
- Retorna JSON em camelCase só com IDs; os nomes vêm dos catálogos `commodityAttributes`, `commodities`, `countries` e
  `unitsOfMeasure`, guardados no pacote
- Código fora do catálogo devolve HTTP 200 com `[]`, sem erro: o agrobr valida antes da rede
- Timeout: 60s
- Rate limit: 1s

---

## INMET (Meteorologia)

!!! info "Licença: Dados públicos"

!!! danger "Falha silenciosa sem token"
    Sem `AGROBR_INMET_TOKEN`, a API retorna **HTTP 204 (No Content)**
    — não 401, não 403. O request "funciona" mas retorna vazio.

- API declarada como instável
- Recomendação: usar **NASA POWER** como alternativa

---

## DERAL (Lavouras Paraná)

!!! info "Licença: Dados públicos"

- URL direta para `.xls` (Excel 97-2003, não XLSX)
- **Encoding:** ISO-8859-1
- Parsing específico do layout da planilha
- Rate limit: 3s

---

## B3 Futuros Agro

!!! warning "Zona cinza"
    Sem termos de uso específicos para acesso programático.
    Redistribuição em produto comercial deve ser verificada com B3
    (marketdata@b3.com.br).

**Acesso:** BVBG-086 ZIP/XML.

**URL:** `https://www.b3.com.br/pesquisapregao/download?filelist=PR{yymmdd}.zip`

- ZIP nested: externo → interno (`BVBG086.zip`) → snapshots XML (usar último, definitivo)
- Namespace XML: `urn:bvmf.217.01.xsd`
- Parsing via `lxml.etree.iterparse` (streaming)
- Rate limit: 1s
- Tratamento de fins de semana necessário (bolsa não opera)

**Contratos disponíveis:**

| Código BMF | agrobr | Produto |
|-----------|--------|---------|
| BGI | `boi` | Boi Gordo |
| CCM | `milho` | Milho |
| ICF | `cafe_arabica` | Café Arábica |
| CNL | `cafe_conillon` | Café Conillon |
| ETH | `etanol` | Etanol Hidratado |
| SJC | `soja_cross` | Soja Cross |
| SOY | `soja_fob` | Soja FOB |

---

## IMEA (Mato Grosso)

!!! danger "Redistribuição proibida"
    Termos de uso do IMEA proíbem compartilhamento de dados sem autorização
    escrita. API não documentada oficialmente.

- Endpoint: `api1.imea.com.br/api/v2/mobile/cadeias`
- Descoberto por engenharia reversa — sem documentação pública
- Retorna JSON
- 8 cadeias: algodão, bovinocultura, milho, soja, conjuntura, suinocultura, leite, custo de produção
- Rate limit: 1s

---

## ABIOVE (Exportação Complexo Soja)

!!! warning "Zona cinza"
    Sem termos de uso públicos. Autorização formal pendente.

- **Acesso:** download de XLSX por mês/ano
- URL: `https://abiove.org.br/abiove_content/Abiove/exp_{YYYYMM}.xlsx`
- Fallback: tenta meses em ordem decrescente (12→1) se mês não especificado
- Publicação mensal, nem sempre no prazo
- Timeout: 60s
- Rate limit: 3s

---

## ANDA (Fertilizantes)

!!! warning "Zona cinza"
    Sem termos de uso públicos. Autorização formal pendente.

- **Acesso indireto:** scraping de HTML para extrair links de PDF, depois parsing do PDF
- URL: `https://anda.org.br/recursos/`
- Busca por keywords "entrega" ou "fertilizante" nos links
- Os boletins de entregas expõem apenas o total agregado; não há recorte por formulação
- **Dependência opcional:** requer `pdfplumber` (`pip install agrobr[pdf]`)
- Timeout: 60s
- Rate limit: 3s

---

## Notícias Agrícolas (Fallback CEPEA)

!!! danger "Licença restritiva"
    Todos os direitos reservados (Lei 9.610/98). **Não recomendado como
    fonte primária em ports.** Prefira CEPEA direto via headless browser.

Mapeamento de URLs (não padronizado, precisa ser hardcoded):

| agrobr | Path |
|--------|------|
| `soja` | `soja/soja-indicador-cepea-esalq-porto-paranagua` |
| `soja_parana` | `soja/indicador-cepea-esalq-soja-parana` |
| `milho` | `milho/indicador-cepea-esalq-milho` |
| `boi` | `boi-gordo/boi-gordo-indicador-esalq-bmf` |
| `cafe` | `cafe/indicador-cepea-esalq-cafe-arabica` |
| `algodao` | `algodao/algodao-indicador-cepea-esalq-a-prazo` |
| `trigo` | `trigo/preco-medio-do-trigo-cepea-esalq` |
| `arroz` | `arroz/arroz-em-casca-esalq-bbm` |
| `acucar` | `sucroenergetico/acucar-cristal-cepea` |
| `acucar_refinado` | `sucroenergetico/acucar-refinado-amorfo` |
| `etanol_hidratado` | `sucroenergetico/indicador-semanal-etanol-hidratado-cepea-esalq` |
| `etanol_anidro` | `sucroenergetico/indicador-semanal-etanol-anidro-cepea-esalq` |
| `frango_congelado` | `frango/precos-do-frango-congelado-cepea-esalq` |
| `frango_resfriado` | `frango/precos-do-frango-resfriado-cepea-esalq` |
| `suino` | `suinos/indicador-do-suino-vivo-cepea-esalq` |
| `leite` | `leite/leite-precos-ao-produtor-cepea-rs-litro` |
| `laranja_industria` | `laranja/laranja-industria` |
| `laranja_in_natura` | `laranja/laranja-pera-in-natura` |

A tabela mostra 18 dos 22 mapeamentos (incluindo aliases; 20 URLs únicas); a lista completa está em `NOTICIAS_AGRICOLAS_PRODUTOS`, em `agrobr/constants.py`.

Base: `https://www.noticiasagricolas.com.br/cotacoes/{path}`

---

## Desmatamento (PRODES + DETER)

!!! info "Licença: Dados públicos"

- Fonte: TerraBrasilis/INPE via GeoServer (WFS)
- **PRODES** = consolidado anual
- **DETER** = alertas near-real-time
- Respostas GeoJSON/JSON do WFS podem ser grandes
- Rate limit: 2s

---

## Queimadas (INPE)

!!! info "Licença: Dados públicos"

- CSV/ZIP do Brasil inteiro por dia, mês ou ano; satélite, bioma e UF são filtrados depois
- Arquivos podem ser grandes (centenas de MB em meses secos)
- Rate limit: 1s

---

## MapBiomas

!!! info "Licença: Livre com citação"

- XLSX de cobertura da terra: coleção 10 no Dataverse do MapBiomas (`data.mapbiomas.org`); coleção 11 no site e no Google Drive
- Fonte mais estável do ecossistema
- Rate limit: 2s

---

## Normalização Cross-Source

### Nomes de culturas

Cada fonte usa nomes diferentes para a mesma cultura.
**Sem normalização, joins entre fontes quebram.**

```
CEPEA:     "soja"
CONAB:     "Soja"
USDA:      "Soybeans"
```

O agrobr normaliza **158 variantes → 43 nomes canônicos**,
com busca case-insensitive e accent-insensitive.

Mapeamento completo em `agrobr/normalize/crops.py`.

### Safras

| Fonte | Formato | Exemplo |
|-------|---------|---------|
| CONAB | ano-safra | `"2024/25"` |
| IBGE | ano-calendário | `2024` |
| USDA | marketing year (ano inicial) | `2024` (safra 2024/25) |

Ano-safra BR começa em julho. Conversão depende da cultura e região.
Lógica em `agrobr/normalize/dates.py`.

### Unidades

Preços e volumes aparecem em unidades diferentes entre fontes.
Conversões em `agrobr/normalize/units.py` (sacas, toneladas, arrobas,
bushels — 14 tipos).

### Municípios

JSON com 5.571 municípios BR + código IBGE de 7 dígitos.
Necessário para cruzar dados municipais entre fontes.
Arquivo: `agrobr/normalize/_municipios_ibge.json` (259 KB).
