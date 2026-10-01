# Portando agrobr para Outras Linguagens

O agrobr é escrito em Python, mas os **dados e as armadilhas são universais**.
Se o objetivo é acessar dados agrícolas brasileiros em R, Julia, JavaScript
ou qualquer outra linguagem, este guia documenta tudo que é necessário
para não reinventar meses de engenharia reversa.

!!! warning "Licenças dos Dados"
    O agrobr (código) é MIT, mas os **dados** pertencem às respectivas fontes
    e possuem licenças próprias — algumas restritivas. Antes de implementar
    um port, leia a [página de licenças](../licenses.md) e verifique se o
    caso de uso está em conformidade com cada fonte.

---

## Filosofia

O agrobr é a **especificação de referência** para acesso a dados agrícolas
brasileiros. O código Python é uma implementação — mas o conhecimento
sobre como cada fonte funciona, quebra e muda é o verdadeiro valor.

Este guia existe para que a comunidade possa construir implementações
equivalentes em qualquer linguagem, com o mínimo de surpresas.

---

## Princípios para um Port

1. **Comece pela normalização, não pela infra** — cache, alertas e fingerprinting
   são opcionais. Normalização de culturas, safras e unidades são essenciais
   desde o dia 1.

2. **Acesse o CEPEA diretamente via headless browser** — contorna o Cloudflare
   sem depender de fontes com licença restritiva.

3. **Respeite rate limits** — fontes governamentais BR bloqueiam IP. Cada fonte
   tem seu próprio intervalo mínimo (veja [Armadilhas por Fonte](gotchas.md)).

4. **Normalize nomes de culturas desde o dia 1** — sem isso, joins entre
   CEPEA, CONAB e IBGE não funcionam.

5. **Teste contra golden data** — os arquivos em `tests/golden_data/` servem
   como referência para validar parsers em qualquer linguagem.

---

## Arquitetura

### Camadas da Biblioteca

```
┌─────────────────────────────────────────────┐
│              API Pública                     │
│   cepea.indicador()  conab.safras()  ...    │
├─────────────────────────────────────────────┤
│           Camada Semântica (datasets/)       │
│   fallback automático, contratos, MetaInfo   │
├─────────────────────────────────────────────┤
│        Fontes Individuais (cepea/, conab/,   │
│        ibge/, nasa_power/, bcb/, ...)        │
│   client → parser → models → API pública    │
├─────────────────────────────────────────────┤
│           Infraestrutura (http/, cache/,     │
│           normalize/, health/, contracts/)   │
└─────────────────────────────────────────────┘
```

**Fontes** são autônomas — cada uma tem seu próprio client HTTP, parser e
modelos internos. A camada de **datasets** apenas orquestra, normaliza e
garante o contrato final. Nunca mover lógica de parsing para datasets.

### Orquestração de Datasets

O coração do agrobr é o mecanismo de **fallback entre fontes**:

```
DatasetSource(name, priority, fetch_fn)
       │
       ▼
BaseDataset._try_sources(produto)
       │
       ├─ Fonte prioridade 1 → sucesso? → retorna (df, source, meta, attempted)
       ├─ Fonte prioridade 2 → sucesso? → retorna
       ├─ Fonte prioridade N → sucesso? → retorna
       ├─ Todas falharam por layout → ParseError(errors=[...])
       └─ Outras falhas esgotaram as fontes → SourceUnavailableError(errors=[...])
```

Cada `DatasetSource` encapsula:

- `name` — identificador da fonte
- `priority` — ordem de tentativa (menor = primeiro)
- `fetch_fn` — callable async que retorna `(DataFrame, metadata)`

O método `_try_sources()` tenta apenas fontes habilitadas, por prioridade, e retorna
o primeiro resultado obtido com proveniência completa. Um resultado vazio também é
aceito; quando há contrato registrado, recebe as colunas e os tipos desse contrato.

Falhas de rede (`httpx.HTTPError`, `httpx.TimeoutException` e `OSError`), `ParseError`,
`ContractViolationError` levantado pelo fetcher e `SourceUnavailableError` são
registrados e permitem tentar a próxima fonte. Se todas as tentativas falham por
layout, a cascata levanta `ParseError` agregado. Nos demais casos de esgotamento,
inclusive falhas mistas ou de contrato, levanta `SourceUnavailableError`. Os dois
erros incluem `errors`, `attempted_sources` e a última causa em `__cause__`.

`InvalidParameterError`, `TypeError`, `CacheMigrationError` e `ResourceLimitError`
interrompem a cascata. `SourceFallbackWarning` também propaga quando configurado
como erro. Outros erros de programação não são capturados nem acionam fallback.

**MetaInfo** inclui:

- `attempted_sources` — identificadores tentados em ordem, conforme a convenção abaixo
- `selected_source` — identificador selecionado, conforme a convenção abaixo
- `fetch_timestamp` — instante UTC da aquisição do corpo que o topo descreve (igual ao `fetched_at`; nulo nos registros do cache DuckDB do CEPEA)
- `fetched_at` — instante da aquisição original da fonte, preservado inclusive ao ler cache
- `schema_version` — versão do contrato

`fetched_at`, `timestamp`, `cache_expires_at` e `fetch_timestamp` são normalizados para UTC com fuso na construção e em atribuições posteriores. Valores sem fuso são interpretados como UTC; offsets são convertidos preservando o instante. Campos opcionais continuam aceitando `None`. O construtor e as atribuições recebem `datetime`; strings cruas geram `AttributeError`. `from_dict()` aceita ISO com e sem fuso; `to_dict()` emite `+00:00` nos valores preenchidos. Para comparar horários, use `datetime.now(UTC)`.

A identidade publicada segue duas convenções:

- **Rota da fonte:** `comercio_internacional`, `desmatamento`, `empregadores_lista_suja`, `unidades_conservacao_federais`, `uso_do_solo`, `cultivares_registradas` e `cultivares_protegidas` preservam a rota e as tentativas informadas pela fonte, mesmo com uma única tentativa. As duas funções de cultivares compartilham essa regra em `_rnc.py`. Na ausência dessa proveniência, o nome do adaptador serve como identificação.
- **Adaptador do dataset:** nos demais datasets, `selected_source` usa `DatasetSource.name` e `attempted_sources` lista os adaptadores tentados. Por exemplo, `cadastro_rural` publica `selected_source="sicar"` e `["sicar"]` em uma tentativa simples, enquanto a API da fonte publica `sicar_wfs`.

A regra da base adota a proveniência interna quando a fonte informa mais de uma tentativa ou `selected_source="cache"`: preserva a rota selecionada e combina os adaptadores anteriores com as tentativas internas, sem duplicatas e na ordem original. Sem identificador interno selecionado, conserva o nome do adaptador. `from_cache` é propagado independentemente; `from_cache=True` sozinho não muda a convenção dos nomes.

Datasets são registrados automaticamente via registry com auto-descoberta.

A base também propaga `raw_content_hash`, `raw_content_size`, `cache_key`, `cache_expires_at`, `fetch_duration_ms` e `parse_duration_ms` da fonte selecionada. Esses campos descrevem o recurso, cache e trabalho da fonte; não representam hash do DataFrame normalizado, cache próprio do dataset ou duração total do wrapper. Sem metadados da fonte, conservam None/0. A presença de chave ou TTL não determina `from_cache`, e `source_details` permanece uma cópia independente.

### Hierarquia de Exceções

Qualquer port deve implementar equivalentes para tratamento de erros
consistente.

| Exceção | Quando |
|---------|--------|
| `AgrobrError` | Base de todas as exceções |
| `InvalidParameterError` | Parâmetro do usuário inválido; também é `ValueError` e interrompe a cascata |
| `SourceUnavailableError` | A fonte não entregou o dado: timeout, falha de conexão ou status HTTP de erro (depois dos retries, nos status que se repetem), com a fonte, a URL e o status, e a exceção do httpx em `__cause__`. No dataset, as fontes se esgotaram sem que todas as falhas fossem de layout: `attempted_sources` e `errors` dizem quais e por quê |
| `NetworkError` | Reservado: segue exportado, mas não é levantado na 2.0; o status HTTP de erro sai como `SourceUnavailableError` |
| `ParseError` | Layout mudou, HTML/JSON inesperado; no dataset, agrega `errors` e `attempted_sources` quando todas as fontes falham por layout |
| `ContractViolationError` | DataFrame não bate com contrato (colunas, tipos) |
| `ValidationError` | Pydantic ou validação estatística falhou |
| `FingerprintMismatchError` | Estrutura da página mudou significativamente |

**Warnings** (não interrompem execução):

| Warning | Quando |
|---------|--------|
| `SourceFallbackWarning` | Fonte primária falhou e o dataset devolveu um fallback |
| `StaleDataWarning` | Dados do cache expirados mas retornados |

---

## Normalização — Módulos para Portar

A normalização é o que permite joins entre fontes. **Porte estes módulos
primeiro**, antes de qualquer client HTTP.

### Culturas (`normalize/crops.py`)

**144 variantes → 41 nomes canônicos**, com busca case-insensitive e
accent-insensitive.

```
CEPEA:     "soja"
CONAB:     "Soja"
IBGE:      "Soja (em grão)"
USDA:      "Soybeans"
ComexStat: "SOJA MESMO TRITURADA"
     ↓ normalizar_cultura()
     → "soja"
```

Funções: `normalizar_cultura()`, `listar_culturas()`, `is_cultura_valida()`

### Safras (`normalize/dates.py`)

Cada fonte usa formato diferente de safra:

| Fonte | Formato | Exemplo |
|-------|---------|---------|
| CONAB | ano-safra | `"2024/25"` |
| IBGE | ano-calendário | `2024` |
| USDA | marketing year (ano inicial) | `2024` (safra 2024/25) |

O ano-safra brasileiro começa em **julho** (mês 7). A safra "2024/25"
vai de 1 de julho de 2024 a 30 de junho de 2025.

Funções: `normalizar_safra()`, `safra_atual()`, `safra_anterior()`,
`safra_posterior()`, `lista_safras()`, `periodo_safra()`,
`safra_para_anos()`, `anos_para_safra()`

Formatos aceitos: `2024/25`, `24/25`, `2024/2025`

### Unidades (`normalize/units.py`)

Fontes reportam preços e volumes em unidades diferentes.

| Unidade | Peso | Uso |
|---------|------|-----|
| Saca 60kg | 60 kg | Soja, milho, café, trigo |
| Saca 50kg | 50 kg | Arroz |
| Arroba | 15 kg | Boi gordo |
| Bushel soja | 27.2155 kg | USDA, CBOT |
| Bushel milho | 25.4012 kg | USDA, CBOT |
| Bushel trigo | 27.2155 kg | USDA, CBOT |

14 tipos de unidade com conversões cruzadas. Funções: `converter()`,
`sacas_para_toneladas()`, `toneladas_para_sacas()`,
`preco_saca_para_tonelada()`, `preco_tonelada_para_saca()`

### Regiões e UFs (`normalize/regions.py`)

- 27 UFs com código IBGE e região
- 5 regiões (Norte, Nordeste, Centro-Oeste, Sudeste, Sul)
- Praças CEPEA por produto (soja, milho, boi_gordo, café)

Funções: `normalizar_uf()`, `uf_para_nome()`, `uf_para_regiao()`,
`uf_para_ibge()`, `ibge_para_uf()`, `normalizar_praca()`

### Municípios (`normalize/municipalities.py`)

- 5.571 municípios com código IBGE de 7 dígitos + centroides
- Busca por nome (case/accent-insensitive) com desambiguação por UF
- Geocodificação reversa offline: `(lat, lon)` → município mais próximo (sub-ms)
- Arquivo: `normalize/_municipios_ibge.json` (259 KB)

Funções: `municipio_para_ibge()`, `ibge_para_municipio()`,
`buscar_municipios()`, `coordenada_para_municipio()`, `total_municipios()`

### Encoding (`normalize/encoding.py`)

Fontes governamentais BR misturam encodings sem declarar corretamente.
Fallback chain de 3 encodings. O ISO-8859-1 decodifica qualquer byte, então a chain termina nele:

```
UTF-8 → Windows-1252 → ISO-8859-1
```

Funções: `decode_content()`, `detect_encoding()`

---

## Variáveis de Ambiente

Algumas fontes exigem configuração via variáveis de ambiente:

| Variável | Fonte | Obrigatória? | Consequência sem ela |
|----------|-------|:------------:|----------------------|
| `AGROBR_USDA_API_KEY` | USDA PSD | Sim | `SourceUnavailableError` antes da rede |
| `AGROBR_INMET_TOKEN` | INMET | Sim, na API observacional | `SourceUnavailableError`, com orientação para definir o token. O catálogo de estações e os ZIPs históricos são públicos |

Em `clima_uf`, a ausência de token é recusada antes de listar estações. No acesso de
`estacao`, HTTP 204 sem token e HTTP 403 são convertidos em `SourceUnavailableError`
com orientação para configurar `AGROBR_INMET_TOKEN`.

Rate limits e timeouts também são configuráveis via env vars com prefixo
`AGROBR_HTTP_` (ex: `AGROBR_HTTP_RATE_LIMIT_CEPEA=5.0`).

---

## Golden Data

Os arquivos em `tests/golden_data/` contêm dados de referência estáticos
para validar parsers em qualquer linguagem:

1. Alimente seu parser com o golden input (HTML, JSON, CSV, XLSX, PDF)
2. Compare o output com o `expected.json`, com as observações do caso no manifesto (ANDA) ou com o oráculo do caso
   (Rio Verde: as linhas de `oraculo_20260923.json`; IMEA: o próprio JSON oficial, como em `tests/test_imea/oficial.py`;
   Desmatamento: as propriedades de cada feição do JSON oficial, como em `tests/test_desmatamento/test_json_parser.py`)
3. Se bater, seu parser está correto

### Conjuntos de teste disponíveis (amostra: 26 fontes, 33 casos)

| Fonte | Caso de teste | Arquivos |
|-------|--------------|----------|
| ABIOVE | `exportacao_sample` | response.xlsx, expected.json |
| ANDA | `reconciliacao_r7_20260918` | anda/anda_Principais_Indicadores_2024.pdf, anda_manifest.json (`anda_2024`) |
| B3 | `posicoes_sample` | response.csv, expected.json |
| BCB | `custeio_sample` | response.json, expected.json |
| CEPEA | `soja_sample` | response.html, expected.json |
| Comtrade | `comercio_sample` | response.json, expected.json |
| Comtrade | `mirror_sample` | response_reporter.json, response_partner.json, expected.json |
| ComexStat | `exportacao_soja_sample` | response.csv, expected.json |
| CONAB | `safra_2025_26_agosto` | response.xlsx, expected.json |
| CONAB CEASA | `precos_sample` | ceasas_response.json, precos_response.json, expected.json |
| CONAB Progresso | `progresso_sample` | progresso_sample.xlsx, expected.json |
| DERAL | `pc_sample` | response.xlsx, expected.json |
| Desmatamento | `selecao_20260907` | 8 corpos JSON oficiais (PRODES em 6 biomas, DETER Amazônia e Cerrado), manifest.json |
| Desmatamento | `geo_20260923` | hits (XML) e página GeoJSON de 3 seleções (PRODES Pantanal, DETER Amazônia e Cerrado), manifest.json |
| IBGE | `abate_bovino_sample` | response.csv, expected.json |
| IBGE | `censo_agro_efetivo_sample` | response.csv, expected.json |
| IBGE | `pam_soja_sample` | response.csv, expected.json |
| IBGE | `reconciliacao_r15_20260918` (PPM, PEVS) | agregados/*.json, manifest.json |
| IBGE | `leite_trimestral_sample` | response.csv, expected.json |
| IBGE | `pib_agro_sample` | response.csv, expected.json |
| IMEA | `oficial_20260923` | cadeias.json, cotacoes_{id}.json e indicadores_{id}.json (8 cadeias), manifest.json |
| INMET | `observacoes_sample` | response.json, expected.json |
| MapBiomas | `biome_state_sample` | biome_state_sample.xlsx, expected.json |
| Notícias Agrícolas | `soja_sample` | response.html, expected.json |
| NASA POWER | `daily_sample` | response.json, expected.json |
| Queimadas | `focos_sample` | response.csv, expected.json |
| USDA | `psd_gateway_20260926` | 34 corpos de duas capturas independentes, capturas do gateway (404, 403, `[]`, série antiga), oráculos de 72 e 48 valores, manifest.json |
| RNC | `registradas_sample` | registradas_sample.csv (25 rows), expected.json |
| Rio Verde | `oraculo_20260923` | ensaio_soja_2023_2024.pdf, ensaio_soja_2024_2025.pdf, ensaio_soja_2025_2026.pdf, oraculo_20260923.json |
| BCB SGS | `sgs_sample` | sgs_sample.json (10 rows), expected.json |
| BCB PTAX | `ptax_sample` | ptax_sample.json (5 rows), expected.json |
| BCB Focus | `focus_sample` | focus_sample.json (5 rows), expected.json |
| ZARC | `tabua_risco_sample` | response.csv, expected.json |

A tabela acima é uma amostra; o diretório `tests/golden_data/` contém 41 fontes e 60 casos no total. Cada diretório também contém `metadata.json` com contexto do teste.

---

## Fontes por Prioridade de Implementação

| Prioridade | Fonte | Licença | Acesso | Justificativa |
|:---:|--------|---------|--------|---------------|
| 1 | CEPEA | CC BY-NC | Headless browser | Preços diários, alta demanda |
| 2 | IBGE/SIDRA | Livre | API REST | API limpa, dados públicos oficiais |
| 3 | CONAB Série Histórica | Livre | HTTP direto | Safras desde 1976, sem browser |
| 4 | CONAB CEASA | Livre | HTTP direto | 48 hortifrutis, 43 CEASAs, sem browser |
| 5 | CONAB Progresso | Livre | HTTP direto | Plantio/colheita semanal, sem browser |
| 6 | CONAB Boletim | Livre | Headless browser | Safra corrente, requer JS |
| 7 | NASA POWER | CC BY 4.0 | API REST | Clima, API limpa |
| 8 | BCB/SICOR | Livre | API OData | Crédito rural |
| 9 | ComexStat | Livre | HTTP direto | Exportações, CSV bulk |
| 10 | CONAB Custo Produção | Livre | HTTP direto | Custos por cultura/UF |
| 11+ | DERAL, USDA, Queimadas, Desmatamento, MapBiomas | Livre | Varia | Conforme necessidade |

!!! warning "Fontes com restrição"
    IMEA e Notícias Agrícolas possuem licença **restrita** (redistribuição
    proibida). B3, ANDA e ABIOVE estão em **zona cinza** (sem termos claros
    para acesso programático). Consulte a [página de licenças](../licenses.md)
    antes de implementar acesso a essas fontes.

---

## Guias por Linguagem

| Linguagem | Guia |
|-----------|------|
| R | [Guia para Desenvolvedores R](r.md) |

---

## Contribuindo com Ports

Se você implementar um port em outra linguagem:

- Abra uma issue no [repositório do agrobr](https://github.com/bruno-portfolio/agrobr) com o link
- Considere usar os mesmos nomes canônicos de culturas (veja `agrobr/normalize/crops.py`)
- Use os golden tests como suíte de validação
- A documentação de [armadilhas por fonte](gotchas.md) aplica-se a qualquer linguagem

---

## Implementações Conhecidas

| Linguagem | Repo | Status |
|-----------|------|--------|
| Python | [agrobr](https://github.com/bruno-portfolio/agrobr) | Referência |
