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
       └─ Todas falharam → SourceUnavailableError(errors=[...])
```

Cada `DatasetSource` encapsula:

- `name` — identificador da fonte
- `priority` — ordem de tentativa (menor = primeiro)
- `fetch_fn` — callable async que retorna `(DataFrame, metadata)`

O método `_try_sources()` tenta fontes por prioridade, captura erros
por categoria (rede, parsing, contrato, inesperado) e retorna proveniência
completa.

**MetaInfo** inclui:

- `attempted_sources` — lista de fontes tentadas em ordem
- `selected_source` — fonte que forneceu os dados
- `fetch_timestamp` — hora da coleta
- `schema_version` — versão do contrato

Datasets são registrados automaticamente via registry com auto-descoberta.

### Hierarquia de Exceções

Qualquer port deve implementar equivalentes para tratamento de erros
consistente.

| Exceção | Quando |
|---------|--------|
| `AgrobrError` | Base de todas as exceções |
| `SourceUnavailableError` | Todas as fontes falharam após retries |
| `NetworkError` | Timeout, HTTP error, DNS |
| `ParseError` | Layout mudou, HTML/JSON inesperado |
| `ContractViolationError` | DataFrame não bate com contrato (colunas, tipos) |
| `ValidationError` | Pydantic ou validação estatística falhou |
| `CacheError` | Operação de cache falhou |
| `FingerprintMismatchError` | Estrutura da página mudou significativamente |

**Warnings** (não interrompem execução):

| Warning | Quando |
|---------|--------|
| `StaleDataWarning` | Dados do cache expirados mas retornados |
| `PartialDataWarning` | Dados retornados incompletos |
| `LayoutChangeWarning` | Possível mudança de layout (baixa confiança) |
| `AnomalyDetectedWarning` | Anomalia estatística nos dados |
| `ParserFallbackWarning` | Parser primário falhou, usando fallback |

---

## Normalização — Módulos para Portar

A normalização é o que permite joins entre fontes. **Porte estes módulos
primeiro**, antes de qualquer client HTTP.

### Culturas (`normalize/crops.py`)

**156 variantes → 41 nomes canônicos**, com busca case-insensitive e
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
| USDA | marketing year | `"2024/25"` |

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

- 5.570 municípios com código IBGE de 7 dígitos
- Busca por nome (case/accent-insensitive) com desambiguação por UF
- Arquivo: `normalize/_municipios_ibge.json` (164 KB)

Funções: `municipio_para_ibge()`, `ibge_para_municipio()`,
`buscar_municipios()`, `total_municipios()`

### Encoding (`normalize/encoding.py`)

Fontes governamentais BR misturam encodings sem declarar corretamente.
Fallback chain de 5 encodings + detecção automática com chardet
(threshold > 0.7):

```
UTF-8 → ISO-8859-1 → Windows-1252 → UTF-16 → ASCII → chardet → replace
```

Funções: `decode_content()`, `detect_encoding()`

---

## Variáveis de Ambiente

Algumas fontes exigem configuração via variáveis de ambiente:

| Variável | Fonte | Obrigatória? | Consequência sem ela |
|----------|-------|:------------:|----------------------|
| `AGROBR_USDA_API_KEY` | USDA PSD | Sim | `SourceUnavailableError(401)` |
| `AGROBR_INMET_TOKEN` | INMET | Sim | HTTP 204 — retorna vazio sem erro |

Rate limits e timeouts também são configuráveis via env vars com prefixo
`AGROBR_HTTP_` (ex: `AGROBR_HTTP_RATE_LIMIT_CEPEA=2.0`).

---

## Golden Data

Os arquivos em `tests/golden_data/` contêm dados de referência estáticos
para validar parsers em qualquer linguagem:

1. Alimente seu parser com o golden input (HTML, JSON, CSV, XLSX)
2. Compare o output com o `expected.json`
3. Se bater, seu parser está correto

### Conjuntos de teste disponíveis (19 fontes, 23 casos)

| Fonte | Caso de teste | Arquivos |
|-------|--------------|----------|
| ABIOVE | `exportacao_sample` | response.xlsx, expected.json |
| ANDA | `entregas_sample` | response.json, expected.json |
| B3 | `ajustes_sample` | response.html, response_weekend.html, expected.json |
| BCB | `custeio_sample` | response.json, expected.json |
| CEPEA | `soja_sample` | response.html, expected.json |
| ComexStat | `exportacao_soja_sample` | response.csv, expected.json |
| CONAB | `safra_sample` | response.xlsx, expected.json |
| CONAB CEASA | `precos_sample` | ceasas_response.json, precos_response.json, expected.json |
| CONAB Progresso | `progresso_sample` | progresso_sample.xlsx, expected.json |
| DERAL | `pc_sample` | response.xlsx, expected.json |
| Desmatamento | `deter_sample` | response.csv, expected.json |
| Desmatamento | `prodes_sample` | response.csv, expected.json |
| IBGE | `abate_bovino_sample` | response.csv, expected.json |
| IBGE | `censo_agro_efetivo_sample` | response.csv, expected.json |
| IBGE | `pam_soja_sample` | response.csv, expected.json |
| IBGE | `ppm_bovino_sample` | response.csv, expected.json |
| IMEA | `cotacoes_soja_sample` | response.json, expected.json |
| INMET | `observacoes_sample` | response.json, expected.json |
| MapBiomas | `biome_state_sample` | biome_state_sample.xlsx, expected.json |
| Notícias Agrícolas | `soja_sample` | response.html, expected.json |
| NASA POWER | `daily_sample` | response.json, expected.json |
| Queimadas | `focos_sample` | response.csv, expected.json |
| USDA | `psd_soja_sample` | response.json, expected.json |

Cada diretório também contém `metadata.json` com contexto do teste.

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
