# Política de Dependências

## Classificação

### Core (obrigatórias)

Instaladas com `pip install agrobr`:

| Dependência | Uso | Pin |
|---|---|---|
| `httpx` | HTTP client async | `>=0.25.0` |
| `beautifulsoup4` | HTML parsing | `>=4.12.0` |
| `lxml` | Parser HTML/XML | `>=5.0.0` |
| `pandas` | DataFrames | `>=2.0.0` |
| `pydantic` | Validação de modelos | `>=2.5.0` |
| `pydantic-settings` | Configurações | `>=2.1.0` |
| `duckdb` | Cache local | `>=1.5.0` |
| `structlog` | Logging estruturado | `>=23.2.0` |
| `chardet` | Detecção de encoding | `>=5.2.0` |
| `typer` | CLI | `>=0.9.0` |
| `openpyxl` | Leitura de Excel (.xlsx) | `>=3.1.0` |
| `python-calamine` | Fallback Excel (Rust, ignora estilos) | `>=0.3.0` |
| `xlrd` | Leitura de Excel legado (.xls) | `>=2.0.1` |
| `sidrapy` | API IBGE SIDRA | `>=0.1.4` |
| `requests` | HTTP síncrono (basedosdados, utilitários) | `>=2.32.0` |

### Opcionais

Instaladas via extras:

```bash
pip install agrobr[pdf]       # pdfplumber para PDFs
pip install agrobr[browser]   # Playwright para sites com JS
pip install agrobr[polars]    # Suporte a Polars DataFrames
pip install agrobr[geo]       # GeoDataFrames (SICAR, desmatamento, etc)
pip install agrobr[bigquery]  # Fallback BigQuery (BCB/SICOR)
pip install agrobr[all]       # Tudo (exceto app)
```

| Extra | Dependência | Uso |
|---|---|---|
| `[pdf]` | `pdfplumber>=0.10.0` | Parsing de PDFs |
| `[browser]` | `playwright>=1.55.1` | Sites que requerem JS |
| `[polars]` | `polars>=0.19.0` | DataFrames Polars |
| `[bigquery]` | `basedosdados>=2.0.0` | Fallback BigQuery (BCB/SICOR) |
| `[geo]` | `geopandas>=1.0.0` | GeoDataFrames (SICAR, desmatamento, etc) |
| `[app]` | `streamlit>=1.54.0` | App/dashboard Streamlit |

### Dev

```bash
pip install agrobr[dev]
```

Inclui: pytest (+asyncio, cov, recording, timeout), ruff, mypy, pre-commit, pandas-stubs, types-requests, xlwt.

## Regras de Pin

- **Lower bound only** (`>=X.Y.Z`): permite atualizações compatíveis
- **Sem upper bound**: evita "dependency hell" com conflitos de pins
- **Exceção**: se uma versão específica tem bug conhecido, usamos `!=`

## Adicionando dependências

Critérios para aceitar uma nova dependência core:

1. **Necessária**: não há forma razoável de implementar sem ela
2. **Estável**: >= 1.0.0 ou com histórico de estabilidade comprovado
3. **Mantida**: último release < 6 meses
4. **Licença compatível**: MIT, BSD, Apache 2.0
5. **Sem dependências transitivas pesadas**: evitar C extensions complexas

Se uma dependência é útil mas não essencial, vai como **optional extra**.

## Python

- Mínimo suportado: **Python 3.11**
- Target principal: **Python 3.12**
- Testado em CI: 3.11, 3.12, 3.13
