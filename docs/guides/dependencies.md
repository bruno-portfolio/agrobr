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
| `duckdb` | Cache local | `>=0.9.0` |
| `structlog` | Logging estruturado | `>=23.2.0` |
| `chardet` | Detecção de encoding | `>=5.2.0` |
| `typer` | CLI | `>=0.9.0` |
| `openpyxl` | Leitura de Excel | `>=3.1.0` |
| `sidrapy` | API IBGE SIDRA | `>=0.1.4` |

### Opcionais

Instaladas via extras:

```bash
pip install agrobr[pdf]       # pdfplumber para ANDA
pip install agrobr[browser]   # Playwright para sites com JS
pip install agrobr[polars]    # Suporte a Polars DataFrames
pip install agrobr[all]       # Tudo
```

| Extra | Dependência | Uso |
|---|---|---|
| `[pdf]` | `pdfplumber>=0.10.0` | Parsing de PDFs ANDA |
| `[browser]` | `playwright>=1.40.0` | Sites que requerem JS |
| `[polars]` | `polars>=0.19.0` | DataFrames Polars |

### Dev

```bash
pip install agrobr[dev]
```

Inclui: pytest, ruff, mypy, black, pre-commit, pandas-stubs.

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
