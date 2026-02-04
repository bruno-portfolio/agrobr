# Contribuindo para o agrobr

Obrigado pelo interesse em contribuir com o agrobr! Este documento fornece diretrizes para contribuições.

## Como Contribuir

### Reportando Bugs

1. Verifique se o bug já não foi reportado nas [Issues](https://github.com/bruno-portfolio/agrobr/issues)
2. Se não encontrar, abra uma nova issue com:
   - Título descritivo
   - Passos para reproduzir
   - Comportamento esperado vs atual
   - Versão do Python e do agrobr
   - Sistema operacional

### Sugerindo Melhorias

1. Abra uma issue descrevendo a melhoria
2. Explique o caso de uso
3. Discuta a implementação antes de começar

### Pull Requests

1. Fork o repositório
2. Crie uma branch para sua feature: `git checkout -b feature/nova-feature`
3. Faça commits com mensagens claras
4. Escreva testes para novas funcionalidades
5. Certifique-se que todos os testes passam
6. Abra um Pull Request

## Setup de Desenvolvimento

```bash
# Clone o repositório
git clone https://github.com/bruno-portfolio/agrobr.git
cd agrobr

# Crie ambiente virtual
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# ou
.venv\Scripts\activate  # Windows

# Instale dependências de desenvolvimento
pip install -e ".[all]"

# Instale Playwright (necessário para alguns scrapers)
playwright install chromium

# Instale pre-commit hooks
pre-commit install
```

## Rodando Testes

```bash
# Todos os testes
pytest

# Com cobertura
pytest --cov=agrobr --cov-report=html

# Apenas testes rápidos (sem network)
pytest -m "not slow"

# Testes de integração
pytest -m integration
```

## Padrões de Código

### Style Guide

- Seguimos o [Google Python Style Guide](https://google.github.io/styleguide/pyguide.html)
- Formatação com `black` (line-length=100)
- Linting com `ruff`
- Type hints obrigatórios (verificados com `mypy --strict`)

### Commits

- Use mensagens em português ou inglês
- Seja descritivo no que foi alterado
- Referencie issues quando aplicável: `Fix #123`

### Docstrings

```python
def funcao(param1: str, param2: int) -> bool:
    """
    Descrição curta da função.

    Descrição mais detalhada se necessário.

    Args:
        param1: Descrição do primeiro parâmetro
        param2: Descrição do segundo parâmetro

    Returns:
        Descrição do retorno

    Raises:
        ValueError: Quando param2 é negativo
    """
    pass
```

### Testes

- Um arquivo de teste por módulo: `tests/test_<modulo>/test_<arquivo>.py`
- Nomes descritivos: `test_<funcionalidade>_<cenario>`
- Use fixtures do pytest quando apropriado
- Mocke chamadas HTTP em testes unitários

## Estrutura do Projeto

```
agrobr/
├── agrobr/
│   ├── cepea/          # Módulo CEPEA
│   ├── conab/          # Módulo CONAB
│   ├── ibge/           # Módulo IBGE
│   ├── cache/          # Sistema de cache DuckDB
│   ├── http/           # Cliente HTTP, retry, rate limiting
│   ├── normalize/      # Normalização de dados
│   ├── validators/     # Validação de dados
│   ├── health/         # Health checks
│   ├── alerts/         # Sistema de alertas
│   └── telemetry/      # Telemetria opt-in
├── tests/              # Testes
├── scripts/            # Scripts utilitários
├── docs/               # Documentação
└── examples/           # Exemplos de uso
```

## Adicionando Nova Fonte de Dados

1. Crie diretório: `agrobr/<fonte>/`
2. Implemente:
   - `client.py`: Cliente HTTP async
   - `parsers/v1.py`: Parser com can_parse() e parse()
   - `parsers/fingerprint.py`: Extração de fingerprint
   - `api.py`: Funções públicas async
   - `models.py` (se necessário): Modelos Pydantic específicos
3. Adicione à CLI: `cli.py`
4. Adicione constantes: `constants.py`
5. Escreva testes
6. Crie golden data
7. Documente

## Criando Novo Parser

Quando o layout de uma fonte muda:

1. Crie `parsers/v{N+1}.py` baseado no anterior
2. Atualize `valid_until` do parser antigo
3. Atualize `expected_fingerprint` do novo
4. Adicione golden data com HTML novo
5. Verifique que testes antigos continuam passando
6. **Nunca delete parsers antigos** (dados históricos podem precisar)

## Dúvidas?

- Abra uma issue com a tag `question`
- Ou entre em contato via [email/discord/etc]

Obrigado por contribuir!
