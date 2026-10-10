# producao_acucar_etanol v1.0

Produção de açúcar, de etanol de cana e de milho e ATR médio por safra e UF, da série histórica industrial da cana da CONAB.

## Fontes

| Prioridade | Fonte | Descrição |
|------------|-------|-----------|
| 1 | CONAB | Séries Históricas — cana-de-açúcar, indústria (`canaseriehist-industria.xls`) |

## Interpretação

Cada aba da planilha vira uma coluna, nas unidades publicadas e sem conversão. Há uma linha por safra e UF: as 27 UFs aparecem em toda safra fechada, mesmo quando a planilha só traz traços para a UF. Regiões, `NORTE/NORDESTE`, `CENTRO-SUL` e `BRASIL` não são publicados.

**Etanol de milho.** A aba "Etanol Total (cana e milho)" soma o anidro e o hidratado de cana e de milho, e `etanol_total_mil_l` é esse total publicado: não é o etanol de cana. Em MT, safra 2024/25, o total é 6.577.571,7 mil litros, dos quais 5.418.000,0 mil litros (82%) são de milho; o etanol de cana (anidro + hidratado de cana) é 1.159.571,7 mil litros, e ler o total como cana o multiplica por 5,7. Somar todas as colunas de etanol conta o etanol duas vezes. O milho aparece na planilha a partir de 2018/19 (GO, MT e PR nessa safra); antes disso as colunas de milho são nulas, não zero.

**Zero × vazio.** Zero publicado sai `0.0`. Traço (`-`), célula vazia e erro do Excel (`#N/A`) saem nulos, nunca zero. Na planilha de 10/10/2026, RR traz zero nas abas de açúcar e etanol de cana/total na maioria das safras; em 2021/22 essas abas e ATR trazem traços. As duas abas de milho ficam vazias para RR em todas as safras fechadas. DF 2019/20 traz `#N/A` no etanol total e no hidratado de cana. ATR `0` só aparece em UF e safra sem açúcar nem etanol de cana: não é uma medida, então tire esses zeros antes de calcular média. ATR é média por tonelada; não some ATR entre UFs.

**Avisos, sem alterar números.** O agrobr repassa os números publicados e avisa (`warnings.warn` e `MetaInfo.validation_warnings`) quando:

- o etanol total difere da soma das quatro parcelas (parcela nula conta zero). Na planilha de 10/10/2026: CE 2009/10, RO 2018/19, RO 2021/22 e SC 2021/22;
- a soma das UFs difere do BRASIL publicado numa coluna de volume. Na planilha de 10/10/2026: açúcar 2005/06, etanol anidro de cana 2009/10 e 2025/26 e etanol total 2021/22;
- a planilha publica erro do Excel numa célula de UF;
- a consulta inclui a safra fechada mais recente da planilha (2025/26 em 10/10/2026), que a CONAB pode revisar nos levantamentos quadrimestrais da safra de cana.

**Estimativa.** A última coluna da planilha, marcada com `(¹)` e explicada no rodapé ("Estimativa em agosto de 2026"), fica fora. Os filtros inclusivos `ano_inicio`/`ano_fim` usam o ano inicial da safra e exigem inteiros; `uf` aceita a sigla.

**Layout.** Aba ausente, desconhecida ou repetida, título ou unidade diferentes dos medidos, UF faltando ou repetida, texto no lugar de número, coluna marcada antes da última safra, estimativa anunciada no rodapé sem coluna marcada e abas com safras diferentes geram `ParseError`. Nada disso vira coluna vazia.

**Modo determinístico.** Não se aplica: a planilha é a corrente. Dentro de `datasets.deterministic(...)` a consulta segue e avisa.

Licença: CONAB, `livre`.

## Schema

| Coluna | Tipo | Nullable | Unidade | Estável |
|--------|------|----------|---------|---------|
| `safra` | str | ❌ | - | Sim |
| `regiao` | str | ❌ | - | Sim |
| `uf` | str | ❌ | - | Sim |
| `acucar_mil_ton` | float | ✅ | mil t | Sim |
| `etanol_anidro_cana_mil_l` | float | ✅ | mil litros | Sim |
| `etanol_hidratado_cana_mil_l` | float | ✅ | mil litros | Sim |
| `etanol_anidro_milho_mil_l` | float | ✅ | mil litros | Sim |
| `etanol_hidratado_milho_mil_l` | float | ✅ | mil litros | Sim |
| `etanol_total_mil_l` | float | ✅ | mil litros | Sim |
| `atr_kg_t` | float | ✅ | kg/t de cana | Sim |

**Primary key:** `[safra, uf]`

**Constraints:** todas as medidas `>= 0`

## Garantias

- PK única por safra + uf
- Só linhas de UF, as 27 em toda safra fechada; regiões, `NORTE/NORDESTE`, `CENTRO-SUL` e `BRASIL` ficam fora
- Safras fechadas desde 2005/06; a coluna da estimativa (marcada com nota) fica fora
- Unidades da fonte, sem conversão: açúcar em mil t, etanol em mil litros, ATR em kg/t de cana
- Zero publicado sai `0.0`; traço, célula vazia e erro do Excel saem nulos, nunca zero
- `etanol_total_mil_l` é o publicado e inclui o etanol de milho; total diferente da soma das 4 parcelas (vazio conta 0) gera aviso em `validation_warnings` e `UserWarning`, sem alterar número
- Soma das UFs diferente do BRASIL publicado nas colunas de volume gera aviso, sem alterar número
- Medidas `>= 0` quando presentes
- Aba, título, unidade, UFs ou colunas de safra diferentes do layout medido levantam `ParseError`

## Exemplo

Medidas usam `float64`, inclusive no vazio; texto usa o dtype padrão do pandas. `as_polars` e `return_meta` são somente nomeados. `conab.cana_industria()` devolve a mesma tabela direto da fonte.

```python
from agrobr import datasets

# Async
df = await datasets.producao_acucar_etanol(ano_inicio=2020, ano_fim=2025, uf="MT")
etanol_cana = df["etanol_anidro_cana_mil_l"] + df["etanol_hidratado_cana_mil_l"]
etanol_milho = df[["etanol_anidro_milho_mil_l", "etanol_hidratado_milho_mil_l"]].sum(
    axis=1, min_count=1
)

# Com metadados e avisos
df, meta = await datasets.producao_acucar_etanol(return_meta=True)
print(meta.validation_warnings)

# Sync
from agrobr.sync import datasets
df = datasets.producao_acucar_etanol(2024, 2025)
```

## Schema JSON

Disponível em `agrobr/schemas/producao_acucar_etanol.json`.

```python
from agrobr.contracts import get_contract
contract = get_contract("producao_acucar_etanol")
print(contract.to_json())
```
