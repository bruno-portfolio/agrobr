# serie_historica_safra v1.1

Série histórica de safras por produto, safra, região e UF.

## Fontes

| Prioridade | Fonte | Descrição |
|------------|-------|-----------|
| 1 | CONAB | Séries Históricas de Safras |

## Produtos

45 produtos: `soja`, `milho`, `milho_1`, `milho_2`, `milho_3`, `arroz`, `arroz_irrigado`, `arroz_sequeiro`, `feijao`, `feijao_1`, `feijao_2`, `feijao_3`, `feijao_caupi`, `feijao_caupi_1`, `feijao_caupi_2`, `feijao_caupi_3`, `feijao_cores`, `feijao_cores_1`, `feijao_cores_2`, `feijao_cores_3`, `feijao_preto`, `feijao_preto_1`, `feijao_preto_2`, `feijao_preto_3`, `algodao`, `algodao_pluma`, `algodao_caroco`, `trigo`, `sorgo`, `aveia`, `cevada`, `canola`, `girassol`, `mamona`, `amendoim`, `amendoim_1`, `amendoim_2`, `centeio`, `triticale`, `gergelim`, `cafe`, `cafe_arabica`, `cafe_conilon`, `cana`, `cana_area_total`

## Interpretação dos produtos

Cada produto é uma série da CONAB: cultura, safra (`milho_1` a `milho_3`, feijões) ou recorte (`cana_area_total`, `algodao`, `algodao_pluma`, `algodao_caroco`).

O período `safra` segue o publicado: ano civil `YYYY` para `cafe`, `cafe_arabica`, `cafe_conilon`, `trigo`, `aveia`, `cevada`, `canola`, `centeio`, `triticale`; os demais produtos usam `YYYY/YY`. Os filtros `inicio`/`fim` continuam usando o ano inicial.

A coluna de previsão (rótulos como `Previsão` ou `(¹)`) não entra na série histórica; sua exclusão fica registrada por produto, aba e rótulo. Para a safra em curso, use `estimativa_safra` nos produtos disponíveis nesse dataset.

As safras que o levantamento mensal de grãos da CONAB ainda publica (a corrente e a anterior; o 1º levantamento de cada safra sai em outubro) podem ter sido revisadas depois do XLS anual. Consulta de grãos que inclua essas safras emite um aviso (`warnings.warn`, uma vez por produto e safras) apontando `conab.safras` e `estimativa_safra`; o número da série não é substituído. Café e cana têm levantamentos próprios e não recebem esse aviso.

`algodao` representa algodão em caroço; `algodao_pluma`, pluma; `algodao_caroco`, semente (caroço de algodão). Os três usam a mesma área, com produção e produtividade do respectivo recorte.

`cana` publica a aba Área, cujo título na planilha oficial é "Série Histórica de Área Colhida", em `area_colhida_mil_ha`; `area_plantada_mil_ha` fica nula nesse produto e não é imputada a partir de `cana_area_total`.

`regiao` é a macrorregião sob a qual a planilha lista a UF, reconhecida só pelo rótulo exato. Sub-regiões (as do café na Bahia e em Minas Gerais) e agregados (`NORTE/NORDESTE`, `CENTRO-SUL`, `OUTROS`) não mudam a região e não são publicados.

`cana_area_total` usa somente a aba Área Total, e a composição que a CONAB publica nela muda ao longo da série. De 2007/08 a 2021/22 (exceto 2016/17), é a colhida mais o plantio (expansão e renovação) mais as mudas. Em 2016/17, é igual à colhida em 23 UFs. Em 2022/23, soma colhida e plantio, sem as mudas, em 19 UFs. De 2023/24 a 2025/26, é igual à colhida em 19 UFs, entre elas SP e MG. O agrobr entrega o número publicado; comparar safras nessa coluna exige levar a quebra em conta.

Células vazias da Área Total não recebem valores de Área Colhida; produção e produtividade permanecem nulas. Abas de mudas e modalidades de colheita não integram essa série.

Zero publicado na planilha sai `0.0`, inclusive o de UF sem produção naquela safra (a planilha lista as 27 UFs). A exceção é a coluna de safra com zero em todas as UFs, que é safra não levantada e não gera linha: trigo 1976 tem zero até na linha BRASIL, e canola, triticale, girassol, feijão 3ª safra e milho 2ª safra começam assim. Nas 40 planilhas de grãos e cana medidas em 23/09/2026, 7.533 zeros são de safra não levantada; os outros 43.981 zeros publicados saem `0.0`. O café não tem coluna assim.

Abas selecionadas ilegíveis, ausentes ou ambíguas geram `ParseError`; filtros sem observações retornam uma tabela vazia com schema. Abas desconhecidas geram aviso e são apontadas na reconciliação.

## Schema

| Coluna | Tipo | Nullable | Unidade | Estável |
|--------|------|----------|---------|---------|
| `produto` | str | ❌ | - | Sim |
| `safra` | str | ❌ | - | Sim |
| `regiao` | str | ✅ | - | Sim |
| `uf` | str | ✅ | - | Sim |
| `area_plantada_mil_ha` | float | ✅ | mil ha | Sim |
| `area_em_producao_mil_ha` | float | ✅ | mil ha | Não (opcional, desde 1.1) |
| `area_formacao_mil_ha` | float | ✅ | mil ha | Não (opcional, desde 1.1) |
| `area_colhida_mil_ha` | float | ✅ | mil ha | Não (opcional, desde 1.1) |
| `producao_mil_ton` | float | ✅ | mil ton | Sim |
| `produtividade_kg_ha` | float | ✅ | kg/ha | Sim |

**Primary key:** `[produto, safra, regiao, uf]`

**Constraints:** `area_plantada_mil_ha >= 0`, `producao_mil_ton >= 0`, `produtividade_kg_ha >= 0`

## Garantias

- PK única por combinação produto + safra + região + uf
- `produto` lowercase (ex: soja, milho_2)
- `safra` é o período publicado pela CONAB: YYYY/YY (ex: 2023/24) ou YYYY (ex: 2025; café e cereais de inverno)
- `regiao` quando presente: NORTE, NORDESTE, CENTRO-OESTE, SUDESTE, SUL
- `uf` quando presente: código UF de 2 letras uppercase
- Métricas (area, produção, produtividade) >= 0 quando presentes

Para café, `area_plantada_mil_ha` é a soma das áreas em produção e em formação;
permanece nula se um dos componentes estiver ausente. As duas áreas oficiais são
preservadas separadamente. A produção em mil sacas beneficiadas de 60 kg é
convertida para mil toneladas (× 0,06); a produtividade em sacas/ha é convertida
para kg/ha (× 60) e continua referida à área em produção.

`cana_industria` foi retirado do suporte anunciado na versão 2.0 do pacote: suas
métricas de açúcar, etanol e ATR exigem um contrato próprio.

## Exemplo

```python
from agrobr import datasets

# Async
df = await datasets.serie_historica_safra("soja")
df = await datasets.serie_historica_safra("soja", inicio=2020, fim=2024, uf="MT")

# Com metadados
df, meta = await datasets.serie_historica_safra("soja", return_meta=True)

# Sync
from agrobr.sync import datasets
df = datasets.serie_historica_safra("soja")
```

## Schema JSON

Disponível em `agrobr/schemas/serie_historica_safra.json`.

```python
from agrobr.contracts import get_contract
contract = get_contract("serie_historica_safra")
print(contract.to_json())
```
