# SICOR — RegiaoUF de 2022 e 2023, entidades por produto e municípios de jan/2023

Corpos oficiais do OData v2 do SICOR (Olinda), capturados na conferência
(25/09/2026). Cada arquivo é o gzip da resposta HTTP 200 como recebida, sem `$select`, exceto a `SemFiltros`, que veio com
os 17 campos da URL (o corpo inteiro passa de 14 MB por mês); `manifest.json` traz a URL, as linhas, os bytes e o SHA-256 do
corpo descomprimido, e o SHA-256 do arquivo.

- `RegiaoUF_AAAA_MM.json.gz`: os 24 meses de jan/2022 a dez/2023, com as quatro finalidades em colunas. Cobrem a safra
  2022/23 inteira (jul/2022 a jun/2023) e os dois anos civis.
- `CusteioRegiaoUFProduto_2023_01`, `InvestRegiaoUFProduto_2023_01` e `ComercRegiaoUFProduto_2023_01`: as entidades por
  produto de jan/2023, lidas pelo `credito_rural`.
- `CusteioInvestimentoComercialIndustrialSemFiltros_2023_01`: os municípios de jan/2023, sem produto.
- `metadata.xml`: o `$metadata` do serviço (sem gzip), com as propriedades de cada entidade.

Oráculo (soma com `json` e `Decimal`, sem o parser do agrobr), igual à tabela da conferência:

| Ano | Finalidade | Contratos | Valor (R$) |
|---|---|---:|---:|
| 2022 | custeio | 942.825 | 207.709.040.433,19 |
| 2022 | investimento | 1.023.854 | 100.448.934.321,77 |
| 2022 | comercializacao | 22.988 | 32.986.781.718,21 |
| 2022 | industrializacao | 1.732 | 21.849.654.291,96 |
| 2023 | custeio | 964.287 | 225.139.296.410,76 |
| 2023 | investimento | 1.129.634 | 101.930.864.138,82 |
| 2023 | comercializacao | 33.610 | 51.413.811.887,88 |
| 2023 | industrializacao | 1.868 | 28.089.305.375,65 |

Na safra 2022/23, 104 pares UF × finalidade têm operação; faltam a industrialização no AM, no AP e em RR e a
comercialização no AP. Em jan/2023, a soma dos municípios e a soma por produto (custeio, investimento e comercialização)
fecham com a `RegiaoUF` em cada UF.
