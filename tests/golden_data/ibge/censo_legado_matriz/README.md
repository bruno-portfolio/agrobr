# Censo Agropecuário 1995/96 — seis temas nas 27 UFs

O manifesto cobre 162 URLs oficiais: tecnologia, pessoal ocupado, máquinas, produção animal, valor da produção e financeiro para cada UF. Referências `fixture` são relativas a `tests/golden_data`. Doze caminhos do diretório `censo_legado_oficial` são reutilizados sem alteração; este diretório acrescenta 149 ZIPs únicos. As 162 URLs somam 161 conteúdos distintos.

Os bytes são oficiais e não foram recortados ou reconstruídos. A captura desta expansão realizou 128 downloads, reutilizou 22 capturas de tecnologia anteriores e referenciou as 12 fixtures estaduais existentes. O manifesto mantém URL, SHA-256, tamanho, método, href do índice e timestamp quando ele foi registrado na captura original; timestamps antigos não disponíveis ficam nulos, sem inventar horário.

`expected.json` foi construído por build_oracles.py, que não importa `agrobr`. O leitor usa células/tipos/formatos de `xlrd` e HTML com `BeautifulSoup(..., "html.parser")`; os cabeçalhos brutos BIFF/HTML foram conferidos separadamente em `source_inventory.json`. As seis sequências de medidas e unidades foram declaradas a partir desses cabeçalhos, não da saída do parser. Há 778 células de totais estaduais e 2.305 células entre totais e primeiro/último município de cada arquivo, além de 125 exemplos individuais de zero. As contagens se sobrepõem. São confrontadas também as contagens de linhas, municípios, zeros e nulos de cada arquivo.

O oráculo usa um mapa explícito dos dois formatos XF observados: inteiro ou divisor 1.000. Em HTML, as medidas já estão na unidade impressa; hífen numérico é zero conforme a legenda da publicação de 1995/96. Foram observados 9.988 zeros nas linhas dos 161 arquivos válidos e nenhum valor numérico desconhecido/nulo; não foram inventadas ausências para aumentar cobertura. Os controles preexistentes de marcadores nulos continuam em `test_censo_legado_oficial.py`.

As famílias reais são 149 BIFF70, dez HTML (GO, RN e SC) e três BIFF80 (tecnologia, pessoal e máquinas de SE). A leitura dos mesmos bytes usa `xlrd` também no produto, portanto não é uma segunda engine de XLS. A independência é da seleção, dos cálculos e das expectativas; não se afirma revisão visual de cada célula municipal.

## Exceção oficial do Pará

`Para/Tab_7Mn.zip` e `Para/Tab_6Mn.zip` devolvem exatamente os mesmos 9.733 bytes, SHA-256 `38f3873778dd67255647334b869b3111018fb40e1dc5b4d093b9ba59e4b7a1be`, com cabeçalho de pessoal ocupado. As duas URLs referenciam `Para_Tab_6Mn.zip` no manifesto. O caso máquinas deve levantar `ParseError`; a cobertura é **161 pares com dados válidos e uma rejeição correta**, não 162 sucessos de dados.

A sonda limitada `Para/Tab_7.zip` retorna tratores por potência e atividade econômica, uma tabela diferente. Não é usada como substituto municipal. O par de ZIPs incorretos foi confirmado por uma leitura independente.

## Regressão de cabeçalhos SE

No ZIP oficial de máquinas de Sergipe, as cinco caixas de texto são paralelas, mas seus limites horizontais se sobrepõem. As células B7:F7 são 2.984 tratores, 578 máquinas para plantio, 149 para colheita, 588 caminhões e 975 utilitários. A regressão impede que caixas na mesma altura virem uma hierarquia falsa. As alturas BIFF são discretizadas em 1/256 de linha; níveis verticais distintos continuam formando cabeçalhos hierárquicos.
