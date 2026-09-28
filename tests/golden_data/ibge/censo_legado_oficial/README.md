Arquivos ZIP originais do Censo Agropecuário 1995/96, preservados sem alteração.
URLs e SHA-256 estão em `PROVENANCE.json`.

Brasil: tecnologia Tab2, pessoal Tab5, máquinas Tab7, animais Tab6, valor da
produção Tab10, financeiro Tab11 (despesas) e Tab12 (receitas). Goiás: temas
3/6/7/9/10/11; São Paulo e Distrito Federal: financeiro Tab11.

O XLS BIFF5 guarda títulos e cabeçalhos em objetos de texto, fora das células.
As medidas com formato numérico `##0,` usam escala /1000. Os testes verificam
células brutas e XF independentemente do parser: Brasil Tab11 C12 = 4622807
informantes; D12 = 26880228229 bruto, ou 26880228.229 mil reais após a escala.
Goiás inclui HTML nos ZIPs 3/6/7/9; a indentação diferencia níveis geográficos.

A leitura dos objetos segue a [especificação BIFF da Microsoft](https://www.loc.gov/preservation/digital/formats/digformatspecs/Excel97-2007BinaryFileFormat(xls)Specification.pdf).
A [documentação Microsoft de formatos numéricos](https://support.microsoft.com/en-us/excel/review-guidelines-for-customizing-a-number-format)
fundamenta a escala por vírgulas. As unidades vêm dos cabeçalhos históricos;
não há atualização monetária, arredondamento de exibição ou códigos municipais
atuais inferidos de nomes de 1995/96.

Regressões adicionais do default estadual: Acre usa `tab_3mn.zip` em
minúsculas, assim como Alagoas, Amapá e Amazonas; Maranhão e Mato Grosso
incluem objetos BIFF5 sem nome textual; Sergipe usa BIFF8. As quatro fixtures
adicionais preservam cada um desses casos. BIFF8 usa propriedades OfficeArt
para nome e âncora, e TxO/Continue para texto, conforme as especificações
[TxO](https://learn.microsoft.com/en-us/openspecs/office_file_formats/ms-xls/638c08e6-2942-4783-b71b-144ccf758fc7),
[âncora](https://learn.microsoft.com/en-us/openspecs/office_file_formats/ms-xls/fd656a2c-d5ee-4171-8f65-17a08b9f2262)
e [nome de forma](https://learn.microsoft.com/en-us/openspecs/office_file_formats/ms-odraw/aaa94f58-eab7-4e88-ba37-de97d319c2e7).

Os 447 hífens numéricos originais dos quatro HTML de Goiás representam zero.
A legenda da [publicação nacional 1995/96](https://biblioteca.ibge.gov.br/visualizacao/periodicos/48/agro_1995_1996_n1_br.pdf),
página 24 do PDF, distingue fenômeno inexistente (`-`) de valor inferior à
unidade adotada (`0`). Textos como `...` continuam nulos; não são atribuídos
a essa legenda histórica.
