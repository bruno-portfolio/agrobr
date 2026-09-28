from __future__ import annotations

import re

import structlog

from agrobr.exceptions import InvalidParameterError
from agrobr.normalize import regions

logger = structlog.get_logger()

SICOR_PROGRAMAS: dict[str, str] = {
    "0001": "PRONAF",
    "0050": "PRONAMP",
    "0070": "FUNCAFÉ (PROGRAMA DE DEFESA DA ECONOMIA CAFEEIRA)",
    "0100": "PRLC-BA (PROG RECUP LAVOURA CACAUEIRA BAIANA) ENCERRADO",
    "0110": "PRODECER III",
    "0151": "PROCAP-AGRO (PROGRAMA DE CAPITALIZAÇÃO DAS COOPERATIVAS DE PRODUÇÃO AGROPECUÁRIAS)",
    "0152": "PROIRRIGA",
    "0153": "MODERAGRO",
    "0154": "MODERFROTA",
    "0155": "PRODECOOP",
    "0156": "ABC + Programa para a Adaptação à Mudança do Clima e Baixa Emissão de Carbono",
    "0157": "PSI-RURAL",
    "0158": "PROCAP-CRED (PROG CAPIT COOP CRÉDITO) ENCERRADO",
    "0159": "MODERMAQ",
    "0160": "PRI",
    "0161": "PRORENOVA-RURAL- PROG APOIO  RENOV IMPLANTAÇÃO NOVOS CANAVIAIS- ENCERRADO",
    "0162": "INOVAGRO",
    "0163": "PCA",
    "0164": "PRORENOVA-IND- PROG APOIO RENOV IMPLANT NOVOS CANAVIAIS",
    "0165": "PROAQÜICULTURA-PROG APOIO DESENVSETOR AQUÍCOLA",
    "0180": "FNO-ABC (PROG FINANC AGRICULTURA BAIXO CARBONO) ENCERRADO",
    "0200": "PROCERA",
    "0201": "PROGRAMA NACIONAL DE CRÉDITO FUNDIÁRIO (FTRA)",
    "0222": "RenovAgro",
    "0240": "ANF",
    "0721": "Linha Crédito Rural instit Res. 4.028/2011 (Dívidas Composição e Renegoc PRONAF)",
    "0722": "Linha Crédito Rural inst Res. 4.029/2011 (Reneg Crédito Fundiário) ENCERRADO",
    "0730": "Linha Crédito Rural inst Res. 4.083/2012 (Enchentes Reg Norte) ENCERRADO",
    "0735": "Linha de Crédito Rural instituida pela Res. 4.126/2012 (Produtores de Maçã) ENCERRADO",
    "0776": "Linha Crédito Rural Inst Res. 4.147/2012 e 4.260/2013 (Agricultores Familiares) ENCERRADO",
    "0777": "Linha Crédito Rural inst Res. 4.147/2012 e 4.260/2013 (Demais Agricultores) ENCERRADO",
    "0779": "Linha de Crédito Rural instituida pela Res. 4.161/2012 (Produtores de Arroz) ENCERRADO",
    "0783": "Linha Crédito Rural inst pelas Res 4.189 e 4.212/2013-PRONAF (Estiagem Area Sudene) ENCERRADO",
    "0784": "Linha Credito Rural inst Res. 4.188 e 4.211/2013-Demais Produtores (Estiagem Area Sudene) ENCERRADO",
    "0785": "Linha Crédito Rural inst  Res. 4.220/2013 (Recursos BNDES-Estiagem Área da Sudene) ENCERRADO",
    "0786": "Linha de Crédito Rural Instituída pela Res. 4.289/2013 (Renegociação Café Arábica) ENCERRADO",
    "0790": "Linha de Crédito Rural inst Res 5.120/2024 (Linha emergencial Custeio Pecuário)",
    "0888": "Outras Linhas de Crédito Rural não Especificadas",
    "0901": "Eco Invest Brasil",
    "0999": "FINANCIAMENTO SEM VÍNCULO A PROGRAMA ESPECÍFICO",
}

SICOR_TIPOS_SEGURO: dict[str, str] = {
    "0": "Não se aplica",
    "1": "Proagro tradicional",
    "2": "Proagro mais",
    "3": "Outro seguro",
    "9": "Sem adesão a seguro",
}


SICOR_FONTES_RECURSO: dict[str, str] = {
    "0100": "TESOURO NACIONAL - DIRECIONADA/CONTROLADA",
    "0201": "OBRIGATÓRIOS - MCR 6.2 - DIRECIONADA/CONTROLADA",
    "0202": "EXIGIBILIDADE ADICIONAL DOS RECURSOS À VISTA",
    "0222": "Exigibilidade Adicional dos Recursos à Vista - Resolução 5030 ENCERRADO",
    "0225": "Exigibilidade Adicional dos Recursos à Vista - Resolução 5087 - ENCERRADO",
    "0226": "Exigibilidade Adicional dos Recursos à Vista - Resolução 5157",
    "0250": "FACULDADE DE APLICAÇÃO - COMPULSÓRIO",
    "0260": "COMPULSÓRIO SOBRE RECURSOS À VISTA - REFORÇO DO INVESTIMENTO (CIRC 3.745)",
    "0300": "POUPANÇA RURAL - EQUALIZADA - DIRECIONADA/CONTROLADA",
    "0301": "POUPANÇA RURAL - CONTROLADOS - CONDIÇÕES MCR 6.2",
    "0302": "POUPANÇA RURAL - CONTROLADOS - FATOR DE PONDERAÇÃO",
    "0303": "POUPANÇA RURAL - DIRECIONADA/NÃO CONTROLADA",
    "0304": "EXIGIBILIDADE ADICIONAL DA POUPANÇA RURAL",
    "0402": "RECURSOS LIVRES - LIVRE/NÃO CONTROLADA",
    "0403": "RECURSOS LIVRES - EQUALIZADA - LIVRE/CONTROLADA",
    "0405": "FUNDO DE COMMODITIES",
    "0430": "LETRA DE CRÉDITO DO AGRONEGÓCIO (LCA) - DIRECIONADA/NÃO CONTROLADA",
    "0431": "LETRA DE CRÉDITO DO AGRONEGÓCIO (LCA) - EQUALIZADA - DIRECIONADA/CONTROLADA",
    "0440": "LETRA DE CRÉDITO DO AGRONEGÓCIO (LCA) - TAXA FAVORECIDA",
    "0450": "INSTR HIBRIDO CAPITAL DÍVIDA-IHCD (Lei 12.793/2013 - Art. 6º) - EQUALIZÁVEL",
    "0451": "INSTR HIBRIDO CAPITAL DÍVIDA-IHCD (Lei 12.793/2013 - Art. 6º) - DIRECIONADA",
    "0501": "FUNDO CONSTITUCIONAL DE FINANCIAMENTO DO NORTE (FNO) - DIRECIONADA/CONTROLADA",
    "0502": "FUNDO CONSTITUCIONAL DE FINANCIAMENTO DO NORDESTE (FNE) - DIRECIONADA/CONTROLADA",
    "0503": "FUNDO CONSTITUCIONAL DE FINANCIAMENTO DO CENTRO-OESTE (FCO) - DIRECIONADA/CONTROLADA",
    "0505": "BNDES/FINAME - EQUALIZADA - DIRECIONADA/CONTROLADA",
    "0506": "BNDES - LIVRE/NÃO CONTROLADA",
    "0507": "INCRA - DIRECIONADA/CONTROLADA",
    "0520": "FUNDO DE TERRAS E DA REFORMA AGRÁRIA - DIRECIONADA/CONTROLADA",
    "0600": "GOVERNOS E FUNDOS ESTADUAIS OU MUNICIPAIS - DIRECIONADA/CONTROLADA",
    "0650": "FAT - FUNDO DE AMPARO AO TRABALHADOR - DIRECIONADA/CONTROLADA",
    "0680": "PIS/PASEP",
    "0800": "FUNCAFE - FUNDO DE DEFESA DA ECONOMIA CAFEEIRA - DIRECIONADA/CONTROLADA",
    "0850": "CAPTAÇÃO EXTERNA - LIVRE/NÃO CONTROLADA",
    "0900": "ATIVIDADE NÃO FINANCIADA ENQUADRADA NO PROAGRO (MCR 16.8)",
    "0910": "FUNDO NACIONAL SOBRE MUDANÇA (FUNDO CLIMA) - RES CMN 5.130/2024",
    "0911": "FUNDO DE DESENVOLVIMENTO CIENTÍFICO E TECNOLÓGICO (FNDCT) - MP 1374 – DIRECIONADA/CONTROLADA",
    "0990": "OUTRAS FONTES DE RECURSOS NÃO ESPECIFICADAS",
}

SICOR_MODALIDADES: dict[str, str] = {
    "0": "Indicador de renegociação",
    "1": "LAVOURA",
    "2": "EXTRATIVISMO DE ESPÉCIES NATIVAS",
    "3": "BENEFICIAMENTO OU INDUSTRIALIZAÇÃO",
    "4": "Crédito para Cooperativa Pronaf MCR 10-3-1A",
    "5": "AQUISIÇÃO DE INSUMOS PARA INDÚSTRIA FAMILIAR",
    "7": "Crédito de Investimento para Agregação de Renda MCR 10-18-17",
    "8": "Crédito de Investimento para Agregação de Renda MCR 10-18-16",
    "10": "PASTAGEM",
    "11": "FLORESTAMENTO E REFLORESTAMENTO",
    "12": "FORMAÇÃO DE CULTURAS PERENES",
    "13": "MELHORAMENTO DAS EXPLORAÇÕES",
    "14": "MÁQUINAS, EQUIPAMENTOS, MATERIAIS E UTENSÍLIOS",
    "15": "AQUISIÇÃO DE VEÍCULOS",
    "16": "AQUISIÇÃO DE ANIMAIS DE SERVIÇO (USO AGRICULTURA)",
    "17": "Aquisição de Matéria Prima direto do Produtor/Cooperativa",
    "18": "FEPM (EX-EGF) - encerrado",
    "19": "FAC - Financiamento para Aquisição de Café",
    "20": "FEE (EX-LEC)",
    "21": "PRÉ-COMERCIALIZAÇÃO - encerrado",
    "22": "PROTEÇÃO DE PREÇOS/PRÊMIOS - encerrado",
    "23": "DESCONTO (NPR E DR)",
    "24": "CPR (CÉDULA DE PRODUTO RURAL)",
    "25": "ESTOCAGEM",
    "26": "MANUTENÇÃO/CRIAÇÃO DE ANIMAIS (RECRIA E ENGORDA)",
    "27": "AQUISIÇÃO DE ANIMAIS",
    "28": "AQUISIÇÃO DE ANIMAIS DE SERVIÇO",
    "29": "CRIA DE ANIMAIS",
    "30": "FGPP-Financiamento para Garantia de Preços ao Produtor",
    "31": "AQUISIÇÃO E MANUTENÇÃO DE ANIMAIS",
    "32": "IMPLANTAÇÃO E MELHORAMENTO",
    "33": "Financiamento para Aquisição da Produção/Materia Prima - encerrado",
    "34": "MONITORAMENTO AMBIENTAL",
    "35": "MANUTENÇÃO/CRIAÇÃO DE ANIMAIS (CRIA)",
    "37": "BOVINOCULTURA",
    "38": "SUINOCULTURA",
    "39": "AVICULTURA",
    "40": "ATENDIMENTO A COOPERADOS",
    "41": "INTEGRALIZAÇÃO DE COTAS PARTES",
    "42": "TAXA DE RETENÇÃO",
    "43": "EXPLORAÇÃO SOB REGIME DE INTEGRAÇÃO - ENCERRADO",
    "44": "Agroindústria Familiar (MCR 10-11)",
    "45": "COOPERATIVAS DE CRÉDITO (SINGULAR OU CENTRAL) - encerrado",
    "46": "FINANCIAMENTO PARA PROTEÇÃO DE PREÇOS EM OPERAÇÕES NO MERCADO FUTURO E DE OPÇÕES",
    "47": "FINANCIAMENTO PROCAP-AGRO",
    "48": "GASTOS FUNDAMENTAIS AO BEM-ESTAR FAMILIAR (MCR 3-2-9)",
    "50": "PESCA",
    "55": "APICULTURA",
    "56": "MINHOCULTURA",
    "60": "AQUICULTURA",
    "63": "BUBALINOCULTURA",
    "65": "SERICICULTURA",
    "67": "CAPRINOCULTURA",
    "70": "AQUISIÇÃO DE ATIVOS OPERACIONAIS",
    "73": "EQUINOCULTURA",
    "75": "CUNICULTURA E DEMAIS ROEDORES",
    "77": "OVINOCULTURA",
    "80": "PESQUISA E ASSISTÊNCIA AGROPECUÁRIA",
    "85": "AQUISIÇÃO DE PROPRIEDADES RURAIS",
    "90": "SERVIÇOS PROFISSIONAIS/TÉCNICOS",
    "95": "COVID-19 - Resolução 4801/2020 - Encerrado",
    "96": "ESTIAGEM - Resolução 4802/2020 - Encerrado",
    "98": "Renegociação Parcial com Nova Operação - Encerrrado",
    "99": "Renegociação Total com Nova Operação - Encerrado",
}

SICOR_ATIVIDADES: dict[str, str] = {
    "1": "Agrícola",
    "2": "Pecuário(a)",
}


def _resolve(dicionario: dict[str, str], codigo: str, dominio: str) -> str:
    nome = dicionario.get(codigo)
    if nome is not None:
        return nome
    logger.warning("sicor_codigo_desconhecido", dominio=dominio, codigo=codigo)
    return f"Desconhecido ({codigo})"


def resolve_programa(cd: str) -> str:
    return _resolve(SICOR_PROGRAMAS, cd, "programa")


def resolve_tipo_seguro(cd: str) -> str:
    return _resolve(SICOR_TIPOS_SEGURO, cd, "tipo_seguro")


def resolve_fonte_recurso(cd: str) -> str | None:
    return SICOR_FONTES_RECURSO.get(cd)


def resolve_modalidade(cd: str) -> str | None:
    return SICOR_MODALIDADES.get(cd.lstrip("0") or "0")


def resolve_atividade(cd: str) -> str | None:
    return SICOR_ATIVIDADES.get(cd)


SICOR_PRODUTOS: dict[str, str] = {
    "soja": '"SOJA"',
    "milho": '"MILHO"',
    "arroz": '"ARROZ"',
    "feijao": '"FEIJÃO"',
    "trigo": '"TRIGO"',
    "algodao": '"ALGODÃO"',
    "cafe": '"CAFÉ"',
    "cana": '"CANA-DE-AÇUCAR"',
    "mandioca": '"MANDIOCA (AIPIM, MACAXEIRA)"',
    "sorgo": '"SORGO"',
}

UF_CODES: dict[str, str] = {
    "RO": "11",
    "AC": "12",
    "AM": "13",
    "RR": "14",
    "PA": "15",
    "AP": "16",
    "TO": "17",
    "MA": "21",
    "PI": "22",
    "CE": "23",
    "RN": "24",
    "PB": "25",
    "PE": "26",
    "AL": "27",
    "SE": "28",
    "BA": "29",
    "MG": "31",
    "ES": "32",
    "RJ": "33",
    "SP": "35",
    "PR": "41",
    "SC": "42",
    "RS": "43",
    "MS": "50",
    "MT": "51",
    "GO": "52",
    "DF": "53",
}

SICOR_TOTAL_FINALIDADES: dict[str, tuple[str, str]] = {
    "custeio": ("QtdCusteio", "VlCusteio"),
    "investimento": ("QtdInvestimento", "VlInvestimento"),
    "comercializacao": ("QtdComercializacao", "VlComercializacao"),
    "industrializacao": ("QtdIndustrializacao", "VlIndustrializacao"),
}

SICOR_TOTAL_CAMPOS: tuple[str, ...] = (
    "nomeUF",
    "AnoEmissao",
    "MesEmissao",
    "cdPrograma",
    *(campo for par in SICOR_TOTAL_FINALIDADES.values() for campo in par),
)


def normalize_safra_sicor(safra: str) -> str:
    texto = safra.strip() if isinstance(safra, str) else ""
    if re.fullmatch(r"[1-9]\d{3}", texto):
        return f"{int(texto) - 1}/{texto}"
    match = re.fullmatch(r"([1-9]\d{3})/(\d{2}|\d{4})", texto)
    if match:
        ano_fim = int(match.group(1)) + 1
        if match.group(2) in {str(ano_fim), f"{ano_fim % 100:02d}"}:
            return f"{match.group(1)}/{ano_fim}"
    raise InvalidParameterError(
        f"safra inválida: {safra!r}; use AAAA, AAAA/AA ou AAAA/AAAA com anos consecutivos"
    )


def normalize_produto_sicor(produto: str) -> str:
    return regions.remover_acentos(produto.strip().strip('"').strip()).casefold()


def resolve_produto_sicor(produto: str) -> str:
    if not isinstance(produto, str) or not produto.strip():
        raise InvalidParameterError("produto deve ser uma string não vazia")
    lower = normalize_produto_sicor(produto)
    if lower in {"cafe_arabica", "cafe_conilon"}:
        raise InvalidParameterError("O SICOR não distingue café arábica/conilon; use 'cafe'")
    name = produto.strip().strip('"').upper()
    return SICOR_PRODUTOS.get(lower, f'"{name}"')
