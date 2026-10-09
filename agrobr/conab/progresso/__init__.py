"""CONAB Progresso de Safra — % plantio e colheita semanal por cultura e UF.

Dados semanais do acompanhamento das lavouras publicados pela CONAB:
percentuais de semeadura e colheita das principais culturas anuais
por estado, com comparativo do ano anterior e media de 5 anos.

Fonte: https://www.gov.br/conab/pt-br/atuacao/informacoes-agropecuarias/safras/progresso-de-safra
LICENCA: CC BY-ND 3.0 (rodapé da ficha da planilha), classificação livre: reprodução comercial
com atribuição à CONAB, sem distribuir adaptações protegidas.
"""

from agrobr.conab.progresso.api import progresso_safra, semanas_disponiveis

__all__ = ["progresso_safra", "semanas_disponiveis"]
