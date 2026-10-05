from __future__ import annotations

from agrobr.bruto import arquivo

from .models import CSV_URL

adaptador = arquivo.AdaptadorArquivo(
    CSV_URL,
    None,
    arquivo.conferir_csv("SEQ_TAD", "NUM_TAD"),
    aviso=(
        "ibama: o CSV de termos de embargo traz nome e CPF/CNPJ das pessoas físicas e jurídicas "
        "embargadas; é dado pessoal: guarde e trate o arquivo conforme a LGPD."
    ),
)
