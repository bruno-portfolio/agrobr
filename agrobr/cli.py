from __future__ import annotations

import json
import logging
import sys
import warnings
from enum import StrEnum
from typing import Any

import typer
from structlog import dev

from agrobr import __version__, _log, constants


class Formato(StrEnum):
    TABLE = "table"
    CSV = "csv"
    JSON = "json"


class SaidaHealth(StrEnum):
    TEXT = "text"
    JSON = "json"


class Pesquisa(StrEnum):
    PAM = "pam"
    LSPA = "lspa"


def _parse_anos(ano: str | None) -> int | list[int] | None:
    if ano is None:
        return None
    try:
        anos = [int(valor.strip()) for valor in ano.split(",")]
    except ValueError:
        raise typer.BadParameter(
            "ano inválido; use um ano inteiro ou uma lista, como 2020,2021",
            param_hint="--ano",
        ) from None
    return anos if "," in ano else anos[0]


def _output_df(df: Any, formato: Formato) -> None:
    if formato == Formato.JSON:
        registros = json.loads(df.to_json(orient="records", date_format="iso"))
        typer.echo(json.dumps(registros, ensure_ascii=False, indent=2))
    elif formato == Formato.CSV:
        typer.echo(df.to_csv(index=False, lineterminator="\n"), nl=False)
    elif df.empty:
        typer.echo("Nenhum dado encontrado")
    else:
        typer.echo(df.to_string(index=False))


app = typer.Typer(
    name="agrobr",
    help="Dados agrícolas brasileiros em uma linha de código",
    add_completion=False,
)


def version_callback(value: bool) -> None:
    if value:
        typer.echo(f"agrobr version {__version__}")
        raise typer.Exit()


def _mostrar_aviso(mensagem: Warning | str, *_args: Any, **_kwargs: Any) -> None:
    typer.echo(f"Aviso: {mensagem}", err=True)


def _configure_cli_logging(verbose: bool, context: typer.Context) -> None:
    logger = logging.getLogger("agrobr")
    handlers, level, propagate = logger.handlers[:], logger.level, logger.propagate
    processors = _log.PROCESSADORES[:]
    mostrar_aviso = warnings.showwarning
    handler = logging.StreamHandler(sys.stderr)

    def restore() -> None:
        logger.handlers = handlers
        logger.setLevel(level)
        logger.propagate = propagate
        _log.PROCESSADORES[:] = processors
        warnings.showwarning = mostrar_aviso
        handler.close()

    context.call_on_close(restore)
    logger.handlers = [handler]
    logger.setLevel(logging.INFO if verbose else logging.WARNING)
    logger.propagate = False
    _log.PROCESSADORES[-1] = dev.ConsoleRenderer(colors=False)
    warnings.showwarning = _mostrar_aviso  # type: ignore[assignment]


@app.callback()  # type: ignore[misc, untyped-decorator]
def main(
    context: typer.Context,
    _version: bool = typer.Option(
        None,
        "--version",
        "-v",
        help="Mostra a versão e sai",
        callback=version_callback,
        is_eager=True,
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        help="Mostra o andamento (logs INFO) na saída de erro",
    ),
) -> None:
    for stream, codificacao in ((sys.stdout, "utf-8"), (sys.stderr, None)):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is not None:
            reconfigure(encoding=codificacao, errors="replace")

    _configure_cli_logging(verbose, context)


cepea_app = typer.Typer(help="Indicadores CEPEA")
app.add_typer(cepea_app, name="cepea")


@cepea_app.command(
    "indicador",
    help="Série diária do indicador CEPEA/ESALQ de um produto (ou o último, com --ultimo)",
)  # type: ignore[misc, untyped-decorator]
def cepea_indicador(
    produto: str = typer.Argument(..., help="Produto (soja, milho, cafe, boi, etc)"),
    inicio: str | None = typer.Option(
        None, "--inicio", "-i", help="Data de início (AAAA-MM-DD ou DD/MM/AAAA)"
    ),
    fim: str | None = typer.Option(
        None, "--fim", "-f", help="Data de fim (AAAA-MM-DD ou DD/MM/AAAA)"
    ),
    praca: str | None = typer.Option(None, "--praca", help="Praça do indicador"),
    ultimo: bool = typer.Option(
        False,
        "--ultimo",
        "-u",
        help="Só o último indicador publicado (não combina com --inicio/--fim)",
    ),
    formato: Formato = typer.Option(Formato.TABLE, "--formato", "-o", help="Formato de saída"),
) -> None:
    import asyncio

    from agrobr import cepea
    from agrobr.cepea import api as cepea_api

    typer.echo(f"Consultando {produto}...", err=True)

    try:
        if ultimo:
            if inicio or fim:
                raise ValueError(
                    "--ultimo não combina com --inicio/--fim: devolve o último indicador publicado"
                )
            recente = asyncio.run(cepea.ultimo(produto, praca=praca))
            df = cepea_api._to_dataframe([recente])
        else:
            df = asyncio.run(cepea.indicador(produto, inicio=inicio, fim=fim, praca=praca))

        _output_df(df, formato)

    except Exception as e:
        typer.echo(f"Erro: {e}", err=True)
        raise typer.Exit(1) from None


@app.command(
    "health", help="Testa a conexão e a resposta de cada fonte; sai com 1 se alguma falhar"
)  # type: ignore[misc, untyped-decorator]
def health(
    source: constants.Fonte | None = typer.Option(
        None,
        "--source",
        "-s",
        case_sensitive=False,
        help="Só esta fonte (nome do módulo: cepea, ibge, mapa_psr...)",
    ),
    deep: bool = typer.Option(
        False,
        "--deep",
        "-d",
        help="Também confere o layout da página e lê os dados (só muda o CEPEA)",
    ),
    formato: SaidaHealth = typer.Option(
        SaidaHealth.TEXT, "--formato", "-o", help="Formato de saída"
    ),
) -> None:
    import asyncio

    from agrobr.health.checker import format_results, run_all_checks
    from agrobr.health.reporter import HealthReport

    sources_list = [source] if source is not None else None

    try:
        results = asyncio.run(run_all_checks(sources_list, deep=deep))  # type: ignore[call-arg]
    except Exception as e:
        typer.echo(f"Erro ao executar health check: {e}", err=True)
        raise typer.Exit(1) from None

    if formato == SaidaHealth.JSON:
        report = HealthReport(results)
        typer.echo(report.to_json(indent=2))
    else:
        typer.echo(format_results(results))

    has_failed = any(r.status.value == "failed" for r in results)
    if has_failed:
        raise typer.Exit(1)


@app.command(
    "doctor", help="Diagnóstico do ambiente: estado das fontes, cache local e próxima atualização"
)  # type: ignore[misc, untyped-decorator]
def doctor(
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="Mostra a URL sondada de cada fonte e a última coleta no cache",
    ),
    formato: SaidaHealth = typer.Option(
        SaidaHealth.TEXT, "--formato", "-o", help="Formato de saída"
    ),
) -> None:
    import asyncio

    from agrobr.health.doctor import run_diagnostics

    try:
        result = asyncio.run(run_diagnostics(verbose=verbose))

        if formato == SaidaHealth.JSON:
            typer.echo(json.dumps(result.to_dict(), indent=2, ensure_ascii=False))
        else:
            typer.echo(result.to_rich())

    except Exception as e:
        typer.echo(f"Erro ao executar o diagnóstico: {e}", err=True)
        raise typer.Exit(1) from None

    if result.overall_status == "error" or any(s.status == "error" for s in result.sources):
        raise typer.Exit(1)


conab_app = typer.Typer(help="Dados da CONAB: safras e balanço")
app.add_typer(conab_app, name="conab")


@conab_app.command("safras", help="Área, produção e produtividade por UF numa safra da CONAB")  # type: ignore[misc, untyped-decorator]
def conab_safras(
    produto: str = typer.Argument(..., help="Produto (soja, milho, arroz, feijao, etc)"),
    safra: str | None = typer.Option(None, "--safra", "-s", help="Safra (ex: 2025/26)"),
    uf: str | None = typer.Option(None, "--uf", "-u", help="Filtrar por UF"),
    levantamento: int | None = typer.Option(
        None, "--levantamento", min=1, help="Levantamento da safra"
    ),
    formato: Formato = typer.Option(Formato.TABLE, "--formato", "-o", help="Formato de saída"),
) -> None:
    import asyncio

    from agrobr import conab

    typer.echo(f"Consultando safras de {produto}...", err=True)

    try:
        df = asyncio.run(
            conab.safras(produto=produto, safra=safra, uf=uf, levantamento=levantamento)
        )

        _output_df(df, formato)

    except Exception as e:
        typer.echo(f"Erro: {e}", err=True)
        raise typer.Exit(1) from None


@conab_app.command(
    "balanco", help="Balanço de oferta e demanda da CONAB (estoques, consumo e exportação)"
)  # type: ignore[misc, untyped-decorator]
def conab_balanco(
    produto: str | None = typer.Argument(None, help="Produto (opcional)"),
    safra: str | None = typer.Option(None, "--safra", "-s", help="Safra (ex: 2025/26)"),
    levantamento: int | None = typer.Option(
        None, "--levantamento", min=1, help="Levantamento da safra"
    ),
    formato: Formato = typer.Option(Formato.TABLE, "--formato", "-o", help="Formato de saída"),
) -> None:
    import asyncio

    from agrobr import conab

    typer.echo("Consultando o balanço de oferta e demanda...", err=True)

    try:
        df = asyncio.run(conab.balanco(produto=produto, safra=safra, levantamento=levantamento))

        _output_df(df, formato)

    except Exception as e:
        typer.echo(f"Erro: {e}", err=True)
        raise typer.Exit(1) from None


@conab_app.command("levantamentos", help="Lista os levantamentos de safra publicados pela CONAB")  # type: ignore[misc, untyped-decorator]
def conab_levantamentos(
    formato: Formato = typer.Option(Formato.TABLE, "--formato", "-o", help="Formato de saída"),
) -> None:
    import asyncio

    import pandas as pd

    from agrobr import conab
    from agrobr.conab.models import ConabLevantamento

    typer.echo("Listando levantamentos...", err=True)

    try:
        levs = asyncio.run(conab.levantamentos())

        _output_df(pd.DataFrame(levs, columns=list(ConabLevantamento.model_fields)), formato)

    except Exception as e:
        typer.echo(f"Erro: {e}", err=True)
        raise typer.Exit(1) from None


@conab_app.command("produtos", help="Lista os produtos aceitos pelos comandos da CONAB")  # type: ignore[misc, untyped-decorator]
def conab_produtos() -> None:
    import asyncio

    from agrobr import conab

    prods = asyncio.run(conab.produtos())
    typer.echo("Produtos disponíveis:")
    for prod in prods:
        typer.echo(f"  - {prod}")


ibge_app = typer.Typer(help="Dados do IBGE: PAM, LSPA e censos agropecuários")
app.add_typer(ibge_app, name="ibge")


@ibge_app.command(
    "pam",
    help="Produção Agrícola Municipal (PAM): área, produção e rendimento por ano (valor_producao sai vazio)",
)  # type: ignore[misc, untyped-decorator]
def ibge_pam(
    produto: str = typer.Argument(..., help="Produto (soja, milho, arroz, etc)"),
    ano: str | None = typer.Option(
        None, "--ano", "-a", help="Ano ou anos (ex: 2023 ou 2020,2021,2022)"
    ),
    uf: str | None = typer.Option(None, "--uf", "-u", help="Filtrar por UF"),
    nivel: str = typer.Option("uf", "--nivel", "-n", help="Nível: brasil, uf ou municipio"),
    formato: Formato = typer.Option(Formato.TABLE, "--formato", "-o", help="Formato de saída"),
) -> None:
    import asyncio

    from agrobr import ibge

    ano_param = _parse_anos(ano)
    typer.echo(f"Consultando PAM para {produto}...", err=True)

    try:
        nivel_typed: Any = nivel
        df = asyncio.run(ibge.pam(produto=produto, ano=ano_param, uf=uf, nivel=nivel_typed))

        _output_df(df, formato)

    except Exception as e:
        typer.echo(f"Erro: {e}", err=True)
        raise typer.Exit(1) from None


@ibge_app.command(
    "lspa", help="Levantamento Sistemático da Produção Agrícola (LSPA): estimativa mensal da safra"
)  # type: ignore[misc, untyped-decorator]
def ibge_lspa(
    produto: str = typer.Argument(..., help="Produto (soja, milho_1, milho_2, etc)"),
    ano: int | None = typer.Option(None, "--ano", "-a", help="Ano de referência"),
    mes: int | None = typer.Option(None, "--mes", "-m", help="Mês (1 a 12)"),
    uf: str | None = typer.Option(None, "--uf", "-u", help="Filtrar por UF"),
    formato: Formato = typer.Option(Formato.TABLE, "--formato", "-o", help="Formato de saída"),
) -> None:
    import asyncio

    from agrobr import ibge

    typer.echo(f"Consultando LSPA para {produto}...", err=True)

    try:
        df = asyncio.run(ibge.lspa(produto=produto, ano=ano, mes=mes, uf=uf))

        _output_df(df, formato)

    except Exception as e:
        typer.echo(f"Erro: {e}", err=True)
        raise typer.Exit(1) from None


@ibge_app.command(
    "censo-historico", help="Censos agropecuários de 1920 a 2006 por tema (os anos variam por tema)"
)  # type: ignore[misc, untyped-decorator]
def ibge_censo_historico(
    tema: str = typer.Argument(..., help="Tema (estabelecimentos_area, uso_terra, etc)"),
    ano: str | None = typer.Option(
        None, "--ano", "-a", help="Ano censitário ou anos (ex: 1985 ou 1970,1985,2006)"
    ),
    uf: str | None = typer.Option(None, "--uf", "-u", help="Filtrar por UF"),
    nivel: str = typer.Option("uf", "--nivel", "-n", help="Nível: brasil, regiao ou uf"),
    formato: Formato = typer.Option(Formato.TABLE, "--formato", "-o", help="Formato de saída"),
) -> None:
    import asyncio

    from agrobr import ibge

    ano_param = _parse_anos(ano)
    typer.echo(f"Consultando o censo histórico: {tema}...", err=True)

    try:
        nivel_typed: Any = nivel
        df = asyncio.run(
            ibge.censo_agro_historico(tema=tema, ano=ano_param, uf=uf, nivel=nivel_typed)
        )

        _output_df(df, formato)

    except Exception as e:
        typer.echo(f"Erro: {e}", err=True)
        raise typer.Exit(1) from None


@ibge_app.command("temas-historico", help="Lista os temas do censo agropecuário histórico")  # type: ignore[misc, untyped-decorator]
def ibge_temas_historico() -> None:
    import asyncio

    from agrobr import ibge

    temas = asyncio.run(ibge.temas_censo_agro_historico())
    typer.echo("Temas disponíveis no Censo Agropecuário Histórico:")
    for tema in temas:
        typer.echo(f"  - {tema}")


@ibge_app.command(
    "censo-municipal-1985",
    help="Censo Agropecuário de 1985 por município (volumes impressos do IBGE)",
)  # type: ignore[misc, untyped-decorator]
def ibge_censo_municipal_1985(
    tema: str = typer.Argument(..., help="Tema (propriedade_terras, condicao_produtor, etc)"),
    uf: str | None = typer.Option(None, "--uf", "-u", help="Filtrar por UF"),
    nivel: str | None = typer.Option(
        None, "--nivel", "-n", help="Nível: uf, mesorregiao, microrregiao ou municipio"
    ),
    formato: Formato = typer.Option(Formato.TABLE, "--formato", "-o", help="Formato de saída"),
) -> None:
    import asyncio

    from agrobr import ibge

    typer.echo(f"Consultando censo municipal 1985: {tema}...", err=True)

    try:
        df = asyncio.run(ibge.censo_agro_municipal_1985(tema, uf=uf, nivel=nivel))

        _output_df(df, formato)

    except Exception as e:
        typer.echo(f"Erro: {e}", err=True)
        raise typer.Exit(1) from None


@ibge_app.command(
    "temas-municipal-1985", help="Lista os temas do censo agropecuário municipal de 1985"
)  # type: ignore[misc, untyped-decorator]
def ibge_temas_municipal_1985() -> None:
    import asyncio

    from agrobr import ibge

    temas = asyncio.run(ibge.temas_censo_agro_municipal_1985())
    typer.echo("Temas do Censo Agropecuário Municipal de 1985 (tabelas 67 a 119):")
    for tema in temas:
        typer.echo(f"  - {tema}")


@ibge_app.command("produtos", help="Lista os produtos da PAM ou do LSPA")  # type: ignore[misc, untyped-decorator]
def ibge_produtos(
    pesquisa: Pesquisa = typer.Option(
        Pesquisa.PAM, "--pesquisa", "-p", case_sensitive=False, help="Pesquisa: pam ou lspa"
    ),
) -> None:
    import asyncio

    from agrobr import ibge

    if pesquisa == Pesquisa.PAM:
        prods = asyncio.run(ibge.produtos_pam())
        typer.echo("Produtos disponíveis na PAM:")
    else:
        prods = asyncio.run(ibge.produtos_lspa())
        typer.echo("Produtos disponíveis no LSPA:")

    for prod in prods:
        typer.echo(f"  - {prod}")


config_app = typer.Typer(help="Configurações")
app.add_typer(config_app, name="config")

snapshot_app = typer.Typer(
    help="Snapshots: cópias locais, em Parquet, de dados do CEPEA, da CONAB e do IBGE"
)
app.add_typer(snapshot_app, name="snapshot")


@config_app.command("show", help="Mostra a pasta do cache e as configurações de HTTP")  # type: ignore[misc, untyped-decorator]
def config_show() -> None:
    typer.echo("=== Cache ===")
    settings = constants.CacheSettings()
    typer.echo(f"  cache_dir: {settings.cache_dir}")
    typer.echo(f"  db_name: {settings.db_name}")

    typer.echo("\n=== HTTP ===")
    http = constants.HTTPSettings()
    typer.echo(f"  timeout_read: {http.timeout_read}s")
    typer.echo(f"  max_retries: {http.max_retries}")


@snapshot_app.command("list", help="Lista os snapshots salvos")  # type: ignore[misc, untyped-decorator]
def snapshot_list(
    formato: SaidaHealth = typer.Option(
        SaidaHealth.TEXT, "--formato", "-o", help="Formato de saída"
    ),
) -> None:
    from agrobr.snapshots import list_snapshots

    snapshots = list_snapshots()

    if not snapshots and formato == SaidaHealth.TEXT:
        typer.echo("Nenhum snapshot encontrado.")
        typer.echo("Use 'agrobr snapshot create' para criar um snapshot.")
        return

    if formato == SaidaHealth.JSON:
        data = [
            {
                "name": s.name,
                "created_at": s.created_at.isoformat(),
                "size_mb": round(s.size_bytes / 1024 / 1024, 2),
                "sources": s.sources,
                "files": s.file_count,
            }
            for s in snapshots
        ]
        typer.echo(json.dumps(data, indent=2, ensure_ascii=False))
    else:
        typer.echo("Snapshots disponíveis:")
        typer.echo("-" * 60)
        for s in snapshots:
            size_mb = s.size_bytes / 1024 / 1024
            typer.echo(f"  {s.name}")
            typer.echo(f"    Criado em: {s.created_at.strftime('%Y-%m-%d %H:%M %Z').rstrip()}")
            typer.echo(f"    Tamanho: {size_mb:.2f} MB")
            typer.echo(f"    Fontes: {', '.join(s.sources)}")
            typer.echo(f"    Arquivos: {s.file_count}")
            typer.echo()


@snapshot_app.command(
    "create",
    help="Cria um snapshot em Parquet com dados do CEPEA, da CONAB e do IBGE (precisa do pyarrow)",
)  # type: ignore[misc, untyped-decorator]
def snapshot_create(
    nome: str | None = typer.Argument(
        None, help="Nome do snapshot (padrão: a data de hoje, AAAA-MM-DD)"
    ),
    sources: str | None = typer.Option(
        None,
        "--sources",
        "-s",
        help="Fontes separadas por vírgula: cepea, conab e ibge (padrão: as 3)",
    ),
) -> None:
    import asyncio

    from agrobr.exceptions import SnapshotError
    from agrobr.snapshots import create_snapshot

    source_list = sources.split(",") if sources else None

    typer.echo(f"Criando snapshot{f' {nome}' if nome else ''}...", err=True)

    try:
        info = asyncio.run(create_snapshot(name=nome, sources=source_list))
        typer.echo(
            "Snapshot criado sem todas as fontes."
            if info.errors
            else "Snapshot criado com sucesso!"
        )
        for fonte, mensagens in info.errors.items():
            for mensagem in mensagens:
                typer.echo(f"Aviso: {fonte}: {mensagem}", err=True)
        typer.echo(f"  Nome: {info.name}")
        typer.echo(f"  Caminho: {info.path}")
        typer.echo(f"  Arquivos: {info.file_count}")
    except (ValueError, ImportError, SnapshotError) as e:
        typer.echo(f"Erro: {e}", err=True)
        raise typer.Exit(1) from None
    except Exception as e:
        typer.echo(f"Erro ao criar snapshot: {e}", err=True)
        raise typer.Exit(1) from None


@snapshot_app.command("delete", help="Remove um snapshot")  # type: ignore[misc, untyped-decorator]
def snapshot_delete(
    nome: str = typer.Argument(..., help="Nome do snapshot a remover"),
    force: bool = typer.Option(False, "--force", "-f", help="Não pedir confirmação"),
) -> None:
    from agrobr.snapshots import delete_snapshot, get_snapshot

    snapshot = get_snapshot(nome)
    if not snapshot:
        typer.echo(f"Erro: snapshot '{nome}' não encontrado.", err=True)
        raise typer.Exit(1)

    if not force:
        confirm = typer.confirm(f"Remover snapshot '{nome}'?")
        if not confirm:
            typer.echo("Operação cancelada.")
            return

    if delete_snapshot(nome):
        typer.echo(f"Snapshot '{nome}' removido com sucesso.")
    else:
        typer.echo("Erro ao remover snapshot.", err=True)
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
