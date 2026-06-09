from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, cast

import typer

from agrobr import __version__, constants

if TYPE_CHECKING:
    import pandas as pd


def _output_df(df: Any, formato: str) -> None:
    if formato == "json":
        typer.echo(df.to_json(orient="records", indent=2))
    elif formato == "csv":
        typer.echo(df.to_csv(index=False))
    else:
        typer.echo(df.to_string(index=False))


app = typer.Typer(
    name="agrobr",
    help="Dados agricolas brasileiros em uma linha de codigo",
    add_completion=False,
)


def version_callback(value: bool) -> None:
    if value:
        typer.echo(f"agrobr version {__version__}")
        raise typer.Exit()


@app.callback()  # type: ignore[misc, untyped-decorator]
def main(
    _version: bool = typer.Option(
        None,
        "--version",
        "-v",
        help="Mostra a versao e sai",
        callback=version_callback,
        is_eager=True,
    ),
) -> None:
    pass


cepea_app = typer.Typer(help="Indicadores CEPEA")
app.add_typer(cepea_app, name="cepea")


@cepea_app.command("indicador")  # type: ignore[misc, untyped-decorator]
def cepea_indicador(
    produto: str = typer.Argument(..., help="Produto (soja, milho, cafe, boi, etc)"),
    inicio: str | None = typer.Option(None, "--inicio", "-i", help="Data inicio (YYYY-MM-DD)"),
    fim: str | None = typer.Option(None, "--fim", "-f", help="Data fim (YYYY-MM-DD)"),
    ultimo: bool = typer.Option(False, "--ultimo", "-u", help="Apenas ultimo valor"),
    formato: str = typer.Option("table", "--formato", "-o", help="Formato: table, csv, json"),
) -> None:
    import asyncio

    from agrobr import cepea

    typer.echo(f"Consultando {produto}...")

    try:
        df = cast("pd.DataFrame", asyncio.run(cepea.indicador(produto, inicio=inicio, fim=fim)))

        if df.empty:
            typer.echo("Nenhum dado encontrado")
            return

        if ultimo:
            df = df.tail(1)

        _output_df(df, formato)

    except Exception as e:
        typer.echo(f"Erro: {e}", err=True)
        raise typer.Exit(1) from None


@app.command("health")  # type: ignore[misc, untyped-decorator]
def health(
    source: str | None = typer.Option(None, "--source", "-s", help="Fonte especifica"),
    deep: bool = typer.Option(False, "--deep", "-d", help="Deep checks (fingerprint+parse)"),
    output: str = typer.Option("text", "--output", "-o", help="Formato: text, json"),
) -> None:
    import asyncio

    from agrobr.health.checker import format_results, run_all_checks
    from agrobr.health.reporter import HealthReport

    sources_list = None
    if source:
        try:
            fonte = constants.Fonte(source.lower())
        except ValueError:
            typer.echo(f"Fonte desconhecida: {source}", err=True)
            raise typer.Exit(1) from None
        sources_list = [fonte]

    try:
        results = asyncio.run(run_all_checks(sources_list, deep=deep))  # type: ignore[call-arg]
    except Exception as e:
        typer.echo(f"Erro ao executar health check: {e}", err=True)
        raise typer.Exit(1) from None

    if output == "json":
        report = HealthReport(results)
        typer.echo(report.to_json(indent=2))
    else:
        typer.echo(format_results(results))

    has_failed = any(r.status.value == "failed" for r in results)
    if has_failed:
        raise typer.Exit(1)


@app.command("doctor")  # type: ignore[misc, untyped-decorator]
def doctor(
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Mostra informacoes detalhadas"),
    json_output: bool = typer.Option(False, "--json", help="Output em formato JSON"),
) -> None:
    import asyncio

    from agrobr.health.doctor import run_diagnostics

    try:
        result = asyncio.run(run_diagnostics(verbose=verbose))

        if json_output:
            typer.echo(json.dumps(result.to_dict(), indent=2, ensure_ascii=False))
        else:
            typer.echo(result.to_rich())

    except Exception as e:
        typer.echo(f"Erro ao executar diagnostico: {e}", err=True)
        raise typer.Exit(1) from None


conab_app = typer.Typer(help="Dados CONAB - Safras e balanco")
app.add_typer(conab_app, name="conab")


@conab_app.command("safras")  # type: ignore[misc, untyped-decorator]
def conab_safras(
    produto: str = typer.Argument(..., help="Produto (soja, milho, arroz, feijao, etc)"),
    safra: str | None = typer.Option(None, "--safra", "-s", help="Safra (ex: 2025/26)"),
    uf: str | None = typer.Option(None, "--uf", "-u", help="Filtrar por UF"),
    formato: str = typer.Option("table", "--formato", "-o", help="Formato: table, csv, json"),
) -> None:
    import asyncio

    from agrobr import conab

    typer.echo(f"Consultando safras de {produto}...")

    try:
        df = asyncio.run(conab.safras(produto=produto, safra=safra, uf=uf))

        if df.empty:
            typer.echo("Nenhum dado encontrado")
            return

        _output_df(df, formato)

    except Exception as e:
        typer.echo(f"Erro: {e}", err=True)
        raise typer.Exit(1) from None


@conab_app.command("balanco")  # type: ignore[misc, untyped-decorator]
def conab_balanco(
    produto: str | None = typer.Argument(None, help="Produto (opcional)"),
    formato: str = typer.Option("table", "--formato", "-o", help="Formato: table, csv, json"),
) -> None:
    import asyncio

    from agrobr import conab

    typer.echo("Consultando balanco oferta/demanda...")

    try:
        df = asyncio.run(conab.balanco(produto=produto))

        if df.empty:
            typer.echo("Nenhum dado encontrado")
            return

        _output_df(df, formato)

    except Exception as e:
        typer.echo(f"Erro: {e}", err=True)
        raise typer.Exit(1) from None


@conab_app.command("levantamentos")  # type: ignore[misc, untyped-decorator]
def conab_levantamentos() -> None:
    import asyncio

    from agrobr import conab

    typer.echo("Listando levantamentos...")

    try:
        levs = asyncio.run(conab.levantamentos())

        for lev in levs[:10]:
            typer.echo(f"  {lev['safra']} - {lev['levantamento']}o levantamento")

        if len(levs) > 10:
            typer.echo(f"  ... e mais {len(levs) - 10} levantamentos")

    except Exception as e:
        typer.echo(f"Erro: {e}", err=True)
        raise typer.Exit(1) from None


@conab_app.command("produtos")  # type: ignore[misc, untyped-decorator]
def conab_produtos() -> None:
    import asyncio

    from agrobr import conab

    prods = asyncio.run(conab.produtos())
    typer.echo("Produtos disponiveis:")
    for prod in prods:
        typer.echo(f"  - {prod}")


ibge_app = typer.Typer(help="Dados IBGE - PAM e LSPA")
app.add_typer(ibge_app, name="ibge")


@ibge_app.command("pam")  # type: ignore[misc, untyped-decorator]
def ibge_pam(
    produto: str = typer.Argument(..., help="Produto (soja, milho, arroz, etc)"),
    ano: str | None = typer.Option(
        None, "--ano", "-a", help="Ano ou anos (ex: 2023 ou 2020,2021,2022)"
    ),
    uf: str | None = typer.Option(None, "--uf", "-u", help="Filtrar por UF"),
    nivel: str = typer.Option("uf", "--nivel", "-n", help="Nivel: brasil, uf, municipio"),
    formato: str = typer.Option("table", "--formato", "-o", help="Formato: table, csv, json"),
) -> None:
    import asyncio

    from agrobr import ibge

    typer.echo(f"Consultando PAM para {produto}...")

    try:
        ano_param: int | list[int] | None = None
        if ano:
            ano_param = [int(a.strip()) for a in ano.split(",")] if "," in ano else int(ano)

        nivel_typed: Any = nivel
        df = asyncio.run(ibge.pam(produto=produto, ano=ano_param, uf=uf, nivel=nivel_typed))

        if df.empty:
            typer.echo("Nenhum dado encontrado")
            return

        _output_df(df, formato)

    except Exception as e:
        typer.echo(f"Erro: {e}", err=True)
        raise typer.Exit(1) from None


@ibge_app.command("lspa")  # type: ignore[misc, untyped-decorator]
def ibge_lspa(
    produto: str = typer.Argument(..., help="Produto (soja, milho_1, milho_2, etc)"),
    ano: int | None = typer.Option(None, "--ano", "-a", help="Ano de referencia"),
    mes: int | None = typer.Option(None, "--mes", "-m", help="Mes (1-12)"),
    uf: str | None = typer.Option(None, "--uf", "-u", help="Filtrar por UF"),
    formato: str = typer.Option("table", "--formato", "-o", help="Formato: table, csv, json"),
) -> None:
    import asyncio

    from agrobr import ibge

    typer.echo(f"Consultando LSPA para {produto}...")

    try:
        df = asyncio.run(ibge.lspa(produto=produto, ano=ano, mes=mes, uf=uf))

        if df.empty:
            typer.echo("Nenhum dado encontrado")
            return

        _output_df(df, formato)

    except Exception as e:
        typer.echo(f"Erro: {e}", err=True)
        raise typer.Exit(1) from None


@ibge_app.command("censo-historico")  # type: ignore[misc, untyped-decorator]
def ibge_censo_historico(
    tema: str = typer.Argument(..., help="Tema (estabelecimentos_area, uso_terra, etc)"),
    ano: str | None = typer.Option(
        None, "--ano", "-a", help="Ano censitario ou anos (ex: 1985 ou 1970,1985,2006)"
    ),
    uf: str | None = typer.Option(None, "--uf", "-u", help="Filtrar por UF"),
    nivel: str = typer.Option("uf", "--nivel", "-n", help="Nivel: brasil, regiao, uf"),
    formato: str = typer.Option("table", "--formato", "-o", help="Formato: table, csv, json"),
) -> None:
    import asyncio

    from agrobr import ibge

    typer.echo(f"Consultando censo historico: {tema}...")

    try:
        ano_param: int | list[int] | None = None
        if ano:
            ano_param = [int(a.strip()) for a in ano.split(",")] if "," in ano else int(ano)

        nivel_typed: Any = nivel
        df = asyncio.run(
            ibge.censo_agro_historico(tema=tema, ano=ano_param, uf=uf, nivel=nivel_typed)
        )

        if df.empty:
            typer.echo("Nenhum dado encontrado")
            return

        _output_df(df, formato)

    except Exception as e:
        typer.echo(f"Erro: {e}", err=True)
        raise typer.Exit(1) from None


@ibge_app.command("temas-historico")  # type: ignore[misc, untyped-decorator]
def ibge_temas_historico() -> None:
    import asyncio

    from agrobr import ibge

    temas = asyncio.run(ibge.temas_censo_agro_historico())
    typer.echo("Temas disponiveis no Censo Agropecuario Historico:")
    for tema in temas:
        typer.echo(f"  - {tema}")


@ibge_app.command("censo-municipal-1985")  # type: ignore[misc, untyped-decorator]
def ibge_censo_municipal_1985(
    tema: str = typer.Argument(..., help="Tema (propriedade_terras, condicao_produtor, etc)"),
    uf: str | None = typer.Option(None, "--uf", "-u", help="Filtrar por UF"),
    nivel: str | None = typer.Option(
        None, "--nivel", "-n", help="Nivel: total, mesorregiao, microrregiao, municipio"
    ),
    formato: str = typer.Option("table", "--formato", "-o", help="Formato: table, csv, json"),
) -> None:
    import asyncio

    from agrobr import ibge

    typer.echo(f"Consultando censo municipal 1985: {tema}...")

    try:
        df = asyncio.run(ibge.censo_agro_municipal_1985(tema, uf=uf, nivel=nivel))

        if df.empty:
            typer.echo("Nenhum dado encontrado")
            return

        _output_df(df, formato)

    except Exception as e:
        typer.echo(f"Erro: {e}", err=True)
        raise typer.Exit(1) from None


@ibge_app.command("temas-municipal-1985")  # type: ignore[misc, untyped-decorator]
def ibge_temas_municipal_1985() -> None:
    import asyncio

    from agrobr import ibge

    temas = asyncio.run(ibge.temas_censo_agro_municipal_1985())
    typer.echo("Temas disponiveis no Censo Agropecuario Municipal 1985:")
    for tema in temas:
        typer.echo(f"  - {tema}")


@ibge_app.command("produtos")  # type: ignore[misc, untyped-decorator]
def ibge_produtos(
    pesquisa: str = typer.Option("pam", "--pesquisa", "-p", help="Pesquisa: pam ou lspa"),
) -> None:
    import asyncio

    from agrobr import ibge

    if pesquisa == "pam":
        prods = asyncio.run(ibge.produtos_pam())
        typer.echo("Produtos disponiveis na PAM:")
    else:
        prods = asyncio.run(ibge.produtos_lspa())
        typer.echo("Produtos disponiveis no LSPA:")

    for prod in prods:
        typer.echo(f"  - {prod}")


config_app = typer.Typer(help="Configuracoes")
app.add_typer(config_app, name="config")

snapshot_app = typer.Typer(help="Gerenciamento de snapshots para modo deterministico")
app.add_typer(snapshot_app, name="snapshot")


@config_app.command("show")  # type: ignore[misc, untyped-decorator]
def config_show() -> None:
    typer.echo("=== Cache Settings ===")
    settings = constants.CacheSettings()
    typer.echo(f"  cache_dir: {settings.cache_dir}")
    typer.echo(f"  db_name: {settings.db_name}")

    typer.echo("\n=== HTTP Settings ===")
    http = constants.HTTPSettings()
    typer.echo(f"  timeout_read: {http.timeout_read}s")
    typer.echo(f"  max_retries: {http.max_retries}")


@snapshot_app.command("list")  # type: ignore[misc, untyped-decorator]
def snapshot_list(
    json_output: bool = typer.Option(False, "--json", help="Output em formato JSON"),
) -> None:
    from agrobr.snapshots import list_snapshots

    snapshots = list_snapshots()

    if not snapshots:
        typer.echo("Nenhum snapshot encontrado.")
        typer.echo("Use 'agrobr snapshot create' para criar um snapshot.")
        return

    if json_output:
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
        typer.echo(json.dumps(data, indent=2))
    else:
        typer.echo("Snapshots disponiveis:")
        typer.echo("-" * 60)
        for s in snapshots:
            size_mb = s.size_bytes / 1024 / 1024
            typer.echo(f"  {s.name}")
            typer.echo(f"    Criado em: {s.created_at.strftime('%Y-%m-%d %H:%M')}")
            typer.echo(f"    Tamanho: {size_mb:.2f} MB")
            typer.echo(f"    Fontes: {', '.join(s.sources)}")
            typer.echo(f"    Arquivos: {s.file_count}")
            typer.echo()


@snapshot_app.command("create")  # type: ignore[misc, untyped-decorator]
def snapshot_create(
    name: str | None = typer.Argument(None, help="Nome do snapshot (default: data atual)"),
    sources: str | None = typer.Option(
        None, "--sources", "-s", help="Fontes a incluir (ex: cepea,conab,ibge)"
    ),
) -> None:
    import asyncio

    from agrobr.snapshots import create_snapshot

    source_list = sources.split(",") if sources else None

    typer.echo(f"Criando snapshot{f' {name}' if name else ''}...")

    try:
        info = asyncio.run(create_snapshot(name=name, sources=source_list))
        typer.echo("Snapshot criado com sucesso!")
        typer.echo(f"  Nome: {info.name}")
        typer.echo(f"  Caminho: {info.path}")
        typer.echo(f"  Arquivos: {info.file_count}")
    except ValueError as e:
        typer.echo(f"Erro: {e}", err=True)
        raise typer.Exit(1) from None
    except Exception as e:
        typer.echo(f"Erro ao criar snapshot: {e}", err=True)
        raise typer.Exit(1) from None


@snapshot_app.command("delete")  # type: ignore[misc, untyped-decorator]
def snapshot_delete(
    name: str = typer.Argument(..., help="Nome do snapshot a remover"),
    force: bool = typer.Option(False, "--force", "-f", help="Nao pedir confirmacao"),
) -> None:
    from agrobr.snapshots import delete_snapshot, get_snapshot

    snapshot = get_snapshot(name)
    if not snapshot:
        typer.echo(f"Snapshot '{name}' nao encontrado.", err=True)
        raise typer.Exit(1)

    if not force:
        confirm = typer.confirm(f"Remover snapshot '{name}'?")
        if not confirm:
            typer.echo("Operacao cancelada.")
            return

    if delete_snapshot(name):
        typer.echo(f"Snapshot '{name}' removido com sucesso.")
    else:
        typer.echo("Erro ao remover snapshot.", err=True)
        raise typer.Exit(1)


@snapshot_app.command("use")  # type: ignore[misc, untyped-decorator]
def snapshot_use(
    name: str = typer.Argument(..., help="Nome do snapshot a usar"),
) -> None:
    from agrobr.config import set_mode
    from agrobr.snapshots import get_snapshot

    snapshot = get_snapshot(name)
    if not snapshot:
        typer.echo(f"Snapshot '{name}' nao encontrado.", err=True)
        typer.echo("Use 'agrobr snapshot list' para ver snapshots disponiveis.")
        raise typer.Exit(1)

    set_mode("deterministic", snapshot=name)
    typer.echo(f"Modo deterministico ativado com snapshot '{name}'.")
    typer.echo("Todas as chamadas usarao dados do snapshot.")
    typer.echo("Use 'agrobr config mode normal' para voltar ao modo normal.")


if __name__ == "__main__":
    app()
