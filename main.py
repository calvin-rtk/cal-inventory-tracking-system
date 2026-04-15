"""
Tool Missing / 7's Checker
--------------------------
CLI entry point.

Commands
--------
  check    <league>   Find missing games for a league (MLB/NHL/NBA/NFL)
  discover            Inspect login forms and page tables (run before first use)
  inventory           Show all events currently in the tools for a league

Usage examples
--------------
  python main.py discover
  python main.py check MLB
  python main.py check NHL --output-dir ./output
  python main.py inventory MLB
"""
#lets go boss

from __future__ import annotations

import logging
import sys
from datetime import date
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from src.auth.client import AuthenticatedClient
from src.comparison.checker import summarise_coverage
from src.config import load_config, load_credentials
from src.leagues import SUPPORTED_LEAGUES, detect_league
from src.report.csv_writer import write_inventory_report, write_missing_report
from src.scrapers.sg import SGScraper
from src.scrapers.te import TEScraper
from src.schedules.registry import get_schedule_provider

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

app = typer.Typer(
    name="checker",
    help="Tool Missing / 7's Checker — finds sports events absent from SG and TE tools.",
    add_completion=False,
)
console = Console()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_clients(cfg: dict, creds: dict) -> tuple[AuthenticatedClient, AuthenticatedClient]:
    sg_client = AuthenticatedClient(
        tool_name="SG",
        base_url=cfg["tools"]["sg"]["base_url"],
        login_url=cfg["tools"]["sg"]["login_url"],
        username=creds["sg"]["username"],
        password=creds["sg"]["password"],
        tool_config=cfg["tools"]["sg"],
    )
    te_client = AuthenticatedClient(
        tool_name="TE",
        base_url=cfg["tools"]["te"]["base_url"],
        login_url=cfg["tools"]["te"]["login_url"],
        username=creds["te"]["username"],
        password=creds["te"]["password"],
        tool_config=cfg["tools"]["te"],
    )
    return sg_client, te_client


def _login_both(sg_client: AuthenticatedClient, te_client: AuthenticatedClient) -> None:
    with console.status("[bold cyan]Logging in to SG…"):
        sg_client.login()
    console.print("[green]✔[/green]  SG login successful.")

    with console.status("[bold cyan]Logging in to TE…"):
        te_client.login()
    console.print("[green]✔[/green]  TE login successful.")


def _scrape_inventory(cfg: dict, sg_client: AuthenticatedClient, te_client: AuthenticatedClient):
    sg_scraper = SGScraper(sg_client, cfg["tools"]["sg"])
    te_scraper = TEScraper(te_client, cfg["tools"]["te"])

    with console.status("[bold cyan]Scraping SG events…"):
        sg_events = sg_scraper.fetch_events()
    console.print(f"[green]✔[/green]  SG: {len(sg_events)} future events found.")

    with console.status("[bold cyan]Scraping TE events…"):
        te_events = te_scraper.fetch_events()
    console.print(f"[green]✔[/green]  TE: {len(te_events)} future events found.")

    return sg_events + te_events


def _season_end(cfg: dict, league: str) -> date:
    raw = cfg.get("schedules", {}).get(league.lower(), {}).get("season_end")
    if raw:
        try:
            return date.fromisoformat(raw)
        except ValueError:
            pass
    return date(date.today().year, 12, 31)


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

@app.command()
def check(
    league: str = typer.Argument(
        ...,
        help=f"League to check: {', '.join(SUPPORTED_LEAGUES)}",
    ),
    output_dir: Optional[Path] = typer.Option(
        None,
        "--output-dir",
        "-o",
        help="Directory for CSV output (default: ./output)",
    ),
    inventory_csv: bool = typer.Option(
        False,
        "--inventory-csv",
        help="Also write a CSV of all matched inventory events.",
    ),
) -> None:
    """
    Check for missing games and write a CSV report.

    Compares official league schedule (today → season end) against the
    combined SG + TE inventory, then outputs missing games to CSV.
    """
    league_upper = league.upper()
    if league_upper not in SUPPORTED_LEAGUES:
        console.print(
            f"[red]Unknown league '{league}'.[/red] "
            f"Supported: {', '.join(SUPPORTED_LEAGUES)}"
        )
        raise typer.Exit(code=1)

    try:
        cfg = load_config()
        creds = load_credentials()
    except (FileNotFoundError, EnvironmentError) as exc:
        console.print(f"[red]Configuration error:[/red] {exc}")
        raise typer.Exit(code=1)

    console.print(Panel(f"[bold]Checking {league_upper}[/bold]", expand=False))

    # ---- Step 1: Login & scrape ----
    sg_client, te_client = _build_clients(cfg, creds)
    try:
        _login_both(sg_client, te_client)
    except RuntimeError as exc:
        console.print(f"[red]Login failed:[/red] {exc}")
        raise typer.Exit(code=1)

    all_events = _scrape_inventory(cfg, sg_client, te_client)

    # ---- Step 2: Filter inventory to selected league ----
    league_events = [e for e in all_events if e.league == league_upper]
    console.print(
        f"[cyan]Inventory events matched to {league_upper}:[/cyan] {len(league_events)}"
    )

    # ---- Step 3: Fetch official schedule ----
    today = date.today()
    season_end = _season_end(cfg, league_upper)

    with console.status(f"[bold cyan]Fetching {league_upper} schedule ({today} → {season_end})…"):
        try:
            provider = get_schedule_provider(league_upper)
            schedule = provider.get_games(today, season_end)
        except Exception as exc:
            console.print(f"[red]Schedule fetch failed:[/red] {exc}")
            raise typer.Exit(code=1)

    console.print(f"[cyan]Official {league_upper} games remaining:[/cyan] {len(schedule)}")

    # ---- Step 4: Compare ----
    min_word_len = cfg.get("matching", {}).get("min_word_len", 4)
    summary = summarise_coverage(league_events, schedule, league_upper)

    # ---- Step 5: Terminal summary ----
    _print_summary(summary)

    # ---- Step 6: Write CSV(s) ----
    out_dir = output_dir or Path("output")
    missing_path = write_missing_report(summary["missing_games"], league_upper, out_dir)
    console.print(f"\n[bold green]Missing report:[/bold green] {missing_path}")

    if inventory_csv:
        inv_path = write_inventory_report(league_events, league_upper, out_dir)
        console.print(f"[bold green]Inventory report:[/bold green] {inv_path}")


@app.command()
def discover() -> None:
    """
    Inspect login forms and page table structure without scraping data.

    Run this once after setting up .env to confirm or update the
    selector settings in config.yaml.
    """
    try:
        cfg = load_config()
        creds = load_credentials()
    except (FileNotFoundError, EnvironmentError) as exc:
        console.print(f"[red]Configuration error:[/red] {exc}")
        raise typer.Exit(code=1)

    sg_client, te_client = _build_clients(cfg, creds)

    for name, client, tool_key in [
        ("SG", sg_client, "sg"),
        ("TE", te_client, "te"),
    ]:
        console.rule(f"[bold]{name} Tool[/bold]")

        # Login form fields
        console.print(f"\n[bold cyan]Login form:[/bold cyan] {client.login_url}")
        form_info = client.discover()
        _print_form_info(form_info)

        # Page tables (requires login first)
        console.print(f"\n[bold cyan]Page tables:[/bold cyan] {cfg['tools'][tool_key]['events_url']}")
        try:
            client.login()
        except RuntimeError as exc:
            console.print(f"[red]Could not log in:[/red] {exc}")
            continue

        if tool_key == "sg":
            from src.scrapers.sg import SGScraper
            scraper = SGScraper(client, cfg["tools"][tool_key])
        else:
            from src.scrapers.te import TEScraper
            scraper = TEScraper(client, cfg["tools"][tool_key])

        tables = scraper.discover_tables(cfg["tools"][tool_key]["events_url"])
        if not tables:
            console.print("[yellow]No tables found on the page.[/yellow]")
        for tbl in tables:
            _print_table_info(tbl)


@app.command()
def inventory(
    league: str = typer.Argument(
        ...,
        help=f"League to list: {', '.join(SUPPORTED_LEAGUES)}",
    ),
    output_dir: Optional[Path] = typer.Option(
        None, "--output-dir", "-o", help="Directory for CSV output."
    ),
) -> None:
    """
    List all events currently in the tools for a league and write to CSV.

    Useful for auditing what IS in the tools before running a full check.
    """
    league_upper = league.upper()
    if league_upper not in SUPPORTED_LEAGUES:
        console.print(
            f"[red]Unknown league '{league}'.[/red] "
            f"Supported: {', '.join(SUPPORTED_LEAGUES)}"
        )
        raise typer.Exit(code=1)

    try:
        cfg = load_config()
        creds = load_credentials()
    except (FileNotFoundError, EnvironmentError) as exc:
        console.print(f"[red]Configuration error:[/red] {exc}")
        raise typer.Exit(code=1)

    sg_client, te_client = _build_clients(cfg, creds)
    try:
        _login_both(sg_client, te_client)
    except RuntimeError as exc:
        console.print(f"[red]Login failed:[/red] {exc}")
        raise typer.Exit(code=1)

    all_events = _scrape_inventory(cfg, sg_client, te_client)
    league_events = [e for e in all_events if e.league == league_upper]
    console.print(
        f"\n[bold]{league_upper} events in tools:[/bold] {len(league_events)}"
    )

    out_dir = output_dir or Path("output")
    path = write_inventory_report(league_events, league_upper, out_dir)
    console.print(f"[bold green]Inventory report:[/bold green] {path}")


# ---------------------------------------------------------------------------
# Rich display helpers
# ---------------------------------------------------------------------------

def _print_summary(summary: dict) -> None:
    league = summary["league"]
    tbl = Table(title=f"{league} Coverage Summary", show_header=True, header_style="bold cyan")
    tbl.add_column("Metric", style="dim")
    tbl.add_column("Count", justify="right")
    tbl.add_row("Official games (today → season end)", str(summary["scheduled"]))
    tbl.add_row("[green]Covered in tools[/green]", str(summary["covered"]))
    tbl.add_row("[red]Missing from tools[/red]", str(summary["missing"]))
    console.print(tbl)

    if summary["missing_games"]:
        console.print(f"\n[bold red]Missing {league} games:[/bold red]")
        missing_tbl = Table(show_header=True, header_style="bold")
        missing_tbl.add_column("Date")
        missing_tbl.add_column("Away")
        missing_tbl.add_column("Home")
        missing_tbl.add_column("Venue")
        for game in sorted(summary["missing_games"], key=lambda g: g.game_date):
            missing_tbl.add_row(
                str(game.game_date), game.away_team, game.home_team, game.venue
            )
        console.print(missing_tbl)


def _print_form_info(form_info: dict) -> None:
    if "error" in form_info:
        console.print(f"[red]{form_info['error']}[/red]")
        return
    console.print(f"  Action : {form_info.get('form_action')}")
    console.print(f"  Method : {form_info.get('form_method')}")
    for name, attrs in form_info.get("fields", {}).items():
        console.print(f"  Field  : [bold]{name}[/bold]  (type={attrs['type']})")


def _print_table_info(tbl: dict) -> None:
    console.print(
        f"\n  [bold]Table #{tbl['table_index']}[/bold] — "
        f"{tbl['row_count']} data rows"
    )
    if tbl["headers"]:
        console.print(f"  Headers : {tbl['headers']}")
    else:
        console.print("  Headers : [yellow](none detected)[/yellow]")
    for i, row in enumerate(tbl["sample_rows"]):
        console.print(f"  Row {i+1}   : {row}")


# ---------------------------------------------------------------------------

if __name__ == "__main__":
    app()
