"""Command-line interface for DataHub."""

from datetime import datetime, timedelta, timezone
from pathlib import Path

import click
from rich.console import Console
from rich.table import Table
from sqlalchemy import func, select

from datahub.config import Config, DEFAULT_CONFIG_DIR
from datahub.db import init_db, get_session, DataPoint, Transaction, SyncLog
from datahub.dedup import deduplicate_daily_totals, get_deduplicated_total, get_daily_average

console = Console()


@click.group()
@click.pass_context
def cli(ctx):
    """DataHub - Your personal data aggregator."""
    ctx.ensure_object(dict)
    ctx.obj["config"] = Config()


@cli.command()
@click.pass_context
def init(ctx):
    """Initialize DataHub database and config."""
    config = ctx.obj["config"]
    db_path = config.get_db_path()

    console.print(f"[blue]Initializing DataHub...[/blue]")
    console.print(f"  Config directory: {config.config_dir}")
    console.print(f"  Database: {db_path}")

    init_db(db_path)
    console.print("[green]DataHub initialized successfully![/green]")


@cli.command()
@click.argument("key")
@click.argument("value")
@click.pass_context
def config(ctx, key: str, value: str):
    """Set a configuration value."""
    cfg = ctx.obj["config"]
    cfg.set(key, value)
    console.print(f"[green]Set {key} = {value}[/green]")


@cli.group()
def import_data():
    """Import data from files."""
    pass


@import_data.command("apple-health")
@click.argument("file_path", type=click.Path(exists=True, path_type=Path))
@click.pass_context
def import_apple_health(ctx, file_path: Path):
    """Import Apple Health XML export."""
    from datahub.connectors.fitness.apple_health import AppleHealthConnector

    config = ctx.obj["config"]
    db_path = config.get_db_path()

    if not db_path.exists():
        console.print("[red]Database not initialized. Run 'datahub init' first.[/red]")
        return

    console.print(f"[blue]Importing Apple Health data from {file_path}...[/blue]")
    console.print("[dim]This may take a while for large exports.[/dim]")

    session = get_session(db_path)
    connector = AppleHealthConnector(session)

    try:
        log = connector.run_import(file_path)
        console.print(f"[green]Import complete![/green]")
        console.print(f"  Records added: {log.records_added}")
        console.print(f"  Records skipped (duplicates): {log.records_updated}")
    except Exception as e:
        console.print(f"[red]Import failed: {e}[/red]")
    finally:
        session.close()


@import_data.command("bank-csv")
@click.argument("file_path", type=click.Path(exists=True, path_type=Path))
@click.option("--format", "bank_format", default="chase",
              type=click.Choice(["chase", "bofa", "apple_card", "amex", "generic"]),
              help="Bank export format")
@click.option("--account", default=None, help="Account name for labeling")
@click.option("--date-col", default=None, help="Date column name (for generic format)")
@click.option("--amount-col", default=None, help="Amount column name (for generic format)")
@click.option("--desc-col", default=None, help="Description column name (for generic format)")
@click.pass_context
def import_bank_csv(ctx, file_path: Path, bank_format: str, account: str,
                    date_col: str, amount_col: str, desc_col: str):
    """Import bank transactions from CSV export."""
    from datahub.connectors.finance.csv_import import CSVBankConnector

    config = ctx.obj["config"]
    db_path = config.get_db_path()

    if not db_path.exists():
        console.print("[red]Database not initialized. Run 'datahub init' first.[/red]")
        return

    # Build custom columns for generic format
    custom_columns = None
    if bank_format == "generic":
        if not all([date_col, amount_col, desc_col]):
            console.print("[red]Generic format requires --date-col, --amount-col, and --desc-col[/red]")
            return
        custom_columns = {
            "date": date_col,
            "amount": amount_col,
            "description": desc_col,
        }

    console.print(f"[blue]Importing transactions from {file_path}...[/blue]")

    session = get_session(db_path)
    connector = CSVBankConnector(
        session,
        bank_format=bank_format,
        custom_columns=custom_columns,
        account_name=account,
    )

    try:
        log = connector.run_import(file_path)
        console.print(f"[green]Import complete![/green]")
        console.print(f"  Transactions added: {log.records_added}")
        console.print(f"  Transactions skipped (duplicates): {log.records_updated}")
    except Exception as e:
        console.print(f"[red]Import failed: {e}[/red]")
    finally:
        session.close()


@cli.group()
def sync():
    """Sync data from APIs."""
    pass


@sync.command("peloton")
@click.option("--days", default=None, type=int, help="Only sync workouts from last N days")
@click.pass_context
def sync_peloton(ctx, days: int | None):
    """Sync workouts from Peloton."""
    from datahub.connectors.fitness.peloton import PelotonConnector

    config = ctx.obj["config"]
    db_path = config.get_db_path()

    if not db_path.exists():
        console.print("[red]Database not initialized. Run 'datahub init' first.[/red]")
        return

    # Get Peloton credentials from config
    peloton_config = {
        "username": config.get("peloton.username"),
        "password": config.get("peloton.password"),
    }

    if not peloton_config["username"] or not peloton_config["password"]:
        console.print("[yellow]Peloton credentials not configured. Run:[/yellow]")
        console.print("  datahub config peloton.username your_email")
        console.print("  datahub config peloton.password your_password")
        return

    console.print("[blue]Syncing Peloton workouts...[/blue]")

    session = get_session(db_path)
    connector = PelotonConnector(session, config=peloton_config)

    since = None
    if days:
        since = datetime.now(timezone.utc) - timedelta(days=days)

    try:
        log = connector.run_sync(since)
        console.print(f"[green]Sync complete![/green]")
        console.print(f"  Records added: {log.records_added}")
        console.print(f"  Workouts skipped (already imported): {log.records_updated}")
    except Exception as e:
        console.print(f"[red]Sync failed: {e}[/red]")
    finally:
        connector.close()
        session.close()


@sync.command("oura")
@click.option("--days", default=30, type=int, help="Days of history to sync")
@click.pass_context
def sync_oura(ctx, days: int):
    """Sync data from Oura Ring."""
    from datahub.connectors.fitness.oura import OuraConnector

    config = ctx.obj["config"]
    db_path = config.get_db_path()

    if not db_path.exists():
        console.print("[red]Database not initialized. Run 'datahub init' first.[/red]")
        return

    oura_config = {"token": config.get("oura.token")}

    if not oura_config["token"]:
        console.print("[yellow]Oura token not configured. Run:[/yellow]")
        console.print("  datahub config oura.token YOUR_TOKEN")
        console.print("\n[dim]Get your token at: https://cloud.ouraring.com/personal-access-tokens[/dim]")
        return

    console.print(f"[blue]Syncing Oura Ring data (last {days} days)...[/blue]")

    session = get_session(db_path)
    connector = OuraConnector(session, config=oura_config)

    since = datetime.now(timezone.utc) - timedelta(days=days)

    try:
        log = connector.run_sync(since)
        console.print(f"[green]Sync complete![/green]")
        console.print(f"  Records added: {log.records_added}")
    except Exception as e:
        console.print(f"[red]Sync failed: {e}[/red]")
    finally:
        connector.close()
        session.close()


@sync.command("tonal")
@click.option("--days", default=None, type=int, help="Only sync workouts from last N days")
@click.pass_context
def sync_tonal(ctx, days: int | None):
    """Sync strength workouts from Tonal."""
    from datahub.connectors.fitness.tonal import TonalConnector

    config = ctx.obj["config"]
    db_path = config.get_db_path()

    if not db_path.exists():
        console.print("[red]Database not initialized. Run 'datahub init' first.[/red]")
        return

    tonal_config = {
        "email": config.get("tonal.email"),
        "password": config.get("tonal.password"),
    }

    if not tonal_config["email"] or not tonal_config["password"]:
        console.print("[yellow]Tonal credentials not configured. Run:[/yellow]")
        console.print("  datahub config tonal.email your_email")
        console.print("  datahub config tonal.password your_password")
        return

    console.print("[blue]Syncing Tonal workouts...[/blue]")

    session = get_session(db_path)
    connector = TonalConnector(session, config=tonal_config)

    since = None
    if days:
        since = datetime.now(timezone.utc) - timedelta(days=days)

    try:
        log = connector.run_sync(since)
        console.print(f"[green]Sync complete![/green]")
        console.print(f"  Records added: {log.records_added}")
        console.print(f"  Workouts skipped (already imported): {log.records_updated}")
    except Exception as e:
        console.print(f"[red]Sync failed: {e}[/red]")
    finally:
        connector.close()
        session.close()


@sync.command("simplefin")
@click.option("--days", default=30, type=int, help="Days of history to sync")
@click.option("--setup", "setup_token", default=None, help="Setup token to claim")
@click.pass_context
def sync_simplefin(ctx, days: int, setup_token: str | None):
    """Sync transactions from SimpleFIN Bridge."""
    from datahub.connectors.finance.simplefin import SimpleFINConnector

    config = ctx.obj["config"]
    db_path = config.get_db_path()

    if not db_path.exists():
        console.print("[red]Database not initialized. Run 'datahub init' first.[/red]")
        return

    # Handle setup token claiming
    if setup_token:
        console.print("[blue]Claiming SimpleFIN setup token...[/blue]")
        session = get_session(db_path)
        connector = SimpleFINConnector(session, config={})

        try:
            access_url = connector.claim_setup_token(setup_token)
            config.set("simplefin.access_url", access_url)
            console.print("[green]SimpleFIN configured successfully![/green]")
            console.print("[dim]Access URL saved to config. You can now run 'datahub sync simplefin'[/dim]")
        except Exception as e:
            console.print(f"[red]Failed to claim setup token: {e}[/red]")
        finally:
            connector.close()
            session.close()
        return

    # Normal sync flow
    simplefin_config = {"access_url": config.get("simplefin.access_url")}

    if not simplefin_config["access_url"]:
        console.print("[yellow]SimpleFIN not configured. Run:[/yellow]")
        console.print("  datahub sync simplefin --setup YOUR_SETUP_TOKEN")
        console.print("\n[dim]Get your setup token from: https://simplefin.org[/dim]")
        return

    console.print(f"[blue]Syncing SimpleFIN transactions (last {days} days)...[/blue]")

    session = get_session(db_path)
    connector = SimpleFINConnector(session, config=simplefin_config)

    since = datetime.now(timezone.utc) - timedelta(days=days)

    try:
        log = connector.run_sync(since)
        console.print(f"[green]Sync complete![/green]")
        console.print(f"  Transactions added: {log.records_added}")
        console.print(f"  Transactions skipped (duplicates): {log.records_updated}")
    except Exception as e:
        console.print(f"[red]Sync failed: {e}[/red]")
    finally:
        connector.close()
        session.close()


@cli.command()
@click.pass_context
def status(ctx):
    """Show DataHub status and data summary."""
    config = ctx.obj["config"]
    db_path = config.get_db_path()

    if not db_path.exists():
        console.print("[yellow]DataHub not initialized. Run 'datahub init' first.[/yellow]")
        return

    session = get_session(db_path)

    # Data points summary
    console.print("\n[bold]Data Points by Type[/bold]")
    table = Table()
    table.add_column("Type", style="cyan")
    table.add_column("Count", justify="right")
    table.add_column("Latest", style="dim")

    stmt = (
        select(
            DataPoint.data_type,
            func.count(DataPoint.id).label("count"),
            func.max(DataPoint.timestamp).label("latest"),
        )
        .group_by(DataPoint.data_type)
        .order_by(func.count(DataPoint.id).desc())
    )

    for row in session.execute(stmt):
        latest = row.latest.strftime("%Y-%m-%d") if row.latest else "N/A"
        table.add_row(row.data_type, str(row.count), latest)

    console.print(table)

    # Data by source
    console.print("\n[bold]Data Points by Source[/bold]")
    source_table = Table()
    source_table.add_column("Source", style="cyan")
    source_table.add_column("Count", justify="right")

    stmt = (
        select(DataPoint.source, func.count(DataPoint.id).label("count"))
        .group_by(DataPoint.source)
        .order_by(func.count(DataPoint.id).desc())
    )

    for row in session.execute(stmt):
        source_table.add_row(row.source, str(row.count))

    console.print(source_table)

    # Transactions summary
    txn_count = session.execute(select(func.count(Transaction.id))).scalar()
    if txn_count:
        console.print(f"\n[bold]Transactions[/bold]")
        txn_table = Table()
        txn_table.add_column("Account/Source", style="cyan")
        txn_table.add_column("Count", justify="right")
        txn_table.add_column("Total", justify="right")

        stmt = (
            select(
                Transaction.source,
                func.count(Transaction.id).label("count"),
                func.sum(Transaction.amount).label("total"),
            )
            .group_by(Transaction.source)
        )

        for row in session.execute(stmt):
            total_str = f"${row.total:,.2f}" if row.total else "$0.00"
            txn_table.add_row(row.source, str(row.count), total_str)

        console.print(txn_table)

    # Recent syncs
    console.print("\n[bold]Recent Syncs[/bold]")
    sync_table = Table()
    sync_table.add_column("Connector", style="cyan")
    sync_table.add_column("Status")
    sync_table.add_column("Records", justify="right")
    sync_table.add_column("When", style="dim")

    stmt = select(SyncLog).order_by(SyncLog.started_at.desc()).limit(5)

    for log in session.execute(stmt).scalars():
        status_color = "green" if log.status == "success" else "red"
        when = log.started_at.strftime("%Y-%m-%d %H:%M")
        sync_table.add_row(
            log.connector,
            f"[{status_color}]{log.status}[/{status_color}]",
            str(log.records_added),
            when,
        )

    console.print(sync_table)
    session.close()


@cli.command()
@click.argument("data_type")
@click.option("--days", default=7, help="Number of days to look back")
@click.pass_context
def query(ctx, data_type: str, days: int):
    """Query data points by type."""
    config = ctx.obj["config"]
    db_path = config.get_db_path()

    if not db_path.exists():
        console.print("[yellow]DataHub not initialized. Run 'datahub init' first.[/yellow]")
        return

    session = get_session(db_path)
    since = datetime.now(timezone.utc) - timedelta(days=days)

    stmt = (
        select(DataPoint)
        .where(DataPoint.data_type == data_type)
        .where(DataPoint.timestamp >= since)
        .order_by(DataPoint.timestamp.desc())
        .limit(50)
    )

    results = list(session.execute(stmt).scalars())

    if not results:
        console.print(f"[yellow]No {data_type} data found in the last {days} days.[/yellow]")
        return

    table = Table(title=f"{data_type} - Last {days} days")
    table.add_column("Date", style="cyan")
    table.add_column("Value", justify="right")
    table.add_column("Unit", style="dim")
    table.add_column("Source", style="dim")

    for dp in results:
        table.add_row(
            dp.timestamp.strftime("%Y-%m-%d %H:%M"),
            f"{dp.value:.1f}",
            dp.unit or "",
            dp.source,
        )

    console.print(table)
    session.close()


@cli.command()
@click.option("--days", default=7, help="Number of days to summarize")
@click.pass_context
def summary(ctx, days: int):
    """Show a daily summary of key metrics (deduplicated)."""
    config = ctx.obj["config"]
    db_path = config.get_db_path()

    if not db_path.exists():
        console.print("[yellow]DataHub not initialized. Run 'datahub init' first.[/yellow]")
        return

    session = get_session(db_path)
    now = datetime.now()
    since = now - timedelta(days=days)

    console.print(f"\n[bold]Daily Summary - Last {days} Days (Deduplicated)[/bold]\n")

    # Steps per day (deduplicated)
    steps_data = deduplicate_daily_totals(session, "steps", since, now)

    if steps_data:
        table = Table(title="Daily Steps")
        table.add_column("Date", style="cyan")
        table.add_column("Steps", justify="right")

        for row in sorted(steps_data, key=lambda x: x["date"], reverse=True):
            table.add_row(row["date"], f"{int(row['total']):,}")

        console.print(table)

    session.close()


@cli.command()
@click.option("--days", default=30, help="Number of days to look back")
@click.option("--category", default=None, help="Filter by category")
@click.option("--limit", default=25, help="Max transactions to show")
@click.pass_context
def transactions(ctx, days: int, category: str | None, limit: int):
    """Show recent transactions."""
    config = ctx.obj["config"]
    db_path = config.get_db_path()

    if not db_path.exists():
        console.print("[yellow]DataHub not initialized. Run 'datahub init' first.[/yellow]")
        return

    session = get_session(db_path)
    since = datetime.now(timezone.utc) - timedelta(days=days)

    stmt = (
        select(Transaction)
        .where(Transaction.date >= since)
        .order_by(Transaction.date.desc())
        .limit(limit)
    )

    if category:
        stmt = stmt.where(Transaction.category == category)

    results = list(session.execute(stmt).scalars())

    if not results:
        console.print(f"[yellow]No transactions found in the last {days} days.[/yellow]")
        return

    table = Table(title=f"Transactions - Last {days} days")
    table.add_column("Date", style="cyan")
    table.add_column("Amount", justify="right")
    table.add_column("Description")
    table.add_column("Category", style="dim")

    for txn in results:
        amount_style = "red" if txn.amount < 0 else "green"
        table.add_row(
            txn.date.strftime("%Y-%m-%d"),
            f"[{amount_style}]${txn.amount:,.2f}[/{amount_style}]",
            txn.description[:40] + "..." if len(txn.description) > 40 else txn.description,
            txn.category or "",
        )

    console.print(table)

    # Show totals
    total = sum(t.amount for t in results)
    console.print(f"\n[bold]Total: ${total:,.2f}[/bold]")

    session.close()


@cli.command()
@click.option("--days", default=30, help="Number of days to analyze")
@click.pass_context
def spending(ctx, days: int):
    """Show spending breakdown by category."""
    config = ctx.obj["config"]
    db_path = config.get_db_path()

    if not db_path.exists():
        console.print("[yellow]DataHub not initialized. Run 'datahub init' first.[/yellow]")
        return

    session = get_session(db_path)
    since = datetime.now(timezone.utc) - timedelta(days=days)

    stmt = (
        select(
            Transaction.category,
            func.count(Transaction.id).label("count"),
            func.sum(Transaction.amount).label("total"),
        )
        .where(Transaction.date >= since)
        .where(Transaction.amount < 0)  # Only spending (negative amounts)
        .group_by(Transaction.category)
        .order_by(func.sum(Transaction.amount))
    )

    results = list(session.execute(stmt))

    if not results:
        console.print(f"[yellow]No spending data found in the last {days} days.[/yellow]")
        return

    table = Table(title=f"Spending by Category - Last {days} days")
    table.add_column("Category", style="cyan")
    table.add_column("Count", justify="right")
    table.add_column("Total", justify="right", style="red")

    total_spending = 0
    for row in results:
        category = row.category or "Uncategorized"
        table.add_row(category, str(row.count), f"${abs(row.total):,.2f}")
        total_spending += row.total

    console.print(table)
    console.print(f"\n[bold]Total Spending: [red]${abs(total_spending):,.2f}[/red][/bold]")

    session.close()


@cli.command()
@click.option("--days", default=30, help="Number of days to analyze")
@click.pass_context
def insights(ctx, days: int):
    """Show insights and correlations in your data (deduplicated)."""
    config = ctx.obj["config"]
    db_path = config.get_db_path()

    if not db_path.exists():
        console.print("[yellow]DataHub not initialized. Run 'datahub init' first.[/yellow]")
        return

    session = get_session(db_path)
    now = datetime.now()
    since = now - timedelta(days=days)

    console.print(f"\n[bold]Insights - Last {days} Days (Deduplicated)[/bold]\n")

    # Get daily aggregates for steps, calories, spending, sleep (deduplicated)
    daily_steps = {
        row["date"]: row["total"]
        for row in deduplicate_daily_totals(session, "steps", since, now)
    }

    daily_calories = {
        row["date"]: row["total"]
        for row in deduplicate_daily_totals(session, "active_calories", since, now)
    }

    daily_sleep = {
        row["date"]: row["total"]
        for row in deduplicate_daily_totals(session, "sleep_minutes", since, now)
    }

    # Spending doesn't need deduplication (no duplicate sources)
    daily_spending = dict(session.execute(
        select(func.date(Transaction.date), func.sum(Transaction.amount))
        .where(Transaction.date >= since)
        .where(Transaction.amount < 0)
        .group_by(func.date(Transaction.date))
    ).all())

    # Readiness scores - use deduplicated data
    daily_readiness = {
        row["date"]: row["total"]
        for row in deduplicate_daily_totals(session, "readiness", since, now)
    }

    # Calculate averages
    if daily_steps:
        avg_steps = sum(daily_steps.values()) / len(daily_steps)
        console.print(f"[cyan]Average Daily Steps:[/cyan] {avg_steps:,.0f}")

        # Find best and worst days
        best_day = max(daily_steps.items(), key=lambda x: x[1])
        worst_day = min(daily_steps.items(), key=lambda x: x[1])
        console.print(f"  Best day: {best_day[0]} ({best_day[1]:,.0f} steps)")
        console.print(f"  Lowest day: {worst_day[0]} ({worst_day[1]:,.0f} steps)")

    if daily_sleep:
        avg_sleep = sum(daily_sleep.values()) / len(daily_sleep)
        console.print(f"\n[cyan]Average Sleep:[/cyan] {avg_sleep / 60:.1f} hours")

    if daily_readiness:
        avg_readiness = sum(daily_readiness.values()) / len(daily_readiness)
        console.print(f"\n[cyan]Average Readiness Score:[/cyan] {avg_readiness:.0f}")

    if daily_spending:
        avg_spending = sum(daily_spending.values()) / len(daily_spending)
        console.print(f"\n[cyan]Average Daily Spending:[/cyan] ${abs(avg_spending):,.2f}")

    # Correlation: High activity days vs spending
    if daily_steps and daily_spending:
        console.print("\n[bold]Activity vs Spending Correlation[/bold]")

        high_activity_days = [d for d, s in daily_steps.items() if s > avg_steps * 1.2]
        low_activity_days = [d for d, s in daily_steps.items() if s < avg_steps * 0.8]

        high_activity_spending = [abs(daily_spending.get(d, 0)) for d in high_activity_days if d in daily_spending]
        low_activity_spending = [abs(daily_spending.get(d, 0)) for d in low_activity_days if d in daily_spending]

        if high_activity_spending and low_activity_spending:
            avg_high = sum(high_activity_spending) / len(high_activity_spending)
            avg_low = sum(low_activity_spending) / len(low_activity_spending)
            console.print(f"  High activity days ({len(high_activity_days)}): avg ${avg_high:,.2f} spending")
            console.print(f"  Low activity days ({len(low_activity_days)}): avg ${avg_low:,.2f} spending")

    # Workout frequency
    workout_count = session.execute(
        select(func.count(DataPoint.id))
        .where(DataPoint.data_type == "workout")
        .where(DataPoint.timestamp >= since)
    ).scalar() or 0

    if workout_count:
        console.print(f"\n[cyan]Workouts:[/cyan] {workout_count} in {days} days ({workout_count / days * 7:.1f}/week)")

    session.close()


@cli.command()
@click.option("--format", "output_format", default="json", type=click.Choice(["json", "csv"]))
@click.option("--type", "data_type", default=None, help="Filter by data type")
@click.option("--days", default=30, help="Days of data to export")
@click.option("--output", "-o", default=None, help="Output file path")
@click.pass_context
def export(ctx, output_format: str, data_type: str | None, days: int, output: str | None):
    """Export data to JSON or CSV."""
    import csv as csv_module
    import json

    config = ctx.obj["config"]
    db_path = config.get_db_path()

    if not db_path.exists():
        console.print("[yellow]DataHub not initialized. Run 'datahub init' first.[/yellow]")
        return

    session = get_session(db_path)
    since = datetime.now(timezone.utc) - timedelta(days=days)

    # Build query
    stmt = select(DataPoint).where(DataPoint.timestamp >= since)
    if data_type:
        stmt = stmt.where(DataPoint.data_type == data_type)
    stmt = stmt.order_by(DataPoint.timestamp)

    results = list(session.execute(stmt).scalars())

    if not results:
        console.print("[yellow]No data found to export.[/yellow]")
        return

    # Convert to dicts
    data = [
        {
            "timestamp": dp.timestamp.isoformat(),
            "type": dp.data_type,
            "value": dp.value,
            "unit": dp.unit,
            "source": dp.source,
        }
        for dp in results
    ]

    # Determine output
    if output:
        output_path = Path(output)
    else:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        type_suffix = f"_{data_type}" if data_type else ""
        output_path = Path(f"datahub_export{type_suffix}_{timestamp}.{output_format}")

    if output_format == "json":
        output_path.write_text(json.dumps(data, indent=2))
    else:
        with open(output_path, "w", newline="") as f:
            writer = csv_module.DictWriter(f, fieldnames=["timestamp", "type", "value", "unit", "source"])
            writer.writeheader()
            writer.writerows(data)

    console.print(f"[green]Exported {len(data)} records to {output_path}[/green]")
    session.close()


@cli.command()
@click.option("--host", default="127.0.0.1", help="Host to bind to")
@click.option("--port", default=8000, help="Port to bind to")
@click.pass_context
def web(ctx, host: str, port: int):
    """Start the web dashboard."""
    try:
        import uvicorn
    except ImportError:
        console.print("[red]Web dependencies not installed. Run:[/red]")
        console.print("  pip install 'datahub[web]'")
        return

    config = ctx.obj["config"]
    db_path = config.get_db_path()

    if not db_path.exists():
        console.print("[red]Database not initialized. Run 'datahub init' first.[/red]")
        return

    console.print(f"[green]Starting DataHub dashboard at http://{host}:{port}[/green]")

    import sys
    from pathlib import Path
    # Add web directory to path
    web_dir = Path(__file__).parent.parent / "web"
    sys.path.insert(0, str(web_dir.parent))

    from web.app import app
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    cli()
