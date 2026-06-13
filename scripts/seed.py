"""
Seed script — posts demo data through the Stratum agent pipeline and prints a
rich summary. Runs automatically as a Docker service after the API is healthy.
"""

import sys
import time
from pathlib import Path

import httpx
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich import box

BASE_URL = "http://api:8000"
SAMPLES_DIR = Path(__file__).parent.parent / "tests" / "samples"
DEMO_FILES = [
    ("customers.csv", "customers", "text/csv"),
    ("orders.json", "orders", "application/json"),
]

console = Console()


def wait_for_api(timeout: int = 120) -> None:
    console.print("[bold cyan]Waiting for Stratum API...[/]")
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = httpx.get(f"{BASE_URL}/health", timeout=3)
            if r.status_code == 200:
                info = r.json()
                console.print(
                    f"[green]API ready[/] — model: [bold]{info.get('model')}[/], "
                    f"backend: [bold]{info.get('backend')}[/]"
                )
                return
        except Exception:
            pass
        time.sleep(3)
    console.print("[red]Timed out waiting for API.[/]")
    sys.exit(1)


def ingest_file(filename: str, source_name: str, mime: str) -> dict:
    path = SAMPLES_DIR / filename
    with open(path, "rb") as f:
        response = httpx.post(
            f"{BASE_URL}/api/v1/ingest",
            files={"file": (filename, f, mime)},
            data={"source_name": source_name},
            timeout=120,
        )
    response.raise_for_status()
    return response.json()


def render_results(results: list[tuple[str, dict]]) -> None:
    for source_name, data in results:
        version = data["latest_version"]
        schema = version["inferred_schema"]
        quality = data["quality_results"]

        tables_inferred = schema.get("tables", [])
        total_fields = sum(len(t.get("fields", [])) for t in tables_inferred)
        passed = sum(1 for q in quality if q["status"] == "pass")
        warned = sum(1 for q in quality if q["status"] == "warn")
        failed = sum(1 for q in quality if q["status"] == "fail")

        # Schema table
        schema_table = Table(box=box.SIMPLE_HEAVY, show_header=True, header_style="bold magenta")
        schema_table.add_column("Table", style="cyan")
        schema_table.add_column("Fields")
        schema_table.add_column("Description", style="dim")
        for t in tables_inferred:
            schema_table.add_row(
                t["table_name"],
                str(len(t.get("fields", []))),
                t.get("description", ""),
            )

        # Quality summary table
        quality_table = Table(box=box.SIMPLE_HEAVY, show_header=True, header_style="bold magenta")
        quality_table.add_column("Rule")
        quality_table.add_column("Field", style="dim")
        quality_table.add_column("Status")
        quality_table.add_column("Message", style="dim")
        for q in quality:
            status_color = {"pass": "green", "warn": "yellow", "fail": "red"}.get(q["status"], "white")
            quality_table.add_row(
                q["rule_name"],
                q.get("field") or "—",
                f"[{status_color}]{q['status'].upper()}[/]",
                q.get("message") or "",
            )

        drift_badge = ""
        if data.get("drift_detected"):
            sev = data.get("drift_severity", "")
            color = {"additive": "yellow", "risky": "dark_orange", "breaking": "red"}.get(sev, "white")
            drift_badge = f"  [bold {color}]DRIFT: {sev.upper()}[/]"

        console.print(
            Panel(
                f"[bold]{schema.get('source_description', source_name)}[/]\n\n"
                f"[bold]Schema[/] — {len(tables_inferred)} table(s), {total_fields} fields inferred{drift_badge}\n"
                + schema_table.__str__() + "\n"
                + f"[bold]Quality[/] — "
                + f"[green]{passed} pass[/]  [yellow]{warned} warn[/]  [red]{failed} fail[/]\n"
                + quality_table.__str__(),
                title=f"[bold white]{source_name}[/]",
                border_style="cyan",
            )
        )


def main() -> None:
    console.print(Panel.fit(
        "[bold cyan]Stratum[/]\nAI-powered schema inference, drift detection & data quality",
        border_style="cyan",
    ))

    wait_for_api()
    console.print()

    results = []
    for filename, source_name, mime in DEMO_FILES:
        console.print(f"[bold]Ingesting[/] [cyan]{filename}[/]...")
        try:
            data = ingest_file(filename, source_name, mime)
            results.append((source_name, data))
            console.print(f"  [green]✓[/] Done — schema version {data['latest_version']['version']}")
        except Exception as exc:
            console.print(f"  [red]✗[/] Failed: {exc}")

    console.print()
    render_results(results)

    console.print(Panel.fit(
        f"[bold green]Stratum is ready![/]\n\n"
        f"  API:          [link]{BASE_URL}[/link]\n"
        f"  Docs:         [link]{BASE_URL}/docs[/link]\n"
        f"  Health:       [link]{BASE_URL}/health[/link]\n\n"
        f"[dim]POST /api/v1/ingest to run the pipeline on your own data.[/dim]",
        border_style="green",
    ))


if __name__ == "__main__":
    main()
