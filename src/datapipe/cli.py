"""User-facing Typer CLI."""

import asyncio
import json
import logging
import signal
from pathlib import Path
from typing import Annotated, Any

import typer

from datapipe import __version__
from datapipe.config.loader import load_config
from datapipe.core.engine import run_pipeline
from datapipe.core.exceptions import ConfigurationError
from datapipe.observability.logging import ContextFilter, JsonLogFormatter
from datapipe.observability.redaction import RedactionFilter
from datapipe.plugins.discovery import discover

app = typer.Typer(
    no_args_is_help=True, help="Asynchronous data ingestion and transformation pipeline."
)


class _SignalTermination(Exception):
    def __init__(self, signum: int) -> None:
        self.signum = signum
        super().__init__(f"received signal {signum}")


def _logging(level: str) -> None:
    handler = logging.StreamHandler()
    handler.addFilter(RedactionFilter())
    handler.addFilter(ContextFilter())
    handler.setFormatter(JsonLogFormatter())
    logger = logging.getLogger("datapipe")
    logger.handlers.clear()
    logger.addHandler(handler)
    logger.setLevel(level.upper())
    logger.propagate = False


def _show_version(value: bool | None) -> None:
    if value:
        typer.echo(__version__)
        raise typer.Exit()


async def _run_with_signals(config_path: Path, fail_fast: bool, dry_run: bool) -> dict[str, Any]:
    """Translate termination signals into task cancellation for async cleanup."""
    loop = asyncio.get_running_loop()
    task = asyncio.current_task()
    previous: dict[signal.Signals, Any] = {}
    received_signal: int | None = None
    if task is None:
        raise RuntimeError("pipeline runner must execute in an asyncio task")

    def request_cancel(_number: int, _frame: Any) -> None:
        nonlocal received_signal
        received_signal = _number
        loop.call_soon_threadsafe(task.cancel)

    for signum in (signal.SIGINT, signal.SIGTERM):
        try:
            previous[signum] = signal.getsignal(signum)
            signal.signal(signum, request_cancel)
        except (OSError, ValueError):
            # Signal handlers can only be installed from the main thread.
            continue
    try:
        config = load_config(config_path)
        logger = logging.getLogger("datapipe")
        handlers = logger.handlers[:]
        level = logger.level
        propagate = logger.propagate
        _logging(config.log_level)
        try:
            return await run_pipeline(config, fail_fast, dry_run)
        except asyncio.CancelledError:
            if received_signal is None:
                raise
            raise _SignalTermination(received_signal) from None
        finally:
            logger.handlers.clear()
            logger.handlers.extend(handlers)
            logger.setLevel(level)
            logger.propagate = propagate
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)


@app.callback()
def root(
    version: Annotated[
        bool | None,
        typer.Option("--version", callback=_show_version, is_eager=True, help="Show version."),
    ] = None,
) -> None:
    """Datapipe CLI."""


@app.command("validate-config")
def validate_config(path: Path, as_json: bool = typer.Option(False, "--json")) -> None:
    """Parse and validate a TOML or YAML pipeline definition."""
    try:
        config = load_config(path)
    except ConfigurationError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(2) from exc
    result = {"valid": True, "pipeline": config.name, "sources": len(config.sources)}
    typer.echo(
        json.dumps(result)
        if as_json
        else f"Configuration valid: {config.name} ({len(config.sources)} sources)"
    )


@app.command()
def run(
    path: Path,
    dry_run: bool = typer.Option(False, "--dry-run"),
    as_json: bool = typer.Option(False, "--json"),
    fail_fast: bool = typer.Option(False, "--fail-fast"),
) -> None:
    """Execute a configured data pipeline."""
    try:
        result = asyncio.run(_run_with_signals(path, fail_fast, dry_run))
    except _SignalTermination as exc:
        raise typer.Exit(128 + exc.signum) from None
    except (ConfigurationError, ValueError) as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(2) from exc
    except Exception as exc:
        typer.echo(f"pipeline failed: {exc}", err=True)
        raise typer.Exit(1) from exc
    typer.echo(
        json.dumps(result, default=str)
        if as_json
        else (
            "Dry run ready."
            if dry_run
            else f"Pipeline {result.get('pipeline', path.stem)} completed: "
            f"{result.get('records_written', 0)} records written."
        )
    )


@app.command("list-plugins")
def list_plugins(as_json: bool = typer.Option(False, "--json")) -> None:
    """List built-in and installed source plugins."""
    try:
        names = discover()
    except Exception as exc:
        typer.echo(f"plugin discovery failed: {exc}", err=True)
        raise typer.Exit(1) from exc
    typer.echo(
        json.dumps(names)
        if as_json
        else ("\n".join(names) if names else "No external source plugins installed.")
    )


@app.command()
def version_command() -> None:
    """Show installed package version."""
    typer.echo(__version__)
