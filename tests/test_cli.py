from pathlib import Path

from typer.testing import CliRunner

from datapipe import __version__
from datapipe.cli import app


def test_cli_version_and_help() -> None:
    runner = CliRunner()
    version = runner.invoke(app, ["--version"])
    help_result = runner.invoke(app, ["--help"])
    assert version.exit_code == 0 and __version__ in version.stdout
    assert help_result.exit_code == 0 and "validate-config" in help_result.stdout


def test_validate_config_json_and_invalid_exit_code(tmp_path: Path) -> None:
    runner = CliRunner()
    valid = runner.invoke(app, ["validate-config", "pipeline.toml", "--json"])
    missing = runner.invoke(app, ["validate-config", str(tmp_path / "absent.toml")])
    assert valid.exit_code == 0 and '"valid": true' in valid.stdout
    assert missing.exit_code == 2


def test_dry_run_json() -> None:
    result = CliRunner().invoke(app, ["run", "pipeline.yaml", "--dry-run", "--json"])
    assert result.exit_code == 0 and '"dry_run": true' in result.stdout
