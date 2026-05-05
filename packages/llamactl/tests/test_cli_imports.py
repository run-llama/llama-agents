import subprocess
import sys
from textwrap import dedent


def test_llamactl_help_does_not_import_heavy_modules() -> None:
    """Ensure `llamactl --help` does not require heavy, optional modules.

    Runs the CLI help in a clean Python subprocess and inspects which modules
    were imported, without mutating this test process's import state.
    """
    forbidden_prefixes = (
        "llama_agents.appserver",
        "aiohttp",
        "httpx",
        "llama_index",
    )

    script = dedent(
        """
        import sys

        from click.testing import CliRunner
        from llama_agents.cli.app import app

        runner = CliRunner()
        result = runner.invoke(app, ["--help"])
        if result.exit_code != 0:
            # Propagate the error code so the parent test can see the failure.
            raise SystemExit(result.exit_code)

        for name in sorted(sys.modules):
            print(name)
        """
    )

    proc = subprocess.run(
        [sys.executable, "-c", script],
        check=False,
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, proc.stderr

    imported = proc.stdout.splitlines()
    imported_heavy = [
        name
        for name in imported
        if any(name == p or name.startswith(f"{p}.") for p in forbidden_prefixes)
    ]
    assert imported_heavy == []
