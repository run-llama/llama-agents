from __future__ import annotations

import io
import subprocess
import sys
import time
from pathlib import Path

import pytest
from llama_agents.appserver.process_utils import (
    _use_color,
    run_process,
    spawn_process,
)
from llama_agents.appserver.workflow_loader import (
    inject_appserver_into_target,
)
from llama_agents.core.deployment_config import DeploymentConfig


@pytest.fixture(autouse=True)
def _no_color(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FORCE_COLOR", raising=False)
    _use_color.cache_clear()


class _FakePopen:
    def __init__(self, text: str, ret: int = 0) -> None:
        self.stdout = io.StringIO(text)
        self.stderr = io.StringIO("")
        self._ret = ret

    def wait(self) -> int:
        return self._ret


def test_run_process_success_and_failure(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    # success path
    monkeypatch.setattr(
        "subprocess.Popen",
        lambda *a, **k: _FakePopen("line1\nline2\n", ret=0),
    )
    run_process(["echo"], prefix="[x]")
    out = capsys.readouterr().out
    assert "[x] line1" in out and "[x] line2" in out

    # failure path raises
    monkeypatch.setattr(
        "subprocess.Popen",
        lambda *a, **k: _FakePopen("oops\n", ret=5),
    )
    with pytest.raises(subprocess.CalledProcessError):
        run_process(["false"], prefix="[x]")


def test_spawn_process_streams_in_background(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    # Provide a short output that will be consumed by the background thread
    monkeypatch.setattr(
        "subprocess.Popen",
        lambda *a, **k: _FakePopen("a\nb\n", ret=0),
    )
    spawn_process(["cmd"], prefix="[p]", color_code="35")
    # Give the background thread a moment to flush
    time.sleep(0.05)
    out = capsys.readouterr().out
    assert "[p] a" in out and "[p] b" in out


def test_run_process_keeps_streaming_with_legacy_console(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    script = tmp_path / "child.py"
    script.write_text(
        "import sys\n"
        "sys.stdout.buffer.write(b'invalid \\xa9\\nplain\\n')\n"
        "sys.stderr.buffer.write(b'unicode \\xe2\\x9e\\x9c\\nfinished\\n')\n",
        encoding="utf-8",
    )

    stdout_bytes, stderr_bytes = io.BytesIO(), io.BytesIO()
    with (
        io.TextIOWrapper(stdout_bytes, encoding="cp1252", newline="\n") as stdout,
        io.TextIOWrapper(stderr_bytes, encoding="cp1252", newline="\n") as stderr,
        monkeypatch.context() as patch,
    ):
        patch.setattr(sys, "stdout", stdout)
        patch.setattr(sys, "stderr", stderr)
        run_process([sys.executable, str(script)], use_tty=False)

        assert b"invalid \\ufffd\nplain\n" in stdout_bytes.getvalue()
        assert b"unicode \\u279c\nfinished\n" in stderr_bytes.getvalue()


def test_install_python_dependencies_calls_when_target_found(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    calls: dict[str, int] = {"ensure": 0, "install": 0}
    monkeypatch.setattr(
        "llama_agents.appserver.workflow_loader._ensure_uv_available",
        lambda: calls.__setitem__("ensure", calls["ensure"] + 1),
    )
    monkeypatch.setattr(
        "llama_agents.appserver.workflow_loader._install_and_add_appserver_if_missing",
        lambda *a, **k: calls.__setitem__("install", calls["install"] + 1),
    )

    cfg = DeploymentConfig(name="n", workflows={})
    inject_appserver_into_target(cfg, tmp_path)
    assert calls == {"ensure": 1, "install": 1}
