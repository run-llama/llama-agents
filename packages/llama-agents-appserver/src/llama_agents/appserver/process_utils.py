import functools
import logging
import os
import platform
import subprocess
import sys
import threading
from dataclasses import dataclass
from typing import Callable, TextIO, Tuple, cast


def run_process(
    cmd: list[str],
    *,
    cwd: os.PathLike | None = None,
    env: dict[str, str] | None = None,
    prefix: str | None = None,
    color_code: str = "36",
    line_transform: Callable[[str], str | None] | None = None,
    use_tty: bool | None = None,
) -> None:
    """Run a process and stream its output with optional TTY semantics.

    If use_tty is None, a PTY will be used only when the parent's stdout is a TTY
    and the platform supports PTYs. When a PTY is used, stdout/stderr are merged.
    """
    use_pty = _should_use_pty(use_tty)
    prefixer = _make_prefixer(prefix, color_code, line_transform)

    spawned = _spawn_process(cmd, cwd=cwd, env=env, use_pty=use_pty)
    threads: list[threading.Thread] = []
    try:
        spawned.cleanup()
        _log_command(cmd, prefixer)
        threads = _start_stream_threads(spawned.sources, prefixer)
        ret = spawned.process.wait()
        if ret != 0:
            raise subprocess.CalledProcessError(ret, cmd)
    finally:
        for t in threads:
            t.join()


def spawn_process(
    cmd: list[str],
    *,
    cwd: os.PathLike | None = None,
    env: dict[str, str] | None = None,
    prefix: str | None = None,
    color_code: str = "36",
    line_transform: Callable[[str], str | None] | None = None,
    use_tty: bool | None = None,
) -> subprocess.Popen:
    """Spawn a process and stream its output in background threads.

    Returns immediately with the Popen object. Streaming threads are daemons.
    """
    use_pty = _should_use_pty(use_tty)
    prefixer = _make_prefixer(prefix, color_code, line_transform)

    spawned = _spawn_process(cmd, cwd=cwd, env=env, use_pty=use_pty)
    spawned.cleanup()
    _log_command(cmd, prefixer)
    _start_stream_threads(spawned.sources, prefixer)
    return spawned.process


@functools.cache
def _use_color() -> bool:
    """Return True if ANSI colors should be emitted to stdout.

    Respects common environment variables and falls back to TTY detection.
    """
    force_color = os.environ.get("FORCE_COLOR")

    return sys.stdout.isatty() or force_color is not None and force_color != "0"


def _colored_prefix(prefix: str, color_code: str) -> str:
    return f"\x1b[{color_code}m{prefix}\x1b[0m " if _use_color() else f"{prefix} "


def _make_prefixer(
    prefix: str | None,
    color_code: str,
    line_transform: Callable[[str], str | None] | None = None,
) -> Callable[[str], str | None]:
    colored = _colored_prefix(prefix, color_code) if prefix else ""

    def _prefixer(line: str) -> str | None:
        transformed = line_transform(line) if line_transform else line
        if transformed is None:
            return None
        return f"{colored}{transformed}"

    return _prefixer


# Unified PTY/Pipe strategy helpers


def _should_use_pty(use_tty: bool | None) -> bool:
    if platform.system() == "Windows":
        return False
    if use_tty is None:
        return sys.stdout.isatty()
    return use_tty and sys.stdout.isatty() and not os.environ.get("NO_COLOR")


def should_use_color() -> bool:
    return _should_use_pty(None)


@dataclass
class SpawnProcessResult:
    process: subprocess.Popen[str] | subprocess.Popen[bytes]
    sources: list[Tuple[int | TextIO, TextIO]]
    cleanup: Callable[[], None]


def _spawn_process(
    cmd: list[str],
    *,
    cwd: os.PathLike | None,
    env: dict[str, str] | None,
    use_pty: bool,
) -> SpawnProcessResult:
    process: subprocess.Popen[str] | subprocess.Popen[bytes]
    if use_pty:
        import pty

        master_fd, slave_fd = pty.openpty()
        process = subprocess.Popen(
            cmd,
            env=env,
            cwd=cwd,
            stdin=slave_fd,
            stdout=slave_fd,
            stderr=slave_fd,
            close_fds=True,
        )

        def cleanup() -> None:
            try:
                os.close(slave_fd)
            except OSError:
                pass

        sources: list[tuple[int | TextIO, TextIO]] = [
            (master_fd, cast(TextIO, sys.stdout)),
        ]
        return SpawnProcessResult(process, sources, cleanup)

    use_shell = False
    if platform.system() == "Windows":
        use_shell = True
    process = subprocess.Popen(
        cmd,
        env=env,
        cwd=cwd,
        stdin=None,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
        shell=use_shell,
    )

    def cleanup_non_pty() -> None:
        return None

    assert process.stdout is not None and process.stderr is not None
    sources = [
        (cast(int | TextIO, process.stdout), cast(TextIO, sys.stdout)),
        (cast(int | TextIO, process.stderr), cast(TextIO, sys.stderr)),
    ]
    return SpawnProcessResult(process, sources, cleanup_non_pty)


def _stream_source(
    source: int | TextIO,
    writer: TextIO,
    transform: Callable[[str], str | None] | None,
) -> None:
    if isinstance(source, int):
        try:
            with os.fdopen(
                source, "r", encoding="utf-8", errors="replace", buffering=1
            ) as f:
                for line in f:
                    out = transform(line) if transform else line
                    if out is not None:
                        try:
                            writer.write(out)
                            writer.flush()
                        except UnicodeEncodeError:
                            pass
        except OSError:
            # PTY EOF may raise EIO; ignore
            pass
    else:
        try:
            for line in iter(source.readline, ""):
                out = transform(line) if transform else line
                if out is None:
                    continue
                try:
                    writer.write(out)
                except UnicodeEncodeError:
                    encoding = writer.encoding
                    if encoding is None:
                        raise
                    writer.write(
                        out.encode(encoding, errors="backslashreplace").decode(encoding)
                    )
                writer.flush()
        finally:
            try:
                source.close()
            except Exception:
                pass


def _log_command(cmd: list[str], transform: Callable[[str], str | None] | None) -> None:
    cmd_str = "> " + " ".join(cmd)
    if transform:
        transformed = transform(cmd_str)
        if transformed is not None:
            cmd_str = transformed
    sys.stderr.write(cmd_str + "\n")


def _start_stream_threads(
    sources: list[tuple[int | TextIO, TextIO]],
    transform: Callable[[str], str | None] | None,
) -> list[threading.Thread]:
    threads: list[threading.Thread] = []
    for src, dst in sources:
        t = threading.Thread(
            target=_stream_source, args=(src, dst, transform), daemon=True
        )
        t.start()
        threads.append(t)
    return threads


class BootstrapHandler(logging.Handler):
    """A logging handler that prints colored-prefixed lines to stderr.

    Matches the visual style of ``run_process`` output so bootstrap messages
    appear inline with subprocess output.  Once ``setup_logging`` configures
    structlog, this handler can be removed and the same ``logger.info()``
    calls will be routed through structlog instead.
    """

    def __init__(
        self,
        prefix: str = "[bootstrap]",
        color_code: str = "33",
        level: int = logging.DEBUG,
    ) -> None:
        super().__init__(level)
        self._colored = _colored_prefix(prefix, color_code)

    def emit(self, record: logging.LogRecord) -> None:
        msg = self.format(record)
        if record.levelno >= logging.WARNING and _use_color():
            msg = f"\x1b[1m{msg}\x1b[0m"
        sys.stderr.write(f"{self._colored}{msg}\n")
        sys.stderr.flush()
