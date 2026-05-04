from unittest.mock import patch

from llama_agents.cli.interactive import is_interactive_session


def _clear_cache() -> None:
    # is_interactive_session is cached; ensure each test has a clean slate
    try:
        is_interactive_session.cache_clear()
    except Exception:
        pass


def test_is_interactive_false_when_not_tty() -> None:
    _clear_cache()
    with (
        patch(
            "llama_agents.cli.interactive.sys.stdin.isatty",
            return_value=False,
        ),
        patch(
            "llama_agents.cli.interactive.sys.stdout.isatty",
            return_value=True,
        ),
    ):
        assert is_interactive_session() is False


def test_is_interactive_false_when_term_dumb() -> None:
    _clear_cache()
    with (
        patch(
            "llama_agents.cli.interactive.sys.stdin.isatty",
            return_value=True,
        ),
        patch(
            "llama_agents.cli.interactive.sys.stdout.isatty",
            return_value=True,
        ),
        patch(
            "llama_agents.cli.interactive.os.environ",
            {"TERM": "dumb"},
        ),
    ):
        assert is_interactive_session() is False


def test_is_interactive_true_in_tty() -> None:
    _clear_cache()
    with (
        patch(
            "llama_agents.cli.interactive.sys.stdin.isatty",
            return_value=True,
        ),
        patch(
            "llama_agents.cli.interactive.sys.stdout.isatty",
            return_value=True,
        ),
        patch("llama_agents.cli.interactive.os.environ", {}),
    ):
        assert is_interactive_session() is True
