from unittest.mock import patch

from llama_agents.cli.interactive import is_interactive_session


def test_is_interactive_false_when_not_tty() -> None:
    with (
        patch(
            "llama_agents.cli.interactive.sys.stdin.isatty",
            return_value=False,
        ),
        patch(
            "llama_agents.cli.interactive.sys.stdout.isatty",
            return_value=True,
        ),
        patch("llama_agents.cli.interactive.os.environ", {}),
    ):
        assert is_interactive_session() is False


def test_is_interactive_false_when_term_dumb() -> None:
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


def test_is_interactive_false_when_ci_set() -> None:
    with (
        patch(
            "llama_agents.cli.interactive.sys.stdin.isatty",
            return_value=True,
        ),
        patch(
            "llama_agents.cli.interactive.sys.stdout.isatty",
            return_value=True,
        ),
        patch("llama_agents.cli.interactive.os.environ", {"CI": "true"}),
    ):
        assert is_interactive_session() is False


def test_is_interactive_true_in_tty() -> None:
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
