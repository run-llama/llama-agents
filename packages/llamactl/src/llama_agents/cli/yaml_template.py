# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Render a :class:`DeploymentDisplay` as commented apply-shaped YAML.

One ``render()`` walks ``DeploymentDisplay``'s top-level identity fields
(``name``, optionally ``generate_name``) and then ``DeploymentSpec.model_fields``
in declaration order, appending lines to a list. Each spec field renders in
one of three states:

* **set** — ``<key>: <scalar>`` (uncommented), with the field's
  :class:`~llama_agents.cli.display.Doc` text as ``## …`` lines above.
  ``secrets`` is the only nested shape; its child keys render at indent 4.
* **required-but-unset** — ``<key>: ~`` with a ``## Required …`` marker so a
  ``grep`` or eyeball pass finds the gaps that block ``apply``.
* **unset** — ``# <key>: <example>`` (commented out one-liner) with the doc
  above. The schema-fixed ``_EXAMPLES`` table supplies the example value.

Top-level ``name`` follows the set / unset split (no required path — the
server slugifies an id when ``name`` is omitted, so a missing top-level
``name`` is never an apply blocker). ``generate_name`` is special-cased: the
caller opts in via ``scaffold_generate_name`` (only the offline ``deployments
template`` flow does), and when emitted it always renders commented-out under
the identity-tier comment block.

A small ``field_alternatives`` mapping lets the caller surface a
commented-out alternative under a *set* field (used to suggest the detected
git remote under an empty ``repo_url``). Scalar quoting delegates to PyYAML;
Single-character strings are force-quoted so values like ``"."`` read
unambiguously rather than hitting YAML plain-scalar edge cases.

Mask sentinels (``SECRET_MASK``) inside ``secrets`` and on
``personal_access_token`` are stripped before rendering — the same filter
:func:`~llama_agents.cli.display.strip_masks` applies to ``-o yaml`` /
``-o json`` output, so a ``get -o template | apply`` round-trip can't push a
literal ``********`` back as the value.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from typing import Any

import yaml
from llama_agents.cli.display import (
    DeploymentDisplay,
    DeploymentSpec,
    Doc,
    TrailingDoc,
    strip_masks,
)
from pydantic.fields import FieldInfo

_INDENT = "  "
_MARKER = "## "
_TRAILING_COLUMN = 45
_SCAFFOLD_IDENTITY_DOCS = (
    "Set 'name' for a stable id, or 'generate_name' to let the server pick a",
    "unique id from this slug. Create needs one of these fields.",
)

# Per-field example values shown when a spec field is unset (rendered commented).
_EXAMPLES: dict[str, Any] = {
    # The push-mode sentinel (``""``) is documented separately via
    # :data:`~llama_agents.cli.display.PUSH_MODE_REPO_URL`; the example here
    # shows a real URL so a user uncommenting the line gets a working shape.
    "repo_url": "https://github.com/owner/repo",
    "deployment_file_path": ".",
    "git_ref": "main",
    "appserver_version": "0.5.0",
    "suspended": False,
    "secrets": {"MY_SECRET": "${MY_SECRET}"},
    "personal_access_token": "${GITHUB_TOKEN}",
}


def render(
    display: DeploymentDisplay,
    *,
    head: Sequence[str] = (),
    secret_comments: Mapping[str, str] = {},
    field_alternatives: Mapping[str, tuple[str, str]] = {},
    required: Iterable[str] = (),
    name_example: str = "my-app",
    scaffold_generate_name: bool = False,
) -> str:
    """Render ``display`` as a commented apply-shaped YAML string.

    Args:
        display: The deployment to render. ``status`` is unconditionally
            omitted; only ``name``, ``generate_name``, and ``spec`` reach the
            output.
        head: Lines emitted at the top of the output as ``## `` comments,
            before ``name:``. An empty-string entry becomes a bare ``##``.
        secret_comments: Map of secret name → comment text. Each becomes a
            trailing ``## `` note on the matching key inside the ``secrets:``
            block. Ignored when ``secrets`` is unset.
        field_alternatives: Map of field name → ``(suggestion, annotation)``.
            For each *set* field listed, a commented-out
            ``# <field>: <suggestion>  ## <annotation>`` line is emitted
            directly under the field. Silently ignored for fields in the
            ``required`` or ``unset`` states.
        required: Field names (python attribute names on ``DeploymentSpec``)
            to force-emit as ``<key>: ~`` with a ``## Required …`` marker
            even when unset. The top-level ``name`` is *not* supported here —
            it has no required path.
        name_example: Example value rendered for an unset top-level ``name``
            (and ``generate_name`` when it has no model value). Defaults to
            ``"my-app"``; the offline template command passes the cwd name.
        scaffold_generate_name: When ``True``, emit a commented-out
            ``# generate_name: <value>`` line at the identity tier. When
            ``False`` (the default), the
            field is omitted entirely. Only the offline ``deployments
            template`` flow opts in; ``get -o template`` does not.
    """
    required_set = set(required)
    out: list[str] = []

    for line in head:
        out.append("##" if line == "" else f"{_MARKER}{line}")
    if head:
        out.append("")

    if scaffold_generate_name:
        out.extend(_doc_lines(_SCAFFOLD_IDENTITY_DOCS, indent=""))
    else:
        name_docs = _docs(DeploymentDisplay.model_fields["name"])
        out.extend(_doc_lines(name_docs, indent=""))
    if display.name is None:
        out.append(f"# name: {name_example}")
    else:
        out.append(f"name: {_scalar(display.name)}")

    if scaffold_generate_name:
        gn_value = display.generate_name or name_example
        out.append(f"# generate_name: {_scalar(gn_value)}")

    out.append("")
    out.append("spec:")
    spec_dump = display.spec.model_dump(mode="json", exclude_none=True)
    spec_set = strip_masks(spec_dump)

    for idx, (fname, finfo) in enumerate(DeploymentSpec.model_fields.items()):
        if idx > 0 and fname in {
            "deployment_file_path",
            "secrets",
            "personal_access_token",
        }:
            out.append("")

        docs = _docs(finfo)
        if fname in required_set and fname not in spec_set:
            docs = _required_docs(docs)
        out.extend(_doc_lines(docs, indent=_INDENT))

        if fname in spec_set:
            _emit_set_field(
                out,
                fname,
                spec_set[fname],
                finfo,
                secret_comments,
                field_alternatives,
            )
        elif fname in required_set:
            out.append(_with_trailing(f"{_INDENT}{fname}: ~", _trailing_doc(finfo)))
        else:
            _emit_unset_field(out, fname, finfo)

    return "\n".join(out) + "\n"


def _emit_set_field(
    out: list[str],
    fname: str,
    value: Any,
    finfo: FieldInfo,
    secret_comments: Mapping[str, str],
    field_alternatives: Mapping[str, tuple[str, str]],
) -> None:
    """Append lines for a spec field that has a value."""
    if fname == "secrets" and isinstance(value, dict):
        out.append(f"{_INDENT}{fname}:")
        for sname, sval in value.items():
            out.append(
                _with_trailing(
                    f"{_INDENT * 2}{sname}: {_scalar(sval)}",
                    _one_line(secret_comments.get(sname)),
                    align=False,
                )
            )
        return
    out.append(
        _with_trailing(f"{_INDENT}{fname}: {_scalar(value)}", _trailing_doc(finfo))
    )
    alt = field_alternatives.get(fname)
    if alt is not None:
        alt_value, alt_note = alt
        out.append(_with_trailing(f"{_INDENT}# {fname}: {alt_value}", alt_note))


def _emit_unset_field(out: list[str], fname: str, finfo: FieldInfo) -> None:
    """Append a commented-out example line for an unset spec field."""
    example = _EXAMPLES.get(fname, "")
    if isinstance(example, dict):
        out.append(f"{_INDENT}# {fname}:")
        for k, v in example.items():
            out.append(f"{_INDENT * 2}# {k}: {_scalar(v)}")
    else:
        out.append(
            _with_trailing(
                f"{_INDENT}# {fname}: {_scalar(example)}",
                _trailing_doc(finfo),
            )
        )


def _doc_lines(docs: Iterable[str], *, indent: str) -> list[str]:
    """Return ``## <doc>`` lines at the given indent, trailing-stripped."""
    return [f"{indent}{_MARKER}{d}".rstrip() for d in docs]


def _docs(info: FieldInfo | None) -> tuple[str, ...]:
    """Return the first ``Doc`` marker text on ``info``, split into lines."""
    if info is None:
        return ()
    for marker in info.metadata:
        if isinstance(marker, Doc):
            return tuple(marker.text.split("\n"))
    return ()


def _trailing_doc(info: FieldInfo | None) -> str | None:
    """Return the first ``TrailingDoc`` marker text on ``info``."""
    if info is None:
        return None
    for marker in info.metadata:
        if isinstance(marker, TrailingDoc):
            return marker.text
    return None


def _required_docs(docs: tuple[str, ...]) -> tuple[str, ...]:
    """Insert a REQUIRED marker above the existing doc lines."""
    return ("REQUIRED.", *docs)


def _one_line(value: str | None) -> str | None:
    if value is None:
        return None
    return " ".join(value.split())


def _with_trailing(line: str, note: str | None, *, align: bool = True) -> str:
    """Append an aligned trailing ``##`` note when ``note`` is present."""
    if not note:
        return line
    gap = max(2, _TRAILING_COLUMN - len(line)) if align else 2
    return f"{line}{' ' * gap}{_MARKER}{note}"


def _scalar(value: Any) -> str:
    """Render ``value`` as a YAML scalar suitable for the right-hand side of
    a key line.

    Delegates to PyYAML in mapping context (``{"_": value}``) so its
    emitter applies block-context rules — plain for ``${VAR}``, URLs,
    versions; quoted for reserved words / flow chars / values containing
    ``: `` or `` #``. Carve-outs: ``None`` emits as ``~`` for parity with
    the required-tilde rendering, ``""`` emits as ``""`` so the push-mode
    signal isn't hidden as PyYAML's default ``''``, and single-character
    strings (except ``"``) are force-quoted so values like ``"."`` read
    unambiguously rather than hitting YAML plain-scalar edge cases.
    """
    if value is None:
        return "~"
    if value == "":
        return '""'
    if isinstance(value, str) and len(value) == 1 and value != '"':
        return f'"{value}"'
    return yaml.safe_dump(
        {"_": value}, default_flow_style=False, width=1 << 30, allow_unicode=True
    ).rstrip()[3:]
