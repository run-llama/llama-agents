from __future__ import annotations

import click
from llama_agents.cli.output import status, warning
from llama_agents.cli.utils.capabilities import probe_organizations_support

from ..app import app
from ..display import OrgDisplay
from ..options import global_options, output_option, render_output
from .auth import _get_service, _list_organizations


@app.group(
    help="Manage organizations",
    no_args_is_help=True,
)
@global_options
def organizations() -> None:
    pass


@organizations.command("get")
@global_options
@output_option
def get_organizations(output: str) -> None:
    """List organizations available to the current profile."""
    try:
        auth_svc = _get_service().current_auth_service()
        if not probe_organizations_support():
            if output == "text":
                warning("this server does not support organizations")
                return
            render_output([], output)
            return

        orgs = _list_organizations(auth_svc)
        if not orgs and output == "text":
            status("no organizations found")
            return

        default_org = next((o.org_id for o in orgs if o.is_default), None)
        displays = [
            OrgDisplay.from_org_summary(org, current_org_id=default_org) for org in orgs
        ]
        render_output(displays, output)

    except Exception as e:
        raise click.ClickException(str(e)) from e
