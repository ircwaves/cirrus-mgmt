import json
import logging
import sys

import click
from cirrus.cli.utils import click as utils_click

logger = logging.getLogger(__name__)


@click.group(
    cls=utils_click.AliasedShortMatchGroup,
)
def payload():
    """
    Commands for working with payloads.
    """
    pass


@payload.command()
def validate():
    from cirrus.lib2.process_payload import ProcessPayload

    payload = sys.stdin.read()
    ProcessPayload(**json.loads(payload))


@payload.command("get-id")
def get_id():
    from cirrus.lib2.process_payload import ProcessPayload

    payload = sys.stdin.read()
    click.echo(ProcessPayload(**json.loads(payload), set_id_if_missing=True)["id"])


@payload.command()
@click.argument(
    "variable_files",
    nargs=-1,
    type=click.File(),
)
@click.option(
    "-x",
    "--var",
    "additional_vars",
    nargs=2,
    multiple=True,
    help="Additional templating variables",
)
@click.option(
    "--silence-templating-errors",
    is_flag=True,
)
def template(variable_files, additional_vars, silence_templating_errors):
    from cirrus.core.deployment import load_env_file

    _vars = {}
    for f in variable_files:
        _vars.update(load_env_file(f))

    click.echo(
        template_payload(
            sys.stdin.read(), _vars, silence_templating_errors, **dict(additional_vars)
        )
    )


def template_payload(template, mapping, silence_templating_errors=False, **kwargs):
    from string import Template

    logger.debug("Templating vars: %s", mapping)
    template_fn = "safe_substitute" if silence_templating_errors else "substitute"
    return getattr(Template(template), template_fn)(mapping, **kwargs)
