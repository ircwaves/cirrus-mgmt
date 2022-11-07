import json
import logging
import sys

import click
from cirrus.cli.utils import click as utils_click

from cirrus.plugins.management.utils.click import (
    additional_variables,
    silence_templating_errors,
)

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
@additional_variables
@silence_templating_errors
def template(additional_variables, silence_templating_errors):
    from cirrus.plugins.management.utils.templating import template_payload

    click.echo(
        template_payload(
            sys.stdin.read(), additional_variables, silence_templating_errors
        )
    )
