import logging

import click
from cirrus.cli.utils import click as utils_click

from cirrus.plugins.management.deployment import Deployment

logger = logging.getLogger(__name__)


@click.group(
    cls=utils_click.AliasedShortMatchGroup,
)
@utils_click.requires_project
def deployments(project):
    """
    List/add/remove project deployments.
    """
    pass


@deployments.command(aliases=["ls", "list"])
@utils_click.requires_project
def show(project):
    for deployment_name in Deployment.yield_deployments(project):
        click.echo(deployment_name)


# TODO: better help
@deployments.command(aliases=["mk"])
@utils_click.requires_project
@click.argument(
    "name",
    metavar="name",
)
@click.option(
    "--stackname",
)
@click.option(
    "--profile",
)
def add(project, name, stackname=None, profile=None):
    Deployment.create(name, project, stackname=stackname, profile=profile)


@deployments.command(aliases=["rm"])
@utils_click.requires_project
@click.argument(
    "name",
    metavar="name",
)
def remove(project, name):
    Deployment.remove(name, project)
