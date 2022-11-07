import json
import logging
import sys
from functools import wraps

import click
from cirrus.cli.utils import click as utils_click
from click_option_group import RequiredMutuallyExclusiveOptionGroup, optgroup

from cirrus.plugins.management.deployment import Deployment
from cirrus.plugins.management.utils.click import (
    additional_variables,
    silence_templating_errors,
)

logger = logging.getLogger(__name__)

pass_deployment = click.make_pass_decorator(Deployment)


def _get_execution(deployment, arn=None, payload_id=None):
    if payload_id:
        return deployment.get_execution_by_payload_id(payload_id)
    return deployment.get_execution(arn)


def execution_arn(func):
    # reverse order because not using decorators
    func = optgroup.option(
        "--payload-id",
        help="payload ID (resolves to latest execution ARN)",
    )(func)
    func = optgroup.option(
        "--arn",
        help="Execution ARN",
    )(func)
    func = optgroup.group(
        "Identifier",
        cls=RequiredMutuallyExclusiveOptionGroup,
        help="Identifer type and value to get execution",
    )(func)
    return func


def raw_option(func):
    return click.option(
        "-r",
        "--raw",
        is_flag=True,
        help="Do not pretty-format the response",
    )(func)


def include_user_vars(func):
    @click.option(
        "--include-user-vars/--exclude-user-vars",
        default=True,
        help="Whether or not to load deployment's user vars into environment",
    )
    @wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapper


@click.group(
    aliases=["mgmt"],
    cls=utils_click.AliasedShortMatchGroup,
)
@utils_click.requires_project
@click.argument(
    "deployment",
    metavar="DEPLOYMENT_NAME",
)
@click.pass_context
def manage(ctx, project, deployment):
    """
    Commands to run management operations against project deployments.
    """
    try:
        ctx.obj = Deployment.from_name(deployment, project)
    except FileNotFoundError:
        logger.error("No such deployment: '%s'. Valid values:", deployment)
        for deployment in Deployment.yield_deployment_dirs(project):
            logger.error(f"    {deployment.name}")
        sys.exit(1)


@manage.command()
@pass_deployment
def show(deployment):
    """Show a deployment configuration"""
    color = "blue"
    click.secho(json.dumps(deployment.meta, indent=4), fg=color)


@manage.command("get-path")
@pass_deployment
def get_path(deployment):
    """Get path to deployment directory"""
    click.echo(deployment.path)


@manage.command()
@pass_deployment
@click.option(
    "--stackname",
)
@click.option(
    "--profile",
)
def refresh(deployment, stackname=None, profile=None):
    """Refresh the environment values from the AWS deployment,
    optionally changing the stackname or profile.
    """
    deployment.refresh(stackname=stackname, profile=profile)


@manage.command("get-payload")
@click.argument(
    "payload-id",
)
@raw_option
@pass_deployment
def get_payload(deployment, payload_id, raw):
    """Get a payload from S3 using its ID"""
    import botocore

    def download(output_fileobj):
        try:
            deployment.get_payload_by_id(payload_id, output_fileobj)
        except botocore.exceptions.ClientError as e:
            # TODO: understand why this is a ClientError even
            #   when it seems like it should be a NoKeyError
            logger.error(e)

    if raw:
        download(sys.stdout.buffer)
    else:
        import io

        with io.BytesIO() as b:
            download(b)
            b.seek(0)
            json.dump(json.load(b), sys.stdout, indent=4)

    # ensure we end with a newline
    print()


@manage.command("get-execution")
@execution_arn
@raw_option
@pass_deployment
def get_execution(deployment, arn, payload_id, raw):
    """Get a workflow execution using its ARN or its input payload ID"""
    execution = _get_execution(deployment, arn, payload_id)

    if raw:
        click.echo(execution)
    else:
        click.echo(json.dumps(execution, indent=4, default=str))


@manage.command("get-execution-input")
@execution_arn
@raw_option
@pass_deployment
def get_execution_input(deployment, arn, payload_id, raw):
    """Get a workflow execution's input payload using its ARN or its input payload ID"""
    _input = json.loads(_get_execution(deployment, arn, payload_id))["input"]

    if raw:
        click.echo(_input)
    else:
        click.echo(json.dumps(_input, indent=4, default=str))


@manage.command("get-execution-output")
@execution_arn
@raw_option
@pass_deployment
def get_execution_output(deployment, arn, payload_id, raw):
    """Get a workflow execution's output payload using its ARN or its input payload ID"""
    output = json.loads(_get_execution(deployment, arn, payload_id))["output"]

    if raw:
        click.echo(output)
    else:
        click.echo(json.dumps(output, indent=4, default=str))


@manage.command("get-state")
@click.argument(
    "payload-id",
)
@pass_deployment
def get_state(deployment, payload_id):
    """Get the statedb record for a payload ID"""
    state = deployment.get_payload_state(payload_id)
    click.echo(json.dumps(state, indent=4))


@manage.command()
@pass_deployment
def process(deployment):
    """Enqueue a payload (from stdin) for processing"""
    click.echo(json.dumps(deployment.process_payload(sys.stdin), indent=4))


@manage.command("template-payload")
@additional_variables
@silence_templating_errors
@include_user_vars
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
@pass_deployment
def template_payload(
    deployment,
    additional_variables,
    silence_templating_errors,
    include_user_vars,
):
    """Template a payload using a deployment's vars"""
    click.echo(
        deployment.template_payload(
            sys.stdin.read(),
            additional_variables,
            silence_templating_errors,
            include_user_vars,
        ),
    )


@manage.command(
    "exec",
    context_settings={
        "ignore_unknown_options": True,
    },
)
@click.argument(
    "command",
    nargs=-1,
)
@include_user_vars
@pass_deployment
@click.pass_context
def _exec(ctx, deployment, command, include_user_vars):
    """Run an executable with the deployment environment vars loaded"""
    if not command:
        return
    deployment.exec(command, include_user_vars=include_user_vars)


# check-pipeline
#   - this is like failmgr check
#   - not sure how to reconcile with cache above
#   - maybe need subcommand for everything it can do
