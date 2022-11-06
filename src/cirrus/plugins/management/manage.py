import json
import logging
import sys

import click
from cirrus.cli.utils import click as utils_click
from click_option_group import RequiredMutuallyExclusiveOptionGroup, optgroup

from cirrus.plugins.management.deployment import Deployment, load_env_file

logger = logging.getLogger(__name__)

MAX_SQS_MESSAGE_LENGTH = 2**18  # max length of SQS message

pass_deployment = click.make_pass_decorator(Deployment)


def execution_arn(func):
    from functools import wraps

    @optgroup.group(
        "Identifier",
        cls=RequiredMutuallyExclusiveOptionGroup,
        help="Identifer type and value to get execution",
    )
    @optgroup.option(
        "--arn",
        help="Execution ARN",
    )
    @optgroup.option(
        "--payload-id",
        help="payload ID (resolves to latest execution ARN)",
    )
    @wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapper


def raw_option(func):
    from functools import wraps

    @click.option(
        "-r",
        "--raw",
        is_flag=True,
        help="Do not pretty-format the response",
    )
    @wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapper


def _get_state(deployment, payload_id):
    from cirrus.lib2.statedb import StateDB

    statedb = StateDB(
        table_name=deployment.env["CIRRUS_STATE_DB"],
        session=deployment.get_session(),
    )
    return statedb.get_dbitem(payload_id)


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
        ctx.obj = Deployment.from_dir(deployment, project)
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
    click.secho(f"Deployment Name: {deployment.name}", fg=color)
    click.secho("Info:", fg=color)
    for k, v in deployment.meta.items():
        click.secho(f"  {k}: {v}", fg=color)
    click.secho("Environment Variables:", fg=color)
    for k, v in deployment.env.items():
        click.secho(f"  {k}: {v}", fg=color)


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
    import io

    from cirrus.lib2.statedb import StateDB

    # TODO: error handling
    bucket, key = StateDB.payload_id_to_bucket_key(
        payload_id,
        payload_bucket=deployment.env["CIRRUS_PAYLOAD_BUCKET"],
    )
    logger.debug("bucket: '%s', key: '%s'", bucket, key)

    session = deployment.get_session()
    s3 = session.client("s3")

    def download(output_fileobj):
        try:
            s3.download_fileobj(bucket, key, output_fileobj)
        except s3.exceptions.ClientError as e:
            # TODO: understand why this is a ClientError even
            #   when it seems like it should be a NoKeyError
            logger.error(e)
            sys.exit(1)

    if raw:
        download(sys.stdout.buffer)
    else:
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
    if payload_id:
        arn = _get_state(deployment, payload_id)["executions"][0]

    sfn = deployment.get_session().client("stepfunctions")
    resp = sfn.describe_execution(executionArn=arn)
    if raw:
        click.echo(resp)
    else:
        click.echo(json.dumps(resp, indent=4, default=str))


@manage.command("get-execution-input")
@execution_arn
@raw_option
@pass_deployment
def get_execution_input(deployment, arn, payload_id, raw):
    """Get a workflow execution's input payload using its ARN or its input payload ID"""
    if payload_id:
        arn = _get_state(deployment, payload_id)["executions"][0]

    sfn = deployment.get_session().client("stepfunctions")
    resp = json.loads(sfn.describe_execution(executionArn=arn)["input"])
    if raw:
        click.echo(resp)
    else:
        click.echo(json.dumps(resp, indent=4, default=str))


@manage.command("get-execution-output")
@execution_arn
@raw_option
@pass_deployment
def get_execution_output(deployment, arn, payload_id, raw):
    """Get a workflow execution's output payload using its ARN or its input payload ID"""
    if payload_id:
        arn = _get_state(deployment, payload_id)["executions"][0]

    sfn = deployment.get_session().client("stepfunctions")
    resp = json.loads(sfn.describe_execution(executionArn=arn)["output"])
    if raw:
        click.echo(resp)
    else:
        click.echo(json.dumps(resp, indent=4, default=str))


@manage.command("get-state")
@click.argument(
    "payload-id",
)
@pass_deployment
def get_state(deployment, payload_id):
    """Get the statedb record for a payload ID"""
    item = _get_state(deployment, payload_id)

    if item:
        click.echo(json.dumps(item, indent=4))
    else:
        logger.error("No item found")


@manage.command()
@pass_deployment
def process(deployment):
    """Enqueue a payload (from stdin) for processing"""
    # add two to account for EOF and needing to know if greater than max length
    payload = sys.stdin.read(MAX_SQS_MESSAGE_LENGTH + 2)

    if len(payload.encode("utf-8")) > MAX_SQS_MESSAGE_LENGTH:
        import uuid

        sys.stdin.buffer.seek(0)
        bucket = deployment.env["CIRRUS_PAYLOAD_BUCKET"]
        key = f"payloads/{uuid.uuid1()}.json"
        url = f"s3://{bucket}/{key}"
        logger.warning("Message exceeds SQS max length.")
        logger.warning("Uploading to '%s'", url)
        s3 = deployment.get_session().client("s3")
        s3.upload_fileobj(sys.stdin.buffer, bucket, key)
        payload = json.dumps({"url": url})

    sqs = deployment.get_session().client("sqs")
    resp = sqs.send_message(
        QueueUrl=deployment.env["CIRRUS_PROCESS_QUEUE_URL"],
        MessageBody=payload,
    )

    click.echo(json.dumps(resp, indent=4))


@manage.command("template-payload")
@click.argument(
    "additional_variable_files",
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
@pass_deployment
def template_payload(
    deployment,
    additional_variable_files,
    additional_vars,
    silence_templating_errors,
):
    """Template a payload using a deployment's vars"""
    from cirrus.cli.payload import template_payload

    _vars = deployment.env.copy()
    for f in additional_variable_files:
        _vars.update(load_env_file(f))

    click.echo(
        template_payload(
            sys.stdin.read(), _vars, silence_templating_errors, **dict(additional_vars)
        )
    )


# check-pipeline
#   - this is like failmgr check
#   - not sure how to reconcile with cache above
#   - maybe need subcommand for everything it can do
