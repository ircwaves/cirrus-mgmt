import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from . import exceptions
from .utils.boto3 import get_mfa_session, validate_session

logger = logging.getLogger(__name__)

DEFAULT_DEPLOYMENTS_DIR_NAME = "deployments"
MAX_SQS_MESSAGE_LENGTH = 2**18  # max length of SQS message


def deployments_dir_from_project(project):
    _dir = project.dot_dir.joinpath(DEFAULT_DEPLOYMENTS_DIR_NAME)
    _dir.mkdir(exist_ok=True)
    return _dir


def now_isoformat():
    return datetime.now(timezone.utc).isoformat()


def _maybe_use_buffer(fileobj):
    return fileobj.buffer if hasattr(fileobj, "buffer") else fileobj


class Deployment:
    def __init__(
        self,
        path: Path,
        meta: dict = None,
    ):
        self.path = path
        self.meta = meta if meta else json.loads(path.read_text())

        self.name = self.meta["name"]
        self.stackname = self.meta["stackname"]
        self.profile = self.meta["profile"]
        self.env = self.meta["environment"]
        self.user_vars = self.meta.get("user_vars", {})

        self._session = None

    @classmethod
    def create(cls, name: str, project, stackname: str = None, profile: str = None):
        if not stackname:
            stackname = project.config.get_stackname(name)

        env = cls.get_env_from_lambda(stackname, cls._get_session(profile))

        now = now_isoformat()
        meta = {
            "name": name,
            "created": now,
            "updated": now,
            "stackname": stackname,
            "profile": profile,
            "environment": env,
        }

        path = cls.get_path_from_project(project, name)
        self = cls(path, meta)
        self.save()

        return self

    @classmethod
    def from_name(cls, name: str, project):
        path = cls.get_path_from_project(project, name)
        if not path.is_file():
            raise exceptions.DeploymentNotFoundError(name)
        return cls(path)

    @classmethod
    def remove(cls, name: str, project):
        cls.get_path_from_project(project, name).unlink(missing_ok=True)

    @staticmethod
    def yield_deployments(project):
        for f in deployments_dir_from_project(project).glob("*.json"):
            if f.is_file():
                try:
                    yield json.loads(f.read_text())["name"]
                except Exception:
                    logger.exception("failed on %s", f)
                    pass

    @staticmethod
    def get_path_from_project(project, name: str):
        return deployments_dir_from_project(project).joinpath(f"{name}.json")

    @staticmethod
    def _get_session(profile: str = None):
        # TODO: MFA session should likely be used only with the cli,
        #   so this probably needs to be parameterized by the caller
        # Likely we need a Session class wrapping the boto3 session
        # object that caches clients. That would be useful in the lib generally.
        return validate_session(get_mfa_session(profile=profile), profile)

    @staticmethod
    def get_env_from_lambda(stackname: str, session):
        aws_lambda = session.client("lambda")

        try:
            process_conf = aws_lambda.get_function_configuration(
                FunctionName=f"{stackname}-process",
            )
        except aws_lambda.exceptions.ResourceNotFoundException:
            # TODO: fatal error bad lambda name, needs better handling
            raise

        return process_conf["Environment"]["Variables"]

    def get_session(self):
        if not self._session:
            self._session = self._get_session(profile=self.profile)
        return self._session

    def refresh(self, stackname: str = None, profile: str = None):
        self.stackname = stackname if stackname else self.stackname
        self.profile = profile if profile else self.profile
        self.env = self.get_env_from_lambda(self.stackname, self.get_session())
        self.meta["updated"] = now_isoformat()
        self.save()

    def set_env(self, include_user_vars=False):
        os.environ.update(self.env)
        if include_user_vars:
            os.environ.update(self.user_vars)
        os.environ["AWS_PROFILE"] = self.profile

    def add_user_vars(self, _vars, save=False):
        self.user_vars.update(_vars)
        if save:
            self.save()

    def del_user_var(self, name, save=False):
        try:
            del self.user_vars[name]
        except KeyError:
            pass
        if save:
            self.save()

    def save(self):
        self.path.write_text(json.dumps(self.meta, indent=4))

    def exec(self, command, include_user_vars=True, isolated=False):
        import os

        if isolated:
            env = self.env.copy()
            if include_user_vars:
                env.update(self.user_vars)
            os.execlpe(command[0], *command, env)

        self.set_env(include_user_vars=include_user_vars)
        os.execlp(command[0], *command)

    def get_payload_state(self, payload_id):
        from cirrus.lib2.statedb import StateDB

        statedb = StateDB(
            table_name=self.env["CIRRUS_STATE_DB"],
            session=self.get_session(),
        )
        state = statedb.get_dbitem(payload_id)
        if not state:
            raise exceptions.PayloadNotFoundError(payload_id)

    def process_payload(self, payload):
        stream = None

        if hasattr(payload, "read"):
            stream = _maybe_use_buffer(payload)
            # add two to account for EOF and needing to know
            # if greater than not just equal tomax length
            payload = payload.read(MAX_SQS_MESSAGE_LENGTH + 2)

        if len(payload.encode("utf-8")) > MAX_SQS_MESSAGE_LENGTH:
            import uuid

            stream.seek(0)
            bucket = self.env["CIRRUS_PAYLOAD_BUCKET"]
            key = f"payloads/{uuid.uuid1()}.json"
            url = f"s3://{bucket}/{key}"
            logger.warning("Message exceeds SQS max length.")
            logger.warning("Uploading to '%s'", url)
            s3 = self.get_session().client("s3")
            s3.upload_fileobj(stream, bucket, key)
            payload = json.dumps({"url": url})

        sqs = self.get_session().client("sqs")
        return sqs.send_message(
            QueueUrl=self.env["CIRRUS_PROCESS_QUEUE_URL"],
            MessageBody=payload,
        )

    def get_payload_by_id(self, payload_id, output_fileobj):
        from cirrus.lib2.statedb import StateDB

        # TODO: error handling
        bucket, key = StateDB.payload_id_to_bucket_key(
            payload_id,
            payload_bucket=self.env["CIRRUS_PAYLOAD_BUCKET"],
        )
        logger.debug("bucket: '%s', key: '%s'", bucket, key)

        s3 = self.get_session.client("s3")

        return s3.download_fileobj(bucket, key, output_fileobj)

    def get_execution(self, arn):
        sfn = self.get_session().client("stepfunctions")
        return sfn.describe_execution(executionArn=arn)

    def get_execution_by_payload_id(self, payload_id):
        execs = self.get_payload_state(payload_id).get("executions", [])
        try:
            exec_arn = execs[0]
        except IndexError:
            raise exceptions.NoExecutionsError(payload_id)

        return self.get_execution(exec_arn)

    def template_payload(
        self,
        payload: str,
        additional_vars: dict = None,
        silence_templating_errors: bool = False,
        include_user_vars: bool = True,
    ):
        from .utils.templating import template_payload

        _vars = self.env.copy()
        if include_user_vars:
            _vars.update(self.user_vars)

        return template_payload(
            payload, _vars, silence_templating_errors, **dict(additional_vars)
        )
