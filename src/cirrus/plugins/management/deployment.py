import dataclasses
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from subprocess import CalledProcessError, check_call
from time import sleep, time

import backoff
from cirrus.lib2.process_payload import ProcessPayload

from . import exceptions
from .utils.boto3 import get_mfa_session, validate_session

logger = logging.getLogger(__name__)

DEFAULT_DEPLOYMENTS_DIR_NAME = "deployments"
MAX_SQS_MESSAGE_LENGTH = 2**18  # max length of SQS message
CONFIG_VERSION = 0

WORKFLOW_POLL_INTERVAL = 15  # seconds between state checks


def deployments_dir_from_project(project):
    _dir = project.dot_dir.joinpath(DEFAULT_DEPLOYMENTS_DIR_NAME)
    _dir.mkdir(exist_ok=True)
    return _dir


def now_isoformat():
    return datetime.now(timezone.utc).isoformat()


def _maybe_use_buffer(fileobj):
    return fileobj.buffer if hasattr(fileobj, "buffer") else fileobj


@dataclasses.dataclass
class DeploymentMeta:
    name: str
    created: str
    updated: str
    stackname: str
    profile: str
    environment: dict
    user_vars: dict
    config_version: int

    @classmethod
    def load(cls, path: Path):
        config = json.loads(path.read_text())
        if version := config.get("config_version") != CONFIG_VERSION:
            raise exceptions.DeploymentConfigurationError(
                f"Unable to load config version: {version}",
            )
        try:
            return cls(**config)
        except TypeError as e:
            raise exceptions.DeploymentConfigurationError(
                f"Failed to load configuration: {e}",
            )

    def save(self):
        self.path.write_text(self.asjson(indent=4))

    def asdict(self):
        return dataclasses.asdict(self)

    def asjson(self, *args, **kwargs):
        return json.dumps(self.asdict(), *args, **kwargs)


@dataclasses.dataclass
class Deployment(DeploymentMeta):
    def __init__(self, path: Path, *args, **kwargs):
        self.path = path

        super().__init__(*args, **kwargs)

        self._session = None
        self._functions = None

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
            "user_vars": {},
            "config_version": CONFIG_VERSION,
        }

        path = cls.get_path_from_project(project, name)
        self = cls(path, **meta)
        self.save()

        return self

    @classmethod
    def from_file(cls, path: Path):
        return cls(path, **DeploymentMeta.load(path).asdict())

    @classmethod
    def from_name(cls, name: str, project):
        path = cls.get_path_from_project(project, name)
        try:
            return cls.from_file(path)
        except FileNotFoundError:
            raise exceptions.DeploymentNotFoundError(name) from None

    @classmethod
    def remove(cls, name: str, project):
        cls.get_path_from_project(project, name).unlink(missing_ok=True)

    @staticmethod
    def yield_deployments(project):
        for f in deployments_dir_from_project(project).glob("*.json"):
            if f.is_file():
                try:
                    yield DeploymentMeta.load(f).name
                except exceptions.DeploymentConfigurationError:
                    yield f"{f.stem} (invalid configuration)"
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

    def get_lambda_functions(self):
        if self._functions is None:
            aws_lambda = self.get_session().client("lambda")

            def deployment_functions_filter(response):
                return [
                    f["FunctionName"].replace(f"{self.stackname}-", "")
                    for f in response["Functions"]
                    if f["FunctionName"].startswith(self.stackname)
                ]

            resp = aws_lambda.list_functions()
            self._functions = deployment_functions_filter(resp)
            while "NextMarker" in resp:
                resp = aws_lambda.list_functions(Marker=resp["NextMarker"])
                self._functions += deployment_functions_filter(resp)
        return self._functions

    def get_session(self):
        if not self._session:
            self._session = self._get_session(profile=self.profile)
        return self._session

    def reload(self):
        self.__dict__.update(DeploymentMeta.load(self.path).asdict())

    def refresh(self, stackname: str = None, profile: str = None):
        self.stackname = stackname if stackname else self.stackname
        self.profile = profile if profile else self.profile
        self.environment = self.get_env_from_lambda(self.stackname, self.get_session())
        self.updated = now_isoformat()
        self.save()

    def set_env(self, include_user_vars=False):
        os.environ.update(self.environment)
        if include_user_vars:
            os.environ.update(self.user_vars)
        if self.profile:
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

    def exec(self, command, include_user_vars=True, isolated=False):
        import os

        if isolated:
            env = self.environment.copy()
            if include_user_vars:
                env.update(self.user_vars)
            os.execlpe(command[0], *command, env)

        self.set_env(include_user_vars=include_user_vars)
        os.execlp(command[0], *command)

    def call(self, command, include_user_vars=True, isolated=False):
        if isolated:
            env = self.environment.copy()
            if include_user_vars:
                env.update(self.user_vars)
            check_call(command, env=env)
        else:
            self.set_env(include_user_vars=include_user_vars)
            check_call(command)

    def get_payload_state(self, payload_id):
        from cirrus.lib2.statedb import StateDB

        statedb = StateDB(
            table_name=self.environment["CIRRUS_STATE_DB"],
            session=self.get_session(),
        )

        @backoff.on_predicate(backoff.expo, lambda x: x is None, max_time=60)
        def _get_payload_item_from_statedb(statedb, payload_id):
            return statedb.get_dbitem(payload_id)

        state = _get_payload_item_from_statedb(statedb, payload_id)

        if not state:
            raise exceptions.PayloadNotFoundError(payload_id)
        return state

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
            bucket = self.environment["CIRRUS_PAYLOAD_BUCKET"]
            key = f"payloads/{uuid.uuid1()}.json"
            url = f"s3://{bucket}/{key}"
            logger.warning("Message exceeds SQS max length.")
            logger.warning("Uploading to '%s'", url)
            s3 = self.get_session().client("s3")
            s3.upload_fileobj(stream, bucket, key)
            payload = json.dumps({"url": url})

        sqs = self.get_session().client("sqs")
        return sqs.send_message(
            QueueUrl=self.environment["CIRRUS_PROCESS_QUEUE_URL"],
            MessageBody=payload,
        )

    def get_payload_by_id(self, payload_id, output_fileobj):
        from cirrus.lib2.statedb import StateDB

        # TODO: error handling
        bucket, key = StateDB.payload_id_to_bucket_key(
            payload_id,
            payload_bucket=self.environment["CIRRUS_PAYLOAD_BUCKET"],
        )
        logger.debug("bucket: '%s', key: '%s'", bucket, key)

        s3 = self.get_session().client("s3")

        return s3.download_fileobj(bucket, key, output_fileobj)

    def get_execution(self, arn):
        sfn = self.get_session().client("stepfunctions")
        return sfn.describe_execution(executionArn=arn)

    def get_execution_by_payload_id(self, payload_id):
        execs = self.get_payload_state(payload_id).get("executions", [])
        try:
            exec_arn = execs[-1]
        except IndexError:
            raise exceptions.NoExecutionsError(payload_id)

        return self.get_execution(exec_arn)

    def invoke_lambda(self, event, function_name):
        aws_lambda = self.get_session().client("lambda")
        if function_name not in self.get_lambda_functions():
            raise ValueError(
                f"lambda named '{function_name}' not found in deployment '{self.name}'"
            )
        full_name = f"{self.stackname}-{function_name}"
        response = aws_lambda.invoke(FunctionName=full_name, Payload=event)
        if response["StatusCode"] < 200 or response["StatusCode"] > 299:
            raise RuntimeError(response)

        return json.load(response["Payload"])

    def run_workflow(
        self,
        payload: dict,
        timeout: int = 3600,
        poll_interval: int = WORKFLOW_POLL_INTERVAL,
    ) -> dict:
        """

        Args:
            deployment (Deployment): where the workflow will be run.

            payload (str): payload to pass to the deployment to kick off the workflow.

            timeout (Optional[int]): - upper bound on the number of seconds to poll the
                                       deployment before considering the test failed.

            poll_interval (Optional[int]): - seconds to delay between checks of the
                                             workflow status.

        Returns:
            dict containing output payload or error message

        """
        payload = ProcessPayload(payload)
        wf_id = payload["id"]
        logger.info("Submitting %s to %s", wf_id, self.name)
        resp = self.process_payload(json.dumps(payload))
        logger.debug(resp)

        state = "PROCESSING"
        end_time = time() + timeout - poll_interval
        while state == "PROCESSING" and time() < end_time:
            sleep(poll_interval)
            resp = self.get_payload_state(wf_id)
            state = resp["state_updated"].split("_")[0]
            logger.debug({"state": state})

        execution = self.get_execution_by_payload_id(wf_id)

        if state == "COMPLETED":
            output = dict(ProcessPayload.from_event(json.loads(execution["output"])))
        elif state == "PROCESSING":
            output = {"last_error": "Unkonwn: cirrus-mgmt polling timeout exceeded"}
        else:
            output = {"last_error": resp.get("last_error", "last error not recorded")}

        return output

    def template_payload(
        self,
        payload: str,
        additional_vars: dict = None,
        silence_templating_errors: bool = False,
        include_user_vars: bool = True,
    ):
        from .utils.templating import template_payload

        _vars = self.environment.copy()
        if include_user_vars:
            _vars.update(self.user_vars)

        return template_payload(
            payload, _vars, silence_templating_errors, **dict(additional_vars)
        )
