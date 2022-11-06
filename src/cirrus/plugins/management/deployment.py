import json
import os
import shlex
from datetime import datetime, timezone
from pathlib import Path

from .utils.boto3 import get_mfa_session, validate_session

DEFAULT_DEPLYOMENTS_DIR_NAME = "deployments"


def load_env_file(path: Path):
    env = {}

    def load(flike):
        for line in flike.readlines():
            name, val = line.split("=")
            val = shlex.split(val)

            if len(val) != 1:
                raise ValueError(f"Malformed env file: {path}")

            env[name] = val[0]

    if hasattr(path, "open"):
        with path.open() as f:
            load(f)
    elif hasattr(path, "readlines"):
        load(path)
    else:
        raise TypeError(f"Cannot load env file: {path}")

    return env


def write_env_file(path: Path, env):
    with path.open("w") as f:
        for name, val in env.items():
            f.write(f"{name}={shlex.quote(val)}\n")


def deployments_dir_from_project(project):
    _dir = project.dot_dir.joinpath(DEFAULT_DEPLYOMENTS_DIR_NAME)
    _dir.mkdir(exist_ok=True)
    return _dir


def now_isoformat():
    return datetime.now(timezone.utc).isoformat()


class Deployment:
    def __init__(
        self,
        path: Path,
        meta: dict,
        env: dict,
    ):
        self.path = path
        self.name = path.name
        self.meta = meta
        self.env = env

        self.stackname = meta["stackname"]
        self.profile = meta["profile"]

        self._session = None

    @classmethod
    def create(cls, name: str, project, stackname: str = None, profile: str = None):
        if not stackname:
            stackname = project.config.get_stackname(name)

        now = now_isoformat()
        meta = {
            "created": now,
            "updated": now,
            "stackname": stackname,
            "profile": profile,
        }

        path = cls.get_path_from_project(project, name)
        env = cls.get_env_from_lambda(stackname, cls._get_session(profile))
        self = cls(path, meta, env)
        self._persist()

        return self

    @classmethod
    def from_dir(cls, name: str, project):
        path = cls.get_path_from_project(project, name)

        return cls(
            path,
            json.loads(path.joinpath(f"{name}.json").read_text()),
            load_env_file(path.joinpath("env")),
        )

    @classmethod
    def remove(cls, project, name: str):
        import shutil

        shutil.rmtree(
            cls.get_path_from_project(project, name),
            ignore_errors=True,
        )

    @staticmethod
    def yield_deployment_dirs(project):
        for f in deployments_dir_from_project(project).iterdir():
            if f.is_dir():
                yield f

    @staticmethod
    def get_path_from_project(project, name: str):
        return deployments_dir_from_project(project).joinpath(name)

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
        self._persist()

    def set_env(self):
        os.environ.update(self.env)

    def _persist(self):
        self.path.mkdir(exist_ok=True)
        self.path.joinpath(f"{self.name}.json").write_text(
            json.dumps(self.meta, indent=4)
        )
        write_env_file(self.path.joinpath("env"), self.env)
