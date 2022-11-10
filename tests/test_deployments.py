import shlex

import pytest
from cirrus.cli.commands import cli
from cirrus.core import constants
from click.testing import CliRunner

DEPLYOMENT_NAME = "test-deployment"


@pytest.fixture(scope="session")
def cli_runner():
    return CliRunner(mix_stderr=False)


@pytest.fixture(scope="session")
def invoke(cli_runner):
    def _invoke(cmd, **kwargs):
        return cli_runner.invoke(cli, shlex.split(cmd), **kwargs)

    return _invoke


@pytest.fixture
def build_dir(project_testdir):
    return project_testdir.joinpath(
        constants.DEFAULT_DOT_DIR_NAME,
        constants.DEFAULT_BUILD_DIR_NAME,
    )


def test_deployments(invoke):
    result = invoke("deployments")
    assert result.exit_code == 0


def test_deployments_show_no_project(invoke):
    result = invoke("deployments show")
    assert result.exit_code == 1


def test_deployments_show_no_deployments(invoke, project):
    result = invoke("deployments show")
    assert result.exit_code == 0
    assert len(result.stdout) == 0


def test_deployments_add(invoke, project):
    result = invoke(f"deployments add {DEPLYOMENT_NAME}")
    assert result.exit_code == 0


def test_deployments_show(invoke, project):
    result = invoke("deployments show")
    assert result.exit_code == 0
    assert result.stdout.strip() == DEPLYOMENT_NAME


def test_deployments_rm(invoke, project):
    result = invoke(f"deployments rm {DEPLYOMENT_NAME}")
    assert result.exit_code == 0
    result = invoke("deployments show")
    assert result.exit_code == 0
    assert len(result.stdout) == 0


def test_deployments_rm_missing(invoke, project):
    result = invoke(f"deployments rm {DEPLYOMENT_NAME}")
    assert result.exit_code == 0
