import json

import pytest

from cirrus.plugins.management.deployment import Deployment

DEPLYOMENT_NAME = "test-deployment"
STACK_NAME = "cirrus-test"


@pytest.fixture
def manage(invoke):
    def _manage(cmd):
        return invoke("manage " + cmd)

    return _manage


@pytest.fixture
def deployment_meta(queue, statedb, payloads, data):
    return {
        "name": DEPLYOMENT_NAME,
        "created": "2022-11-07T04:42:26.666916+00:00",
        "updated": "2022-11-07T04:42:26.666916+00:00",
        "stackname": STACK_NAME,
        "profile": None,
        "environment": {
            "CIRRUS_STATE_DB": statedb.table_name,
            # "CIRRUS_PUBLISH_TOPIC_ARN": ,
            "CIRRUS_LOG_LEVEL": "DEBUG",
            "CIRRUS_STACK": STACK_NAME,
            "CIRRUS_DATA_BUCKET": data,
            "CIRRUS_PAYLOAD_BUCKET": payloads,
            "CIRRUS_PROCESS_QUEUE_URL": queue["QueueUrl"],
            # "CIRRUS_INVALID_TOPIC_ARN": ,
            # "CIRRUS_FAILED_TOPIC_ARN": ,
        },
    }


@pytest.fixture
def deployment(manage, project, deployment_meta):
    dep = Deployment(
        path=Deployment.get_path_from_project(project, DEPLYOMENT_NAME),
        meta=deployment_meta,
    )
    dep.save()

    def _manage(cmd):
        return manage(f"{DEPLYOMENT_NAME} {cmd}")

    yield _manage

    Deployment.remove(dep.name, project)


def test_manage(manage):
    result = manage("")
    assert result.exit_code == 0


def test_manage_show_deployment(deployment, deployment_meta):
    result = deployment("show")
    assert result.exit_code == 0
    assert result.stdout.strip() == json.dumps(deployment_meta, indent=4)


def test_manage_show_unknown_deployment(manage, deployment):
    unknown = "unknown-deployment"
    result = manage(f"{unknown} show")
    assert result.exit_code == 1
    assert result.stderr.strip() == f"Deployment not found: {unknown}"
