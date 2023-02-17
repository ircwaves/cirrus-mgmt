import json
from pprint import pformat
from time import sleep, time_ns
from typing import Optional
import logging


from .deployment import Deployment
from cirrus.lib2.process_payload import ProcessPayload

logger = logging.getLogger(__name__)

CIRRUS_END_STATES = ("COMPLETED", "FAILED", "ABORTED")
_POLL_INTERVAL = 5  # seconds


def run(
    deployment: Deployment,
    payload_path: str,
    force: bool = False,
    out_path: Optional[str] = None,
) -> dict:
    """
    Manage the process of running of a workflow:

    1. submit the payload/fixture

    2. poll the cirrus API until complete

    3. pull final payload result (or return last error encountered

    Args:
        deployment (Deployment): where the workflow will be run.

        payload_path (str): path to payload to pass to the deployment to kick off the workflow.

        out_path (Optional[str]): - path to write the output or error message to.

    Returns:
        dict contaiining output payload or error message

    """
    with open(payload_path) as infile:
        payload = ProcessPayload(json.load(infile))

    if force:
        payload["id"] += f"_force-{time_ns()}"
    wf_id = payload["id"]
    logger.info("Submitting %s to %s", wf_id, deployment.name)
    resp = deployment.process_payload(json.dumps(payload))
    logging.debug(pformat({"sqs response": resp}))

    state = "INIT"
    while state not in CIRRUS_END_STATES:
        sleep(_POLL_INTERVAL)
        resp = deployment.get_payload_state(wf_id)
        state = resp["state_updated"].split("_")[0]
        logging.debug(pformat({"state": state}))

    execution = deployment.get_execution_by_payload_id(wf_id)

    if state == "COMPLETED":
        output = dict(ProcessPayload.from_event(json.loads(execution["output"])))
    else:
        output = {"last_error": resp.get("last_error", "last error not recorded")}

    if out_path is not None:
        with open(out_path, "w", encoding="utf-8") as ofile:
            json.dump(output, ofile, indent=2, sort_keys=True)
            ofile.write("\n")

    return output
