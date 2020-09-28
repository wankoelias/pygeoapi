# =================================================================
#
# Authors: Bernhard Mallinger <bernhard.mallinger@eox.at>
#
# Copyright (c) 2020 Bernhard Mallinger
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# =================================================================

import enum
import logging
from pathlib import Path
import re
import time

from kubernetes import client as k8s_client, config as k8s_config

from pygeoapi.process.base import BaseProcessor

LOGGER = logging.getLogger(__name__)

#: Process metadata and description
PROCESS_METADATA = {
    "version": "0.1.0",
    "id": "notebook",
    "title": "Process notebooks on kubernetes with papermill",
    "description": "",
    "keywords": ["notebook"],
    "links": [
        {
            "type": "text/html",
            "rel": "canonical",
            "title": "eurodatacube",
            "href": "https://eurodatacube.com",
            "hreflang": "en-US",
        }
    ],
    "inputs": [
        {
            "id": "notebook",
            "title": "notebook file",
            "abstract": "notebook file",
            "input": {
                "literalDataDomain": {
                    "dataType": "string",
                    "valueDefinition": {"anyValue": True},
                }
            },
            "minOccurs": 1,
            "maxOccurs": 1,
            "metadata": None,  # TODO how to use?
            "keywords": ["notebook"],
        },
        {
            "id": "parameters",
            "title": "parameters",
            "abstract": "parameters for notebook execution.",
            "input": {
                "literalDataDomain": {
                    "dataType": "string",
                    "valueDefinition": {"anyValue": True},
                }
            },
            "minOccurs": 0,
            "maxOccurs": 1,
            "metadata": None,
            "keywords": ["message"],
        },
    ],
    "outputs": [
        {
            "id": "link_to_result_notebook",
            "title": "Link to result notebook",
            "description": "Link to result notebook",
            "output": {"formats": [{"mimeType": "text/plain"}]},
        }
    ],
    "example": {},
}


class JobStatus(enum.Enum):
    succeeded = "succeeded"
    failed = "failed"
    pending = "pending"
    running = "running"

    @classmethod
    def from_k8s_library(cls, status: k8s_client.V1JobStatus):
        # we assume only 1 run without retries

        # these "integers" are None if they are 0, lol
        if status.succeeded is not None and status.succeeded > 0:
            return cls.succeeded
        elif status.failed is not None and status.failed > 0:
            return cls.failed
        elif status.active is not None and status.active > 0:
            return cls.running
        else:
            return cls.pending


# TODO: job management in k8s, not in tinydb


class PapermillNotebookKubernetesProcessor(BaseProcessor):
    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)

    def execute(self, data):
        value = "papermill {}!".format("yeah")
        outputs = [{"id": "link_to_result_notebook", "value": value}]
        LOGGER.critical("got data %s", data)

        job = create_notebook_job(
            notebook_path=data["notebook"], parameters=data["parameters"]
        )

        namespace = current_namespace()

        # TODO: only load once
        k8s_config.load_incluster_config()

        batch_v1 = k8s_client.BatchV1Api()
        batch_v1.create_namespaced_job(body=job, namespace=namespace)

        LOGGER.info("Add job %s in ns %s", job.metadata.name, namespace)

        while True:
            # TODO: investigate if list_namespaced_job(watch=True) can be used here
            time.sleep(2)
            job_status = JobStatus.from_k8s_library(
                batch_v1.read_namespaced_job_status(
                    name=job.metadata.name, namespace=namespace
                ).status
            )

            LOGGER.debug("waiting for job %s", job_status)

            if job_status not in (JobStatus.running, JobStatus.pending):
                break

        LOGGER.info("job finished: %s", job_status)

        return outputs

    def __repr__(self):
        return "<PapermillNotebookKubernetesProcessor> {}".format(self.name)


def create_notebook_job(
    notebook_path: str,
    parameters: str,
) -> k8s_client.V1Job:
    job_name = "job-notebook"

    home = Path("/home/jovyan")

    # TODO: configurable
    home_pvc = "edc-dev-jupyter-user"

    # TODO: from env
    home_subpath = "users/0c0a0ef4-2d8d0d-2d4e70-2db002-2d033c7cbb41fd"

    # TODO: fetch these values from profile (will require profile selection in future)
    image = "eurodatacube/jupyter-user:0.20.1"
    kernel = "edc"
    cpu_limit = "2"
    memory_limit = "4G"

    output_notebook = re.sub(".ipynb$", "", notebook_path) + "_result.ipynb"

    container = k8s_client.V1Container(
        name=job_name,
        image=image,
        command=[
            "bash",
            "-c",
            f"/opt/conda/envs/*/bin/papermill "
            f'"{notebook_path}" '
            f'"{output_notebook}" '
            f'-b "{parameters}" '
            f"-k {kernel}",
        ],
        working_dir=str(home),
        volume_mounts=[
            k8s_client.V1VolumeMount(
                mount_path=str(home), name="home", sub_path=home_subpath
            ),
        ],
        resources=k8s_client.V1ResourceRequirements(
            limits={
                "cpu": cpu_limit,
                "memory": memory_limit,
            }
        ),
        env=[],
    )

    volumes = [
        k8s_client.V1Volume(
            persistent_volume_claim=k8s_client.V1PersistentVolumeClaimVolumeSource(
                claim_name=home_pvc,
            ),
            name="home",
        ),
    ]

    return k8s_client.V1Job(
        api_version="batch/v1",
        kind="Job",
        metadata=k8s_client.V1ObjectMeta(name=job_name),
        spec=k8s_client.V1JobSpec(
            template=k8s_client.V1PodTemplateSpec(
                spec=k8s_client.V1PodSpec(
                    restart_policy="Never",
                    containers=[container],
                    volumes=volumes,
                ),
            ),
            backoff_limit=0,
            ttl_seconds_after_finished=60 * 60 * 24 * 7,
        ),
    )


def current_namespace() -> str:
    # getting the current namespace like this is documented here, so it should be fine:
    # https://kubernetes.io/docs/tasks/access-application-cluster/access-cluster/
    return open("/var/run/secrets/kubernetes.io/serviceaccount/namespace").read()