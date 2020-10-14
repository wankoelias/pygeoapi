# =================================================================
#
# Authors: Bernhard Mallinger <bernhard.mallinger@eox.at>
#
# Copyright (C) 2020 EOX IT Services GmbH <https://eox.at>
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

from __future__ import annotations

from dataclasses import dataclass, fields
from datetime import datetime
import logging
from pathlib import Path
import os
import re
from typing import Dict, Tuple
import urllib.parse

from kubernetes import client as k8s_client
import requests
from requests.models import HTTPBasicAuth

from pygeoapi.process.manager.kubernetes import KubernetesProcessor

LOGGER = logging.getLogger(__name__)

# move to some config file?
customer_operator_auth = HTTPBasicAuth(
    username=os.environ["CUSTOMER_OPERATOR_USER"],
    password=os.environ["CUSTOMER_OPERATOR_PASSWORD"],
)

#: Process metadata and description
PROCESS_METADATA = {
    "version": "0.1.0",
    "id": "execute-notebook",
    "title": "notebooks on kubernetes with papermill",
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
            "title": "notebook file (path relative to home)",
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
            "keywords": [""],
        },
        {
            "id": "parameters",
            "title": "parameters (base64 encoded yaml)",
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
            "keywords": [""],
        },
        {
            "id": "cpu_limit",
            "title": "CPU limit (default: half of your EOxHub limit)",
            "abstract": "Number of CPUs to use for this job",
            "input": {
                "literalDataDomain": {
                    "dataType": "float",
                    "valueDefinition": {"anyValue": True, "uom": "Number of CPUs"},
                }
            },
            "minOccurs": 0,
            "maxOccurs": 1,
            "metadata": None,
            "keywords": [""],
        },
        {
            "id": "mem_limit",
            "title": "Memory limit  in GiB (default: half of your EOxHub limit)",
            "abstract": "Amount of memory to use for this job",
            "input": {
                "literalDataDomain": {
                    "dataType": "float",
                    "valueDefinition": {"anyValue": True, "uom": "GiB"},
                }
            },
            "minOccurs": 0,
            "maxOccurs": 1,
            "metadata": None,
            "keywords": [""],
        },
    ],
    "outputs": [
        {
            "id": "result_link",
            "title": "Link to result notebook",
            "description": "Link to result notebook",
            "output": {"formats": [{"mimeType": "text/plain"}]},
        }
    ],
    "example": {},
}


class PapermillNotebookKubernetesProcessor(KubernetesProcessor):
    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)
        self.nfs_server = processor_def["nfs_server"]
        self.nfs_share = processor_def["nfs_share"]

    def create_job_pod_spec(
        self,
        data: Dict,
        user_uuid: str,
        user_email: str,
    ) -> Tuple[k8s_client.V1PodSpec, Dict]:
        LOGGER.debug("Starting job with data %s", data)
        notebook_path = data["notebook"]
        parameters = data["parameters"]
        job_name = "job-notebook"

        home = Path("/home/jovyan")

        profile = retrieve_profile(user_uuid=user_uuid, user_email=user_email)

        # be a bit smart to select kernel (this should do for now)
        is_gpu = profile.image.split(":")[0].endswith("-g")
        kernel = "edc-gpu" if is_gpu else "edc"

        filename_without_postfix = re.sub(".ipynb$", "", notebook_path)
        now_formatted = datetime.now().strftime("%Y%m%d-%H%M%S-%f")
        output_notebook = filename_without_postfix + f"_result_{now_formatted}.ipynb"

        cpu_limit = data["cpu_limit"] or profile.cpu_limit / 2
        mem_limit = "{}Gi".format(
            data["mem_limit"] or (profile.mem_limit / 2 / (1024 ** 3))
        )

        notebook_container = k8s_client.V1Container(
            name=job_name,
            image=profile.image,
            command=[
                "bash",
                "-c",
                f"/opt/conda/envs/*/bin/papermill "
                f'"{notebook_path}" '
                f'"{output_notebook}" '
                f"-k {kernel} " + (f'-b "{parameters} " ' if parameters else ""),
            ],
            working_dir=str(home),
            volume_mounts=[
                k8s_client.V1VolumeMount(
                    mount_path=str(home),
                    name="home",
                ),
            ],
            resources=k8s_client.V1ResourceRequirements(
                limits={"cpu": cpu_limit, "memory": mem_limit},
                requests={
                    "cpu": cpu_limit,
                    "memory": mem_limit,
                },
            ),
            env=[
                # this is provided in jupyter worker containers and we also use it
                # for compatibility checks
                k8s_client.V1EnvVar(name="JUPYTER_IMAGE", value=profile.image),
            ],
        )

        volumes = [
            k8s_client.V1Volume(
                persistent_volume_claim=k8s_client.V1PersistentVolumeClaimVolumeSource(
                    claim_name="user"
                ),
                name="home",
            ),
        ]

        return (
            k8s_client.V1PodSpec(
                restart_policy="Never",
                containers=[notebook_container],
                volumes=volumes,
            ),
            {
                "result_type": "link",
                "link": (
                    # NOTE: this link currently doesn't work (even those created in
                    #   the ui with "create sharable link" don't)
                    #   there is a recently closed issue about it:
                    # https://github.com/jupyterlab/jupyterlab/issues/8359
                    #   it doesn't say when it was fixed exactly. there's a possibly
                    #   related fix from last year:
                    # https://github.com/jupyterlab/jupyterlab/pull/6773
                    "https://edc-jupyter.hub.eox.at/hub/user-redirect/lab/tree/"
                    + urllib.parse.quote(output_notebook)
                ),
            },
        )

    def __repr__(self):
        return "<PapermillNotebookKubernetesProcessor> {}".format(self.name)


@dataclass(frozen=True)
class EDCProfile:
    image: str
    mem_limit: int
    mem_guarantee: int
    cpu_limit: float
    cpu_guarantee: float

    @classmethod
    def from_superset(cls, d: Dict) -> EDCProfile:
        field_names = [field.name for field in fields(cls)]
        return cls(**{k: v for k, v in d.items() if k in field_names})


def retrieve_profile(user_uuid: str, user_email: str) -> EDCProfile:
    LOGGER.debug(f"Retrieving profiles for {user_uuid} {user_email}")
    profiles = requests.get(
        f"https://customer-operator.dev.hub.eox.at/tenants/{user_uuid}/profiles?"
        f"email={urllib.parse.quote(user_email)}",
        auth=customer_operator_auth,
    ).json()

    LOGGER.debug("Got profiles %s", profiles)

    try:
        profile = profiles[0]
    except KeyError as e:
        raise RuntimeError("No active user profile found") from e

    return EDCProfile.from_superset(profile)
