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
        notebook_path = data["notebook"]
        parameters = data["parameters"]
        job_name = "job-notebook"

        home = Path("/home/jovyan")

        home_subpath = user_uuid.replace("-", "-2d")

        profile = retrieve_profile(user_uuid=user_uuid, user_email=user_email)

        # be a bit smart to select kernel (this should do for now)
        is_gpu = profile.image.split(":")[0].endswith("-g")
        kernel = "edc-gpu" if is_gpu else "edc"

        filename_without_postfix = re.sub(".ipynb$", "", notebook_path)
        now_formatted = datetime.now().strftime("%y%m%d-%H%M%S")
        output_notebook = filename_without_postfix + f"_result_{now_formatted}.ipynb"

        notebook_container = k8s_client.V1Container(
            name=job_name,
            image=profile.image,
            command=[
                "bash",
                "-c",
                f"/opt/conda/envs/*/bin/papermill "
                f'"{notebook_path}" '
                f'"{output_notebook}" '
                + (f'-b "{parameters}" ' if parameters else "") +
                f"-k {kernel}",
            ],
            working_dir=str(home),
            volume_mounts=[
                k8s_client.V1VolumeMount(
                    mount_path=str(home),
                    name="home",
                    mount_propagation="HostToContainer",
                ),
            ],
            resources=k8s_client.V1ResourceRequirements(
                limits={"cpu": profile.cpu_limit, "memory": profile.mem_limit},
                requests={
                    "cpu": profile.cpu_guarantee,
                    "memory": profile.mem_guarantee,
                },
            ),
            env=[],
        )
        nfs_mounter_container = k8s_client.V1Container(
            name="nfs-mounter",
            image="totycro/nfs-client:0.2.1",
            security_context=k8s_client.V1SecurityContext(privileged=True),
            # 'papermill' is the comm name of the process
            args=[
                "sh",
                "-c",
                "while ! pgrep -x papermill > /dev/null; do sleep 1; done; "
                "echo 'job start detected'; "
                "while pgrep -x papermill > /dev/null; do sleep 1; done; "
                "echo 'job end detected'; ",
            ],
            volume_mounts=[
                k8s_client.V1VolumeMount(
                    name="home",
                    mount_path="/mnt",
                    mount_propagation="Bidirectional",
                ),
            ],
            env=[
                k8s_client.V1EnvVar(
                    name="MOUNT_TARGET",
                    value=f"{self.nfs_server}:{self.nfs_share}/{home_subpath}",
                ),
                k8s_client.V1EnvVar(name="MOUNT_POINT", value="/mnt"),
                k8s_client.V1EnvVar(
                    name="MOUNT_OPTIONS",
                    value="nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2,noresvport",
                ),
            ],
        )

        volumes = [
            k8s_client.V1Volume(
                empty_dir=k8s_client.V1EmptyDirVolumeSource(),
                name="home",
            ),
        ]

        return (
            k8s_client.V1PodSpec(
                restart_policy="Never",
                containers=[notebook_container, nfs_mounter_container],
                volumes=volumes,
                # NOTE: The nfs client shutdown must succeed, otherwise the pod is
                #       stuck forever. Sometimes something in k8s seems to keep some
                #       resource open for many seconds (30-60?), so keep ample time for
                #       this to timeout./
                termination_grace_period_seconds=600,
                # we need this to be able to terminate the nfs sidecar container
                # https://github.com/kubernetes/kubernetes/issues/25908
                share_process_namespace=True,
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
    mem_limit: str
    mem_guarantee: str
    cpu_limit: str
    cpu_guarantee: str

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
