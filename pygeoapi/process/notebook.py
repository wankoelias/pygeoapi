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

import logging
from pathlib import Path
import re
from typing import Dict, Tuple
import urllib.parse

from kubernetes import client as k8s_client, config as k8s_config

from pygeoapi.process.manager.kubernetes import KubernetesProcessor

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

    def create_job_pod_spec(
        self,
        data: Dict,
        user_uuid: str,
    ) -> Tuple[k8s_client.V1PodSpec, Dict]:
        notebook_path = data["notebook"]
        parameters = data["parameters"]
        job_name = "job-notebook"

        home = Path("/home/jovyan")

        # TODO: configurable
        home_pvc = "edc-dev-jupyter-user"

        # TODO: from env
        home_subpath = f"users/{user_uuid}".replace("-", "-2d")

        # TODO: fetch these values from profile (will require profile selection in future)
        image = "eurodatacube/jupyter-user:0.20.1"
        kernel = "edc"
        cpu_limit = "2"
        memory_limit = "4G"

        # TODO: include date(time)
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

        return (
            k8s_client.V1PodSpec(
                restart_policy="Never",
                containers=[container],
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
                    + urllib.parse.quote(output_notebook),
                ),
            },
        )

    def __repr__(self):
        return "<PapermillNotebookKubernetesProcessor> {}".format(self.name)
