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

from datetime import datetime
import logging
from pathlib import PurePath
import re
from typing import Dict, Optional, Tuple
import urllib.parse

from kubernetes import client as k8s_client

from pygeoapi.process.manager.kubernetes import KubernetesProcessor

LOGGER = logging.getLogger(__name__)


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
        self.default_image = processor_def["default_image"]
        self.s3_bucket_name = processor_def["s3_bucket_name"]

    def create_job_pod_spec(
        self,
        data: Dict,
        user_uuid: str,
        s3_bucket_config: Optional[KubernetesProcessor.S3BucketConfig],
    ) -> Tuple[k8s_client.V1PodSpec, Dict]:
        LOGGER.debug("Starting job with data %s", data)
        notebook_path = data["notebook"]
        parameters = data["parameters"]
        job_name = "job-notebook"

        home = PurePath("/home/jovyan")

        # TODO: allow override from parameter
        image = self.default_image

        # be a bit smart to select kernel (this should do for now)
        is_gpu = image.split(":")[0].endswith("-g")
        kernel = "edc-gpu" if is_gpu else "edc"

        filename_without_postfix = re.sub(".ipynb$", "", notebook_path)
        now_formatted = datetime.now().strftime("%Y%m%d-%H%M%S-%f")
        output_notebook = filename_without_postfix + f"_result_{now_formatted}.ipynb"

        resources = k8s_client.V1ResourceRequirements(
            limits=drop_none_values(
                {
                    "cpu": data.get("cpu_limit"),
                    "memory": data.get("mem_limit"),
                }
            ),
            requests=drop_none_values(
                {
                    "cpu": data.get("cpu_requests"),
                    "memory": data.get("mem_requests"),
                }
            ),
        )

        abs_notebook_path = (
            PurePath(notebook_path)
            if PurePath(notebook_path).is_absolute()
            else (home / notebook_path)
        )
        working_dir = str(abs_notebook_path.parent)

        extra_containers, extra_volume_mounts, extra_volumes = [], [], []

        if s3_bucket_config:
            s3_user_bucket_volume_name = "s3-user-bucket"
            extra_volume_mounts.append(
                k8s_client.V1VolumeMount(
                    mount_path="/home/jovyan/s3",
                    name=s3_user_bucket_volume_name,
                    mount_propagation="HostToContainer",
                )
            )
            extra_volumes.append(
                k8s_client.V1Volume(
                    name=s3_user_bucket_volume_name,
                    empty_dir=k8s_client.V1EmptyDirVolumeSource(),
                )
            )
            extra_containers.append(
                k8s_client.V1Container(
                    name="s3mounter",
                    image="totycro/s3fs:0.4.0-1.86",
                    # we need to detect the end of the job here, this container
                    # must end for the job to be considered done by k8s
                    # 'papermill' is the comm name of the process
                    args=[
                        "sh",
                        "-c",
                        'echo "`date` waiting for job start"; '
                        "sleep 5; "
                        'echo "`date` job start assumed"; '
                        "while pgrep -x papermill > /dev/null; do sleep 1; done; "
                        'echo "`date` job end detected"; ',
                    ],
                    security_context=k8s_client.V1SecurityContext(privileged=True),
                    volume_mounts=[
                        k8s_client.V1VolumeMount(
                            name=s3_user_bucket_volume_name,
                            mount_path="/opt/s3fs/bucket",
                            mount_propagation="Bidirectional",
                        ),
                    ],
                    resources=k8s_client.V1ResourceRequirements(
                        limits={"cpu": "0.1", "memory": "128Mi"},
                        requests={
                            "cpu": "0.05",
                            "memory": "32Mi",
                        },
                    ),
                    env=[
                        k8s_client.V1EnvVar(name="S3FS_ARGS", value="-oallow_other"),
                        k8s_client.V1EnvVar(name="UID", value="1000"),
                        k8s_client.V1EnvVar(name="GID", value="2014"),
                        k8s_client.V1EnvVar(
                            name="AWS_S3_ACCESS_KEY_ID",
                            value_from=k8s_client.V1EnvVarSource(
                                secret_key_ref=k8s_client.V1SecretKeySelector(
                                    name=s3_bucket_config.secret_name,
                                    key="username",
                                )
                            ),
                        ),
                        k8s_client.V1EnvVar(
                            name="AWS_S3_SECRET_ACCESS_KEY",
                            value_from=k8s_client.V1EnvVarSource(
                                secret_key_ref=k8s_client.V1SecretKeySelector(
                                    name=s3_bucket_config.secret_name,
                                    key="password",
                                )
                            ),
                        ),
                        k8s_client.V1EnvVar(
                            "AWS_S3_BUCKET",
                            self.s3_bucket_name,
                        ),
                        # due to the shared process namespace, tini is not PID 1, so:
                        k8s_client.V1EnvVar(name="TINI_SUBREAPER", value="1"),
                        k8s_client.V1EnvVar(
                            name="AWS_S3_URL",
                            value="https://s3-eu-central-1.amazonaws.com",
                        ),
                    ],
                )
            )

        notebook_container = k8s_client.V1Container(
            name=job_name,
            image=image,
            command=[
                "bash",
                "-c",
                f"/opt/conda/envs/*/bin/papermill "
                f'"{notebook_path}" '
                f'"{output_notebook}" '
                f"-k {kernel} " + (f'-b "{parameters}" ' if parameters else ""),
            ],
            working_dir=working_dir,
            volume_mounts=[
                k8s_client.V1VolumeMount(
                    mount_path=str(home),
                    name="home",
                ),
            ]
            + extra_volume_mounts,
            resources=resources,
            env=[
                # this is provided in jupyter worker containers and we also use it
                # for compatibility checks
                k8s_client.V1EnvVar(name="JUPYTER_IMAGE", value=image),
            ],
        )

        volumes = [
            k8s_client.V1Volume(
                persistent_volume_claim=k8s_client.V1PersistentVolumeClaimVolumeSource(
                    claim_name="user"
                ),
                name="home",
            ),
        ] + extra_volumes

        return (
            k8s_client.V1PodSpec(
                restart_policy="Never",
                containers=[notebook_container] + extra_containers,
                volumes=volumes,
                # we need this to be able to terminate the sidecar container
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


def drop_none_values(d: Dict) -> Dict:
    return {k: v for k, v in d.items() if v is not None}
