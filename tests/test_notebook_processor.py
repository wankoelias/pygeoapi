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

import pytest
from typing import Dict

from pygeoapi.process.manager.kubernetes import KubernetesProcessor
from pygeoapi.process.notebook import PapermillNotebookKubernetesProcessor


def _create_processor(def_override=None) -> PapermillNotebookKubernetesProcessor:
    return PapermillNotebookKubernetesProcessor(
        processor_def={
            "name": "test",
            "s3_bucket_name": "example",
            "default_image": "example",
            "extra_pvcs": [],
            "home_volume_claim_name": "user",
            "image_pull_secret": "",
            "jupyter_base_url": "",
            **(def_override if def_override else {}),
        }
    )


@pytest.fixture()
def papermill_processor() -> PapermillNotebookKubernetesProcessor:
    return _create_processor()


@pytest.fixture()
def papermill_gpu_processor() -> PapermillNotebookKubernetesProcessor:
    return _create_processor({"default_image": "jupyter-user-g:1.2.3"})


@pytest.fixture()
def create_pod_kwargs() -> Dict:
    return {
        "data": {"notebook": "a", "parameters": ""},
        "user_uuid": "",
        "s3_bucket_config": None,
    }


def test_workdir_is_notebook_dir(papermill_processor, create_pod_kwargs):
    relative_dir = "a/b"
    nb_path = f"{relative_dir}/a.ipynb"
    abs_dir = f"/home/jovyan/{relative_dir}"

    spec, _ = papermill_processor.create_job_pod_spec(
        data={"notebook": nb_path, "parameters": ""},
        user_uuid="",
        s3_bucket_config=None,
    )

    assert f'--cwd "{abs_dir}"' in str(spec.containers[0].command)


def test_default_image_has_no_affinity(papermill_processor, create_pod_kwargs):
    spec, _ = papermill_processor.create_job_pod_spec(**create_pod_kwargs)

    assert spec.affinity is None
    assert spec.tolerations is None


def test_gpu_image_has_affinity(papermill_gpu_processor, create_pod_kwargs):
    spec, _ = papermill_gpu_processor.create_job_pod_spec(**create_pod_kwargs)

    r = spec.affinity.node_affinity.required_during_scheduling_ignored_during_execution
    assert r.node_selector_terms[0].match_expressions[0].values == ["g2"]
    assert spec.tolerations[0].key == "hub.eox.at/gpu"


def test_no_s3_bucket_by_default(papermill_processor, create_pod_kwargs):
    spec, _ = papermill_processor.create_job_pod_spec(**create_pod_kwargs)
    assert "s3mounter" not in [c.name for c in spec.containers]
    assert "/home/jovyan/s3" not in [
        m.mount_path for m in spec.containers[0].volume_mounts
    ]


def test_s3_bucket_present_when_requested(papermill_processor):
    spec, _ = papermill_processor.create_job_pod_spec(
        data={"notebook": "a", "parameters": ""},
        user_uuid="",
        s3_bucket_config=KubernetesProcessor.S3BucketConfig(secret_name="a"),
    )
    assert "s3mounter" in [c.name for c in spec.containers]
    assert "/home/jovyan/s3" in [m.mount_path for m in spec.containers[0].volume_mounts]


def test_extra_pvcs_are_added_on_request(papermill_processor, create_pod_kwargs):
    claim_name = "my_pvc"
    processor = _create_processor(
        {"extra_pvcs": [{"claim_name": claim_name, "mount_path": "/mnt"}]}
    )
    spec, _ = processor.create_job_pod_spec(**create_pod_kwargs)

    assert claim_name in [v.persistent_volume_claim.claim_name for v in spec.volumes]


def test_image_pull_secr_added_when_requested(papermill_processor, create_pod_kwargs):
    processor = _create_processor({"image_pull_secret": "psrcr"})
    spec, _ = processor.create_job_pod_spec(**create_pod_kwargs)
    assert spec.image_pull_secrets[0].name == 'psrcr'