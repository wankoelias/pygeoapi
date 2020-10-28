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

from pygeoapi.process.notebook import PapermillNotebookKubernetesProcessor


def _create_processor(def_override=None) -> PapermillNotebookKubernetesProcessor:
    return PapermillNotebookKubernetesProcessor(
        processor_def={
            "name": "test",
            "s3_bucket_name": "example",
            "default_image": "example",
            **(def_override if def_override else {}),
        }
    )


@pytest.fixture()
def papermill_processor() -> PapermillNotebookKubernetesProcessor:
    return _create_processor()


@pytest.fixture()
def papermill_gpu_processor() -> PapermillNotebookKubernetesProcessor:
    return _create_processor({"default_image": "jupyter-user-g:1.2.3"})


def test_workdir_is_notebook_dir(papermill_processor):
    relative_dir = "a/b"
    nb_path = f"{relative_dir}/a.ipynb"
    abs_dir = f"/home/jovyan/{relative_dir}"

    spec, _ = papermill_processor.create_job_pod_spec(
        data={"notebook": nb_path, "parameters": ""},
        user_uuid="",
        s3_bucket_config=None,
    )

    assert spec.containers[0].working_dir == abs_dir


def test_default_image_has_no_affinity(papermill_processor):
    spec, _ = papermill_processor.create_job_pod_spec(
        data={"notebook": "a", "parameters": ""},
        user_uuid="",
        s3_bucket_config=None,
    )

    assert spec.affinity is None
    assert spec.tolerations is None


def test_gpu_image_has_affinity(papermill_gpu_processor):
    spec, _ = papermill_gpu_processor.create_job_pod_spec(
        data={"notebook": "a", "parameters": ""},
        user_uuid="",
        s3_bucket_config=None,
    )

    sel = (
        spec.affinity.node_affinity.required_during_scheduling_ignored_during_execution
    )
    assert sel.node_selector_terms[0].match_expressions[0].values == ["g2"]
    assert spec.tolerations[0].key == "hub.eox.at/gpu"
