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

from functools import cached_property
from http import HTTPStatus
import logging
import re
import time
from typing import Dict, Optional, Tuple

from kubernetes import client as k8s_client, config as k8s_config
import kubernetes.client.rest

from pygeoapi.util import JobStatus
from pygeoapi.process.base import BaseProcessor
from pygeoapi.process.manager.base import BaseManager


LOGGER = logging.getLogger(__name__)


class KubernetesProcessor(BaseProcessor):
    def create_job_pod_spec(self, data: Dict) -> k8s_client.V1PodSpec:
        raise NotImplementedError()

    def execute(self):
        raise NotImplementedError(
            "Kubernetes Processes can't be executed directly, use KubernetesManager"
        )


class KubernetesManager(BaseManager):
    def __init__(self, manager_def: Dict) -> None:
        super().__init__(manager_def)
        try:
            k8s_config.load_kube_config()
        except Exception:
            # load_kube_config might throw anything :/
            k8s_config.load_incluster_config()

        self.batch_v1 = k8s_client.BatchV1Api()

    def get_jobs(self, processid=None, status=None):
        """
        Get jobs

        :param processid: process identifier
        :param status: job status (accepted, running, successful,
                       failed, results) (default is all)

        :returns: list of jobs (identifier, status, process identifier)
        """

        # TODO
        raise NotImplementedError()

    def get_job_result(self, processid, jobid) -> Optional[Dict]:
        """
        Get a single job

        :param processid: process identifier
        :param jobid: job identifier

        :returns: `dict`  # `pygeoapi.process.manager.Job`
        """

        try:
            job: k8s_client.V1Job = self.batch_v1.read_namespaced_job(
                name=k8s_job_name(jobid),
                namespace=self.namespace,
            )
        except kubernetes.client.rest.ApiException as e:
            if e.status == HTTPStatus.NOT_FOUND:
                return None
            else:
                raise
        else:
            metadata_from_annotation = {
                parsed_key: v
                for orig_key, v in job.metadata.annotations.items()
                if (parsed_key := parse_annotation_key(orig_key))
            }
            return {
                **metadata_from_annotation,
                # NOTE: this is passed as string as compatibility with base manager
                "status": job_status_from_k8s(job.status).value,
            }

    def add_job(self, job_metadata):
        """
        Add a job

        :param job_metadata: `dict` of job metadata

        :returns: add job result
        """

        raise NotImplementedError("For k8s, add_job is implied by executing the job")

    def update_job(self, processid, job_id, update_dict):
        """
        Updates a job

        :param processid: process identifier
        :param job_id: job identifier
        :param update_dict: `dict` of property updates

        :returns: `bool` of status result
        """
        # we could update the metadata by changing the job annotations
        raise NotImplementedError("Currently there's no use case for updating k8s jobs")

    def get_job_status(self, p, job_id, data_dict):
        """"""

    def get_job_output(
        self, processid, job_id
    ) -> Tuple[Optional[JobStatus], Optional[Dict]]:
        """
        Returns the actual output from a finished process, or else None if the
        process has not finished execution.

        :param processid: process identifier
        :param job_id: job identifier

        :returns: tuple of: JobStatus `Enum`, and
        """
        result = self.get_job_result(processid=processid, jobid=job_id)

        if result is None:
            # no such job
            return None, None

        job_status = JobStatus[result["status"]]

        if job_status != JobStatus.successful:
            return job_status, None
        else:
            return (
                job_status,
                {
                    # TODO: this must come from process, write to metadata?
                    #       -> decide when requirements from other fields (start/end times) are clearer
                    # TODO: proper link
                    "notebook_link": f"https://example.com/f{result['notebook']}"
                },
            )

    def delete_job(self, processid, job_id):
        """
        Deletes a job

        :param processid: process identifier
        :param job_id: job identifier

        :returns: `bool` of status result
        """

        raise NotImplementedError()

    def delete_jobs(self, max_jobs, older_than):
        """
        TODO
        """

        raise NotImplementedError()

    def _execute_handler(
        self, p: BaseProcessor, job_id, data_dict: Dict
    ) -> Tuple[Optional[Dict], JobStatus]:
        """
        Synchronous execution handler

        :param p: `pygeoapi.t` object
        :param job_id: job identifier
        :param data_dict: `dict` of data parameters

        :returns: tuple of response payload and status
        """
        self._execute_handler_async(p=p, job_id=job_id, data_dict=data_dict)

        while True:
            # TODO: investigate if list_namespaced_job(watch=True) can be used here
            time.sleep(2)
            result = self.get_job_result(processid=p.metadata["id"], jobid=job_id)
            if result:
                status = JobStatus[result["status"]]
                if status not in (JobStatus.running, JobStatus.accepted):
                    break

        return (
            self.get_job_output(processid=p.metadata["id"], job_id=job_id)[1],
            status,
        )

    def _execute_handler_async(
        self, p: KubernetesProcessor, job_id, data_dict
    ) -> Tuple[None, JobStatus]:
        """
        In practise k8s jobs are always async.

        :param p: `pygeoapi.process` object
        :param job_id: job identifier
        :param data_dict: `dict` of data parameters

        :returns: tuple of None (i.e. initial response payload)
                  and JobStatus.accepted (i.e. initial job status)
        """
        spec = p.create_job_pod_spec(data=data_dict)

        job = k8s_client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=k8s_client.V1ObjectMeta(name=k8s_job_name(job_id)),
            spec=k8s_client.V1JobSpec(
                template=k8s_client.V1PodTemplateSpec(spec=spec),
                backoff_limit=0,
                ttl_seconds_after_finished=60 * 60 * 24 * 7,
            ),
        )

        # TODO: add metadata annotations (make sure status as enum)
        self.batch_v1.create_namespaced_job(body=job, namespace=self.namespace)

        LOGGER.info("Add job %s in ns %s", job.metadata.name, self.namespace)

        return (None, JobStatus.accepted)

    @cached_property
    def namespace(self):
        # getting the current namespace like this is documented here, so it should be fine:
        # https://kubernetes.io/docs/tasks/access-application-cluster/access-cluster/
        return open("/var/run/secrets/kubernetes.io/serviceaccount/namespace").read()


_ANNOTATIONS_PREFIX = "_pygeoapi_"


def parse_annotation_key(key: str) -> Optional[str]:
    matched = re.match(f"^{_ANNOTATIONS_PREFIX}(.+)", key)
    return matched.group(1) if matched else None


def format_annotation_key(key: str) -> str:
    return _ANNOTATIONS_PREFIX + key


def k8s_job_name(job_id: str) -> str:
    # TODO: include process id?
    return f"pygeoapi-job-{job_id}"


def job_status_from_k8s(status: k8s_client.V1JobStatus) -> JobStatus:
    # we assume only 1 run without retries

    # these "integers" are None if they are 0, lol
    if status.succeeded is not None and status.succeeded > 0:
        return JobStatus.successful
    elif status.failed is not None and status.failed > 0:
        return JobStatus.failed
    elif status.active is not None and status.active > 0:
        return JobStatus.running
    else:
        return JobStatus.accepted
