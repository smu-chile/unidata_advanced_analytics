"""Extension of the google.cloud.logging_v2 modules."""
from __future__ import annotations

# Default
import re

# pip
import google.cloud.logging_v2 as gclogging

# Own
from ..utils.queries import QueryDict  # noqa: TID252


def getDataprocJobLogs(
    job_uuid: str,
    region: str = 'us-east1',
    project_name: str = 'cl-bigdata-analytics',
) -> list[str]:
    """Get only the script produced logs from a Dataproc submitted job.

    Parameters
    ----------
    job_uuid : str
        Job UUID that can be found on the task logs in the GCP Composer DAG
        page. If the name of the job is
        `batch-bc027255-0c7b-496a-aa5c-5d8cd259276a` then the UUID is
        `bc027255-0c7b-496a-aa5c-5d8cd259276a`
    region : str, default='us-east1'
        GCP Region
    project_name : str, default='cl-bigdata-analytics'
        Name of the project hosting the GCP Dataproc service

    Returns
    -------
    logs : list[str]
        Script produced logs
    """
    filled_filter = QueryDict({
        'filter':
        """
        log_name="projects/cl-bigdata-analytics/logs/dataproc.googleapis.com%2foutput"
        resource.type="cloud_dataproc_batch"
        resource.labels.project_id="${project_name}"
        resource.labels.location="${region}"
        resource.labels.batch_id="batch-${job_uuid}"
        severity>=default
        timestamp>="2025-01-01t00:00:00.615z"
        """
    }).substitute(
        query_name='filter',
        project_name=project_name,
        region=region,
        job_uuid=job_uuid,
    )

    gclogging_client = gclogging.Client(
        project=project_name
    )

    return '\n'.join([
        entry.payload['message']
        for entry
        in gclogging_client.list_entries(
            order_by=gclogging.ASCENDING,
            filter_=filled_filter
        )
        if re.match(
            r'\[\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}-\d{4}\|\w+@\d+\|\w+\] .*',
            entry.payload['message']
        ) is not None
    ])


if __name__ == '__main__':
    err_msg = 'This file is only meant to be imported.'
    raise Exception(err_msg)
