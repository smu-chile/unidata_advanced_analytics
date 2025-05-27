# Default
import os
import logging
import argparse
from logging import config
from collections import defaultdict

# pip
import pandas as pd
import pendulum
from google.cloud import bigquery

# Own
import common.gcp_extended.bigquery as gbq_extended
from common.constants import LOGGING_CONFIG


# -------------------------------------------------------------------------
#  Config
# -------------------------------------------------------------------------
# Logging config
config.dictConfig(LOGGING_CONFIG)
# Parser config
parser = argparse.ArgumentParser()
parser.add_argument(
    '--project_id', type=str,
    help='GCP project in which the script will be executed'
)
parser.add_argument(
    '--execution_date', type=str,
    help='DAG execution date'
)


# -------------------------------------------------------------------------
# Functions and Classes
# -------------------------------------------------------------------------
def replaceUserLabel(row: pd.Series) -> str:
    """"""
    valid_substitution_users = {
        'ecastrot@unidata.cl': 'ecastrot',
        'csotob@unidata.cl': 'csotob',
        'rarriagada@unidata.cl': 'rarriagada',
        'bmolinab@unidata.cl': 'bmolinab',
        'csmunozr@unidata.cl': 'csmunozr',
        'e_ebenites@smu.cl': 'e_ebenites',
        'sabbas@smu.cl': 'sabbas',
    }

    if row['user'] != 'default':
        return row['user']

    return valid_substitution_users.get(row['user_email'], 'default')


# -------------------------------------------------------------------------
# Main function
# -------------------------------------------------------------------------
def main() -> None:  # noqa: D103
    # Parse input variables
    args = vars(parser.parse_args())
    execution_date: str = args['execution_date']
    gcp_project: str = args['project_id']

    # Get all jobs in the execution_date
    logging.info(f'Getting all jobs started on {execution_date}')
    gbq_client = bigquery.Client()
    all_jobs = gbq_client.list_jobs(
        project=gcp_project,
        all_users=True,
        min_creation_time=pendulum.datetime(
            *map(int, execution_date.split('-')),
            hour=0, minute=0, second=0, microsecond=0,
            tz=pendulum.timezone('America/Santiago')
        ),
        max_creation_time=pendulum.datetime(
            *map(int, execution_date.split('-')),
            hour=23, minute=59, second=59, microsecond=999999,
            tz=pendulum.timezone('America/Santiago')
        ),
    )

    # Save job info into dict[str, list]
    logging.debug('Start parsing jobs')
    jobs_info = defaultdict(list)
    for job in all_jobs:
        jobs_info['job_id'].append(job.job_id)
        jobs_info['job_type'].append(job.job_type)
        jobs_info['state'].append(job.state)
        jobs_info['statement_type'].append(job.statement_type)

        duration = pendulum.instance(job.ended) - pendulum.instance(job.created)
        jobs_info['duration'].append(f'{duration.hours}:{duration.minutes}:{duration.seconds}.{duration.microseconds}')

        jobs_info['user'].append(job.labels.get('user', 'default'))
        jobs_info['user_email'].append(job.user_email)

        jobs_info['total_bytes_processed'].append(
            job.total_bytes_processed if job.total_bytes_processed is not None else 0
        )
        jobs_info['total_bytes_billed'].append(
            job.total_bytes_billed if job.total_bytes_billed is not None else 0
        )

    # dict[str, list] into DataFrame
    jobs_info = pd.DataFrame(jobs_info)
    logging.debug('Ended parsing jobs')
    # Break execution when no job was executed
    if jobs_info.empty:
        logging.info(f'No jobs founded for {execution_date}. Breaking execution')
        return
    logging.info(f'{jobs_info.shape[0]:,} jobs founded')

    # Assign start date
    jobs_info['started_date'] = execution_date

    # Transform bytes to mega bytes
    jobs_info['total_bytes_billed'] = jobs_info['total_bytes_billed'].fillna(0) / 1e6
    jobs_info['total_bytes_processed'] = jobs_info['total_bytes_processed'].fillna(0) / 1e6
    jobs_info = jobs_info.rename(columns={
        'total_bytes_billed': 'total_mb_billed',
        'total_bytes_processed': 'total_mb_processed',
    })

    # Build user name from known users
    # This is made to flag queries made by known users directly on the GCP
    # console
    jobs_info['user'] = jobs_info[['user', 'user_email']].apply(replaceUserLabel, axis=1)

    # Create GBQ table if does not exist
    logging.info('Creating GBQ table using JSON')
    gbq_extended.createTableFromJSON(
        ddl_json_config_path=os.path.join('gbq_objects', 'gbq_job_consumption.json'),
        project=gcp_project,
        if_exists='ignore'
    )

    # Delete data from the execution_date if reprocessing
    gbq_extended.deleteFromTable(
        table_ref=f'{gcp_project}.ML_LAB.GBQ_JOB_CONSUMPTION',
        column_name='started_date',
        column_type='date',
        column_value=execution_date,
    )

    # Upload data
    logging.info(f'Uploading data for {execution_date}')
    gbq_extended.uploadFrame(
        jobs_info[[
            'job_id', 'job_type', 'state', 'statement_type', 'started_date', 'duration',
            'user', 'user_email', 'total_mb_billed', 'total_mb_processed'
        ]],
        table_ref=f'{gcp_project}.ML_LAB.GBQ_JOB_CONSUMPTION',
        table_ddl_json_path=os.path.join('gbq_objects', 'gbq_job_consumption.json'),
        if_exists='append',
    )
    logging.info('Process ended! :)')


if __name__ == '__main__':
    main()
