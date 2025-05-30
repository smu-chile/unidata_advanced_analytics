# Default
import os
import logging
import argparse
from logging import config

# pip
import pandas as pd
import pendulum
from google.cloud import bigquery

# Own
import common.gcp_extended.bigquery as gbq_extended
from common.constants import LOGGING_CONFIG
from common.utils.queries import QueryDict


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
#  SQL Queries
# -------------------------------------------------------------------------
SQL_QUERIES = QueryDict({
    'project_jobs':
    """
    SELECT
        cache_hit,
        creation_time,
        end_time,
        job_id,
        job_type,
        labels,
        project_id,
        state,
        statement_type,
        COALESCE(total_bytes_billed, 0) AS total_bytes_billed,
        COALESCE(total_bytes_processed, 0) AS total_bytes_processed,
        COALESCE(total_slot_ms, 0) AS total_slot_ms,
        user_email

    FROM `${gcp_project}`.`region-${gcp_region}`.INFORMATION_SCHEMA.JOBS

    WHERE
        TIMESTAMP("${min_creation_time}") <= CREATION_TIME
        AND CREATION_TIME <= TIMESTAMP("${min_creation_time}") + INTERVAL 1 DAY
        AND STATEMENT_TYPE != 'SCRIPT'
    """
})


# -------------------------------------------------------------------------
# Functions and Classes
# -------------------------------------------------------------------------
def setUserFromMail(row: pd.Series) -> str:
    """Gets user from email if project name was not set by labels.

    Parameters
    ----------
    row : pd.Series
        Row with user emails and user or project names as columns

    Returns
    -------
    user_or_project_name : str
        Name of the user or project that made the query
    """
    if not pd.isna(row['user_or_project']):
        return row['user_or_project']
    return row['user_email'].split('@')[0]


# -------------------------------------------------------------------------
# Main function
# -------------------------------------------------------------------------
def main() -> None:  # noqa: D103
    user = 'cost_analyzer'
    # Parse input variables
    args = vars(parser.parse_args())
    execution_date: str = args['execution_date']
    gcp_project_id: str = args['project_id']

    gcp_projects = [
        'cl-bigdata-analytics',
        #'cl-bigdata-analytics-preprod',
        #'cl-bigdata-analytics-prod',
        'cl-cda-unidata-dev',
        'cl-cda-unidata-prod',
    ]

    # Set gbq client for all subsequent queries
    gbq_client = bigquery.Client()

    # Scan jobs
    jobs = pd.DataFrame()

    for gcp_project in gcp_projects:
        # Read INFORMATION_SCHEMA.JOBS view
        logging.info(f'Searching jobs in the {gcp_project} project')
        project_jobs = gbq_extended.readBigQuery(
            query=SQL_QUERIES['project_jobs'].substitute(
                gcp_project=gcp_project,
                # Using US general region to catch all jobs
                gcp_region='us',
                min_creation_time=pendulum.datetime(
                    *map(int, execution_date.split('-')),
                    hour=0, minute=0, second=0, microsecond=0,
                    tz=pendulum.timezone('America/Santiago')
                ),
            ),
            user=user,
            gbq_client=gbq_client,
        )

        # Break execution when no job was executed
        if project_jobs.empty:
            logging.info(f'No jobs founded for {execution_date}. Going to the next project')
            continue
        logging.info(f'{project_jobs.shape[0]:,} jobs founded in {gcp_project} project')


        # Compute query duration
        project_jobs['creation_time'] = project_jobs['creation_time'].dt.tz_convert(
            'America/Santiago'
        )
        project_jobs['end_time'] = project_jobs['end_time'].dt.tz_convert(
            'America/Santiago'
        )
        project_jobs['duration'] = (
            project_jobs['end_time'] - project_jobs['creation_time']
        ).astype(str).str.split(' ').str[-1]

        # Get date of the execution
        project_jobs['creation_date'] = project_jobs['creation_time'].dt.date

        # bytes to Mbytes
        project_jobs['total_mb_billed'] = project_jobs['total_bytes_billed'] / 1024 / 1024
        project_jobs['total_mb_processed'] = project_jobs['total_bytes_processed'] / 1024 / 1024
        # Slot usage ms to s
        project_jobs['total_s_slot_usage'] = project_jobs['total_slot_ms'] / 1000 / 60

        # Construct user
        project_jobs['user_or_project'] = project_jobs['labels'].str[0].str.get('value')
        project_jobs['user_or_project'] = project_jobs[[
            'user_email', 'user_or_project'
        ]].apply(
            setUserFromMail,
            axis=1
        )

        jobs = pd.concat([
                jobs,
                project_jobs[[
                    'project_id',
                    'user_or_project',
                    'user_email',
                    'job_id',
                    'job_type',
                    'cache_hit',
                    'creation_date',
                    'duration',
                    'statement_type',
                    'state',
                    'total_mb_billed',
                    'total_mb_processed',
                    'total_s_slot_usage'
                ]]
            ],
            axis=0,
            ignore_index=True,
        )

    if jobs.empty:
        return

    # Create GBQ table if does not exist
    logging.info('Creating GBQ table using JSON')
    gbq_extended.createTableFromJSON(
        ddl_json_config_path=os.path.join('gbq_objects', 'gbq_job_consumption.json'),
        project=gcp_project_id,
        gbq_client=gbq_client,
        if_exists='ignore',
    )

    # Delete data from the execution_date if reprocessing
    gbq_extended.deleteFromTable(
        table_ref=f'{gcp_project_id}.ML_LAB.GBQ_JOB_CONSUMPTION',
        column_name='creation_date',
        column_type='date',
        column_value=execution_date,
        gbq_client=gbq_client,
    )

    # Upload data
    logging.info(f'Uploading data for {execution_date}')
    gbq_extended.uploadFrame(
        jobs,
        table_ref=f'{gcp_project_id}.ML_LAB.GBQ_JOB_CONSUMPTION',
        table_ddl_json_path=os.path.join('gbq_objects', 'gbq_job_consumption.json'),
        gbq_client=gbq_client,
        if_exists='append',
    )
    logging.info('Process ended!')


if __name__ == '__main__':
    main()
