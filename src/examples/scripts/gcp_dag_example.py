"""Example GCP script.

Example on how to import modules from commons.
"""
# Default
import os
import sys
import logging
import argparse
import subprocess
from logging import config

# pip
from google.cloud import bigquery

# Own
import common.gcp_extended.bigquery as gbq_extended
from common.constants import LOGGING_CONFIG, SHORT_STORE_BANNERS
from common.databases.queries import QueryDict


# -------------------------------------------------------------------------
# Logging config
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
# SQL Queries
# -------------------------------------------------------------------------
_SQL_QUERIES = QueryDict({
    'example_query':
    """
    SELECT * FROM ${table} LIMIT 10;
    """
})

def main() -> None:  # noqa: D103
    # Parse input variables
    args = vars(parser.parse_args())
    gcp_project: str = args['project_id']

    # Set gbq client for all subsequent queries
    gbq_client = bigquery.Client()

    logging.info('Store banner mapping:')
    for og_store_banner, short_store_banner in SHORT_STORE_BANNERS.items():
        logging.info(f'{og_store_banner} -> {short_store_banner}')

    logging.info(
        f"Example query: {_SQL_QUERIES['example_query'].substitute(table='schema.table')}"
    )

    python_version = subprocess.run(
        ['python', '--version'],
        capture_output=True
    ).stdout.decode()
    logging.info(python_version)

    pip_version = subprocess.run(
        ['pip', '--version'],
        capture_output=True
    ).stdout.decode()
    logging.info(pip_version)

    pip_freeze = subprocess.run(
        ['pip', 'freeze'],
        capture_output=True
    ).stdout.decode()
    for lib in pip_freeze.split('\n'):
        logging.warning(lib)

    for p in sys.path:
        logging.error(p)

    logging.info('Building table')
    gbq_extended.createTableFromJSON(
        ddl_json_config_path=os.path.join('gbq_objects', 'example_time_partition_ddl.json'),
        project=gcp_project,
        if_exists='ignore',
        gbq_client=gbq_client,
    )

    logging.info('Process ended! :)')


if __name__ == '__main__':
    main()
