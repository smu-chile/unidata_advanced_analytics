from __future__ import annotations

# Default
import time
import logging
import argparse
import subprocess
from logging import config

# pip
from google.cloud.bigquery import Client

# Own
from common.constants import LOGGING_CONFIG
from common.gcp_extended.bigquery import readBigQuery
from common.gcp_extended.secretsmanager import getSecret


# -------------------------------------------------------------------------
# Package config
# -------------------------------------------------------------------------
# Logging config
config.dictConfig(LOGGING_CONFIG)
# Parser
parser = argparse.ArgumentParser()
parser.add_argument(
    '--project_name', type=str, required=True,
    help='Name fo the Advanced Analytics project executed'
)
parser.add_argument(
    '--gcp_project', type=str, required=True,
    help='Name of the GCP project billed. Used to differenciate dev from prod'
)
parser.add_argument(
    '--query', type=str, required=True,
    # TODO(ecastrot): Fill
    help='Query that gets the data from GCP'
)
parser.add_argument(
    '--netezza_table_ref', type=str, required=True,
    # TODO(ecastrot): Fill
    help=''
)
parser.add_argument(
    '--netezza_columns', type=str, required=True,
    # TODO(ecastrot): Fill
    help=''
)
parser.add_argument(
    '--delete_where_clause', type=str, required=False,
    default='',
    # TODO(ecastrot): Fill
    help=''
)
parser.add_argument(
    '--if_exists', type='str', required=True,
    choices=['append', 'rebuild']
)
parser.add_argument(
    '--timeout', type=str, required=False,
    default=0,
    # TODO(ecastrot): Fill
    help=''
)


# -------------------------------------------------------------------------
# Main Function
# -------------------------------------------------------------------------
def main():
    # ----------
    # Parameters
    # ----------
    args = vars(parser.parse_args())

    # Environment
    user: str = 'day_' + args['project_name']
    gcp_project: str = args['gcp_project']
    query: str =  args['query']
    netezza_table_ref: str = args['netezza_table_ref']
    delete_where_clause: str = args['delete_where_clause']
    netezza_columns: str = args['netezza_columns']
    if_exists: str = args['if_exists']
    timeout: str = args['timeout']

    # Static
    csv_path = 'tmp_data.csv'
    gbq_client = Client()

    # Timeout
    if timeout:
        time.sleep(timeout)

    # Get info
    info_df = readBigQuery(
        user=user,
        query=query,
        gbq_client=gbq_client,
    )
    # Query is empty
    if info_df.empty:
        err_msg = 'Dataframe resulting from query is empty'
        raise OSError(err_msg)
    # Print info
    logging.info(f'Info recollected: \n{info_df.head()}')
    logging.info(f'DataFrame shape: {info_df.shape}')

    # Save info as CSV
    info_df.to_csv(
        csv_path,
        index=False, header=False, sep='|'
    )
    logging.info(f'Temporal data file locally written into {csv_path}')

    # Get and build credentials for Netezza connection
    netezza_credentials = getSecret(
        'bdaa_netezza_credentials',
        project=gcp_project,
    )

    netezza_query = ''

    # Remove table when the mode is rebuild
    if if_exists == 'rebuild':
        netezza_query += f'DROP TABLE {netezza_table_ref} IF EXISTS;'

    # Create the table if not exists
    netezza_query += f'CREATE TABLE IF NOT EXISTS {netezza_table_ref} '
    netezza_query += f'({netezza_columns});'

    # Delete the partition that will be written if exists
    if delete_where_clause:
        netezza_query += f'DELETE FROM {netezza_table_ref} '  # noqa: S608
        netezza_query += f'WHERE {delete_where_clause};'

    logging.info(f'Table ddl query to be sent: {netezza_query}')

    # ---
    # DDL
    # ---
    logging.info('Building table...')
    netezza_credentials_str = (
        'Driver={NetezzaSQL};'
        f"servername={netezza_credentials['server']};"
        f"port={netezza_credentials['port']};"
        f"database={netezza_table_ref.split('.')[0]};"
        f"username={netezza_credentials['username']};"
        f"password={netezza_credentials['password']};"
    )
    bash_command_response = subprocess.run((  # noqa: S602
            'nzodbcsql'
            f' -c "{netezza_credentials_str}"'
            f' -q "{query}"'
        ),
        shell=True,
        check=True,
        capture_output=True,
        text=True
    )
    logging.info(f'DDL command response: {bash_command_response}')

    # Load data
    logging.info('Loading data...')
    bash_command_response = subprocess.run((  # noqa: S602
            'nzload'
            f" -host {netezza_credentials['server']}"
            f" -u {netezza_credentials['username']}"
            f" -pw {netezza_credentials['password']}"
            f" -db {netezza_table_ref.split('.')[0]}"
            f" -schema {netezza_table_ref.split('.')[1]}"
            f" -t {netezza_table_ref.split('.')[2]}"
            f" -df {csv_path}"
            " -delim '|' -maxErrors 1 -truncString"
            f" -bf tmp_error.err"
            f" -lf tmp_log.log"
        ),
        shell=True,
        check=True,
        capture_output=True,
        text=True
    )
    logging.info(f'Command response: {bash_command_response}')

    logging.info('Done! :)')
