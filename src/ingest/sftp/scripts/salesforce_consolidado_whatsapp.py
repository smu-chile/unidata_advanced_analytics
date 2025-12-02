"""Script ingesta.

Archivo consolidado Whatsapp en sftpSalesforce hacia BigQuery.
"""
# Default
import os
import logging
import argparse
from logging import config

import pandas as pd
import paramiko

# pip
from google.cloud import bigquery

import common.gcp_extended.bigquery as gbq_extended
import common.gcp_extended.secretsmanager as secretmanager

# Own
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
# Cleaning Func
# -------------------------------------------------------------------------
def cleaning_func(df_file:pd.DataFrame, execution_date:str) -> pd.DataFrame:
    """Transform Dataframe into expected format for uploading into BQ.

    Parameters
    ----------
    df_file : pd.DataFrame
        Input DataFrame to transform.
    execution_date : string
        Execution date to be added as a field to the dataframe.
    """
    logging.info(f'Before cleaning: {df_file}')
    df_file['DATE'] = pd.to_datetime(df_file['DATE'],
                                              format='%Y-%m-%d')
    df_file['TIME'] = pd.to_datetime(df_file['TIME'],
                                              format='%H:%M:%S')
    df_file['FECHA_CARGA'] = pd.to_datetime(execution_date, format='%Y%m%d')

    logging.info(f'After cleaning: {df_file}')


    return df_file

# -------------------------------------------------------------------------
# Main function
# -------------------------------------------------------------------------
def main() -> None:  # noqa: D103
    # Parse input variables
    args = vars(parser.parse_args())
    gcp_project_id: str = args['project_id']
    execution_date: str = args['execution_date']
    # Set all clients

    gbq_client = bigquery.Client()
    #input files

    #table definitions jsons
    json_wp = 'CRM_DATA_SF_CONSOLIDADO_WHATSAPP.json'


    logging.info('Starting extraction of Consolidado Whatsapp from SFTP Marketing Cloud')
    sftp_secret = secretmanager.getSecret('salesforce_sftp_credentials',project=gcp_project_id)
    #connect
    logging.info('Connecting to sftp')
    ssh_session = paramiko.Transport(
        f"{sftp_secret['host']}:{sftp_secret['port']}"
    )
    ssh_session.connect(
        username=sftp_secret['user_unidata'],
        password=sftp_secret['pass_unidata'],
    )
    logging.info('Opening sftp')
    ftp = paramiko.SFTPClient.from_transport(
        ssh_session
    )
    #get file
    logging.info('Getting file Consolidado_WhatsApp.csv')
    csv_name ='Consolidado_WhatsApp.csv'
    try:
        ftp.get(f'/Import/WhatsApp/{csv_name}',csv_name)
    except FileNotFoundError:
        logging.info (f'Archivo {csv_name} no encontrado')
    logging.info(F'Getting {csv_name} into Dataframe')
    df_file = pd.read_csv(f'{csv_name}', sep=',')
    df_file = cleaning_func(df_file,execution_date)
    # Upload data
    logging.info('Uploading data from Dataframe')
    gbq_extended.uploadFrame(
            df_file,
            table_ddl_json_path=os.path.join('gbq_objects', json_wp),
            project=gcp_project_id,
            gbq_client=gbq_client,
            if_exists='replace',
        )
    logging.info('Removing file')
    os.remove(f'{csv_name}')
    logging.info('Process ended!')

if __name__ == '__main__':
    main()
