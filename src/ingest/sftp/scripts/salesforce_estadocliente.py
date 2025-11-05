"""Script ingesta SFTP.

Archivos de estado de cliente en sftp Salesforce hacia BigQuery.
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
def cleaning_func(df_file: pd.DataFrame, execution_date : str) -> pd.DataFrame:
    """Transform Dataframe into expected format for uploading into BQ.

    Parameters
    ----------
    df_file : pd.DataFrame
        Input DataFrame to transform.
    execution_date : str
        Execution date to be added as a new field.
    """
    logging.info('Before cleaning:', df_file)
    df_file['DATE_UNDELIVERABLE'] = pd.to_datetime(df_file['DATE_UNDELIVERABLE'],
                                              format='%Y-%m-%d %H:%M:%S')
    df_file['DATE_JOINED'] = pd.to_datetime(df_file['DATE_JOINED'],
                                              format='%Y-%m-%d %H:%M:%S')
    df_file['DATE_UNSUBSCRIBED'] = pd.to_datetime(df_file['DATE_UNSUBSCRIBED'],
                                              format='%Y-%m-%d %H:%M:%S')
    df_file['CREATED_DATE'] = pd.to_datetime(df_file['CREATED_DATE'],
                                              format='%Y-%m-%d %H:%M:%S')
    df_file['LIST_DATE_UNSUBSCRIBED'] = pd.to_datetime(df_file['LIST_DATE_UNSUBSCRIBED'],
                                              format='%Y-%m-%d %H:%M:%S')


    df_file['FECHA_CARGA'] = pd.to_datetime(execution_date, format='%Y%m%d')
    logging.info('After cleaning:', df_file)


    return df_file

# -------------------------------------------------------------------------
# Main function
# -------------------------------------------------------------------------
def main() -> None:  # noqa: D103
    # Parse input variables
    args = vars(parser.parse_args())
    gcp_project_id: str = args['project_id']
    execution_date: str = args['execution_date']
    formatos = ['unidata']
    # Set all clients

    gbq_client = bigquery.Client()
    #input files

    #table definitions jsons
    json = 'CRM_CLIENT_LIST_MASTER_SUPRESS_LIST.json'
    for formato in formatos:
        logging.info(f'Starting extraction of Reporte SMS {formato} from SFTP Marketing Cloud')
        sftp_secret = secretmanager.getSecret('salesforce_sftp_credentials',project=gcp_project_id)
        #connect
        logging.info('Connecting to sftp')
        ssh_session = paramiko.Transport(
            f"{sftp_secret['host']}:{sftp_secret['port']}"
        )
        ssh_session.connect(
            username=sftp_secret[f'user_{formato}'],
            password=sftp_secret[f'pass_{formato}'],
        )
        logging.info('Opening sftp')
        ftp = paramiko.SFTPClient.from_transport(
            ssh_session
        )

        #get file
        logging.info(f'Getting file Reporte Estado CLiente {execution_date}')
        file_name = f'ReporteEstadoCliente_{execution_date}.csv'
        ftp.get(f'/Import/{file_name}',file_name)
        #close sftp
        ssh_session.close()


        logging.info(F'Getting {file_name} into Dataframe')
        df_file = pd.read_csv(f'{file_name}', sep='|', encoding='UTF-16')

        df_file = cleaning_func(df_file, execution_date)
        # Upload data
        logging.info('Uploading data from Dataframe')
        gbq_extended.uploadFrame(
                df_file,
                table_ddl_json_path=os.path.join('gbq_objects', json),
                project=gcp_project_id,
                gbq_client=gbq_client,
                if_exists='replace',
            )


        logging.info('Removing file')
        os.remove(f'{file_name}')
    logging.info('Process ended!')

if __name__ == '__main__':
    main()
