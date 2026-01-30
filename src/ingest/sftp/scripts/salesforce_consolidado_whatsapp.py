"""Script ingesta.

Archivos de tracking Whatsapp en sftpSalesforce hacia BigQuery.
"""
# Default
import os
import logging
import zipfile
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
    #SELECT COLUMNS
    df_file = df_file[['ContactKey','MobileNumber','EventDateUTC','ChannelID',
                       'ChannelName','Status','Reason','ActivityName']]
    #RENAME COLUMN
    df_file = df_file.rename(columns={'ActivityName': 'Campaign'})
    #DROP Status == SENT
    df_file = df_file.drop(df_file[df_file.Status == 'SENT'].index)

    #ADD MONTH AND YEAR AS NEW COLUMNS
    df_file['EventDateUTC'] = pd.to_datetime(df_file['EventDateUTC'],
                                              format='%m/%d/%Y %I:%M:%S %p')
    df_file['YEAR'] = df_file['EventDateUTC'].dt.year
    df_file['MONTH'] = df_file['EventDateUTC'].dt.month
    #ADD FECHA_CARGA
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
    formatos = ['unimarc', 'alvi']

    # Set all clients

    gbq_client = bigquery.Client()
    #input files

    #table definitions jsons
    json_wp = 'CRM_DATA_SF_TRACKING_WHATSAPP.json'


    logging.info('Starting extraction of Consolidado Whatsapp from SFTP Marketing Cloud')
    sftp_secret = secretmanager.getSecret('salesforce_sftp_credentials',project=gcp_project_id)
    for formato in formatos:
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
        logging.info(f'Getting zip file wsp_tracking_{formato}')
        zip_name =f'wsp_tracking_{formato}_{execution_date}'
        try:
            ftp.get(f'/Import/WhatsApp/{zip_name}',zip_name)
        except FileNotFoundError:
            logging.info (f'Archivo {zip_name} no encontrado')

        logging.info(F'Unzipping {zip_name}')
        with zipfile.ZipFile(zip_name,'r') as zip_ref:
            zip_ref.extractall()
        os.remove(zip_name)

        csv_name = f'chat_wsp_tracking_{formato}_{execution_date}_tracking.csv'
        logging.info(F'Getting {csv_name} into Dataframe')
        df_file = pd.read_csv(f'{csv_name}', sep=',')
        df_file = cleaning_func(df_file,execution_date)
        # Upload data
        logging.info('Uploading data from Dataframe')
        logging.info('Create table if not exists')
        gbq_extended.createTableFromJSON(
                    table_ddl_json_path=os.path.join('gbq_objects', json_wp),
                    project=gcp_project_id,
                    gbq_client=gbq_client,
                    if_exists='ignore',
                )

        logging.info('Delete from table so that data is not duplicated')
        schema = 'CRM'
        table = json_wp.split('.')[0]
        table_ref = f'{gcp_project_id}.{schema}.{table}'
        gbq_extended.deleteFromTable(table_ref= table_ref,
                                     where_clause=f"""(FECHA_CARGA=parse_date('%Y%m%d',"{execution_date}")
                                     AND CHANNEL_NAME = "{formato.capitalize()}") """,
                                     gbq_client=gbq_client
                                    )

        logging.info('Uploading data from Dataframe')
        gbq_extended.uploadFrame(
                df_file,
                table_ddl_json_path=os.path.join('gbq_objects', json_wp),
                project=gcp_project_id,
                gbq_client=gbq_client,
                if_exists='append',
            )

        logging.info('Removing file')
        os.remove(f'{csv_name}')
    logging.info('Process ended!')

if __name__ == '__main__':
    main()
