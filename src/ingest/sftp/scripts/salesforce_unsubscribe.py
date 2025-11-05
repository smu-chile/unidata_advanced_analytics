"""Script ingesta de archivos de clientes desuscritos en sftp
Salesforce hacia BigQuery."""
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
    execution_date : str
        Execution date to be added as a new field.
    """
    logging.info('Before cleaning:', df_file)
    if 'FECHA_UNSUBSCRIBE' in df_file.columns:
        df_file = df_file.rename(columns={'FECHA_UNSUBSCRIBE': 'FECHA'})
    #TODO(csotob): Averiguar formato para ms y arreglar estop
    df_file['FECHA'] = pd.to_datetime(df_file['FECHA'], format='ISO8601', dayfirst= True)

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
    formatos = ['unimarc','alvi', 'm10', 's10','unipay']
    archivos = ['EMAIL', 'SMS']

    # Set all clients

    gbq_client = bigquery.Client()
    #input files


    for formato in formatos:
        logging.info(f'Starting extraction of Reportes Unsub {formato} from SFTP SF')
        sftp_secret = secretmanager.getSecret('salesforce_sftp_credentials',project=gcp_project_id)
         #connect
        logging.info('Connecting to sftp')
        ssh_session = paramiko.Transport(
            f"{sftp_secret['host']}:{sftp_secret['port']}"
        )
        cred_suffix = formato if formato in ('alvi', 'unimarc', 'unipay') else 'm10s10'
        ssh_session.connect(
            username=sftp_secret[f'user_{cred_suffix}'],
            password=sftp_secret[f'pass_{cred_suffix}'],
        )
        logging.info('Opening sftp')
        ftp = paramiko.SFTPClient.from_transport(
            ssh_session
        )
        formato_name = formato.upper()
        #table definitions jsons
        jsons = {
        'EMAIL' : f'CRM_DATA_SF_EMAIL_UNSUBSCRIBE_{formato_name}.json',
        'SMS': f'CRM_DATA_SF_SMS_UNSUBSCRIBE_{formato_name}.json'
        }
        for file in archivos:
            if formato == 'unipay' and file=='SMS':
                continue
            #get file
            logging.info(f'Getting file Reporte Unsubscribe {file} {formato.capitalize()}')
            csv_name = f'ReporteUnsubscribe{file}{formato_name}_{execution_date}.csv'

            ftp.get(f'/Import/Reportes/{csv_name}',csv_name)


            logging.info(F'Getting {csv_name} into Dataframe')
            df_file = pd.read_csv(f'{csv_name}', sep=',', encoding='UTF-16')
            df_file = cleaning_func(df_file, execution_date)

            #Eliminar archivo
            logging.info('Removing file')
            os.remove(f'{csv_name}')
             # Upload data

            logging.info('Uploading data from Dataframe')
            gbq_extended.uploadFrame(
                df_file,
                table_ddl_json_path=os.path.join('gbq_objects', jsons[file]),
                project=gcp_project_id,
                gbq_client=gbq_client,
                if_exists='replace',
            )

    #close sftp
    ssh_session.close()
    logging.info('Process ended!')

if __name__ == '__main__':
    main()
