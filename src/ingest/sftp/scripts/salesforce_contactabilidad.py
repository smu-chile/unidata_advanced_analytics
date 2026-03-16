"""Script ingesta SFTP.

Archivo de contactabilidad Alvi en sftp Salesforce hacia BigQuery.
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
def cleaning_func(df_file: pd.DataFrame, execution_date: str) -> pd.DataFrame:
    """Transform Dataframe into expected format for uploading into BQ.

    Parameters
    ----------
    df_file : pd.DataFrame
        Input DataFrame to transform.
    execution_date : str
        Execution date to be added as a new field.
    """
    logging.info('Before cleaning:', df_file)

    campos_fechas = ['FechaRegistro', 'FechaValidacionEmail',
                     'FechaValidacionWhatsapp']
    for campo in campos_fechas:
        if campo in df_file.columns:
            df_file[campo] = pd.to_datetime(df_file[campo],format='ISO8601', dayfirst= True)

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
    formatos = ['alvi', 'm10s10']

    # Set all clients

    gbq_client = bigquery.Client()
    #input files


    for formato in formatos:
        logging.info(f'Starting extraction of contactabilidad csv {formato} from SFTP SF')
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
        formato = formato.replace('m10','')
        formato_name = formato.upper()
        #table definitions jsons
        json = f'CRM_DATA_SF_CONTACTABILIDAD_{formato_name}.json'

        #get file
        logging.info(f'Getting file extract_contactabilidad {formato.capitalize()}')
        csv_name = 'extract_contactabilidad.csv'
        ftp.get(f'/Import/{csv_name}',csv_name)
        logging.info(F'Getting {csv_name} into Dataframe')
        df_file = pd.read_csv(f'{csv_name}', sep=',', encoding='UTF-16')
        df_file = cleaning_func(df_file, execution_date)
        #Eliminar archivo
        logging.info('Removing file')
        os.remove(f'{csv_name}')
         # Upload data
        logging.info('Uploading data from Dataframe')
        gbq_extended.uploadFrame(
            df_file[['Rut',
                     'Nombre',
                     'Apellido',
                     'TipoRut',
                     'EstadoPersona',
                     'Telefono',
                     'TelefonoVerificado',
                     'Email',
                     'EmailVerificado',
                     'Region',
                     'Comuna',
                     'Direccion',
                     'AceptaBasesConcursos',
                     'AceptaTyCClub',
                     'FechaValidacionEmail',
                     'FechaValidacionWhatsapp'
                     'FechaRegistro',
                     'FECHA_CARGA']],
            table_ddl_json_path=os.path.join('gbq_objects', json),
            project=gcp_project_id,
            gbq_client=gbq_client,
            if_exists='replace',
        )
    #close sftp
    ssh_session.close()
    logging.info('Process ended!')

if __name__ == '__main__':
    main()
