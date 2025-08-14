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
def cleaning_func(df_file, execution_date,archivo,formato):
    print('Before cleaning:', df_file)
    if 'TriggeredSendExternalKey' in df_file.columns and archivo != 'SendJobs':
        df_file = df_file.drop(columns=['TriggeredSendExternalKey'])
    if 'EventDate' in df_file.columns:
        df_file['EventDate'] = pd.to_datetime(df_file['EventDate'],
                                              format='%m/%d/%Y %I:%M:%S %p')
    if 'SchedTime' in df_file.columns:
        df_file['SchedTime'] = pd.to_datetime(df_file['SchedTime'],
                                              format='%m/%d/%Y %I:%M:%S %p')
    if 'SentTime' in df_file.columns:
        df_file['SentTime'] = pd.to_datetime(df_file['SentTime'],
                                              format='%m/%d/%Y %I:%M:%S %p')
    df_file['BUSINESS_UNIT'] = formato
    df_file['FECHA_CARGA'] = pd.to_datetime(execution_date, format='%Y%m%d')
    print('After cleaning:', df_file)


    return df_file

# -------------------------------------------------------------------------
# Main function
# -------------------------------------------------------------------------
def main() -> None:  # noqa: D103
    # Parse input variables
    args = vars(parser.parse_args())
    gcp_project_id: str = args['project_id']
    execution_date: str = args['execution_date']
    formatos = ['unimarc','alvi', 'm10s10','unipay']
    archivos = ['Bounces', 'Clicks', 'Complaints', 'NotSent', 'Opens','SendJobs','Sent','Unsubs']

    # Set all clients

    gbq_client = bigquery.Client()
    #input files

    #table definitions jsons
    jsons = {
    'Bounces' : 'CRM_TMP_DATA_SF_EMAIL_BOUNCE.json',
    'Clicks': 'CRM_TMP_DATA_SF_EMAIL_CLICK.json',
    'Complaints' : 'CRM_TMP_DATA_SF_EMAIL_COMPLAINTS.json',
    'NotSent' : 'CRM_TMP_DATA_SF_EMAIL_NOTSENT.json',
    'Opens': 'CRM_TMP_DATA_SF_EMAIL_OPEN.json',
    'SendJobs' : 'CRM_TMP_DATA_SF_EMAIL_JOBS.json',
    'Sent' : 'CRM_TMP_DATA_SF_EMAIL_SEND.json',
    'Unsubs' : 'CRM_TMP_DATA_SF_EMAIL_UNSUBSCRIBE.json'
    }
    for formato in formatos:
        logging.info(f'Starting extraction of Reporte General {formato} from SFTP Marketing Cloud')
        sftp_secret = secretmanager.getSecret('salesforce_sftp_credentials')
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
        formato_name = formato.upper() if formato == 'm10s10' else formato.capitalize()
        logging.info(f'Getting file ReporteGeneral{formato.capitalize()}')
        zip_file_name = f'ReporteGeneral{formato_name}{execution_date}.zip'
        ftp.get(f'/Import/Reportes/{zip_file_name}',zip_file_name)
        #close sftp
        ssh_session.close()

        logging.info('Unzipping File')
        with zipfile.ZipFile(zip_file_name,'r') as zip_ref:
            zip_ref.extractall()

        logging.info('Removing zip file')
        os.remove(zip_file_name)

        for file in archivos:
            logging.info(F'Getting {file}.csv into Dataframe')
            df_file = pd.read_csv(f'{file}.csv', sep='|')
            df_file = cleaning_func(df_file, execution_date,file,formato_name)
            # Upload data
            logging.info('Create table if not exists')
            gbq_extended.createTableFromJSON(
                    table_ddl_json_path=os.path.join('gbq_objects', jsons[file]),
                    project=gcp_project_id,
                    gbq_client=gbq_client,
                    if_exists='ignore',
                )

            #TODO(csotob): fix where_clause
            logging.info('Delete from table so that data is not duplicated')
            schema = 'TMP_CRM'
            table = jsons[file].removeprefix('CRM_').split('.')[0]
            table_ref = f'{gcp_project_id}.{schema}.{table}'
            gbq_extended.deleteFromTable(table_ref= table_ref,
                                         where_clause=f"""FECHA_CARGA=parse_date('%Y%m%d',"{execution_date}")
                                        AND BUSINESS_UNIT = "{formato_name}" """,
                                         gbq_client=gbq_client
                                         )

            logging.info('Uploading data from Dataframe')
            gbq_extended.uploadFrame(
                df_file,
                table_ddl_json_path=os.path.join('gbq_objects', jsons[file]),
                project=gcp_project_id,
                gbq_client=gbq_client,
                if_exists='replace',
            )


            logging.info('Removing file')
            os.remove(f'{file}.csv')
    logging.info('Process ended!')

if __name__ == '__main__':
    main()
