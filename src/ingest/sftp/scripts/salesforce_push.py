# Default
import os
import logging
import zipfile
import argparse
from logging import config
from datetime import datetime

import pytz
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
def cleaning_func(df_file, execution_date,formato):
    print('Before cleaning:', df_file)
    df_file['Date'] = pd.to_datetime(df_file['Date'],
                                              format='%m/%d/%Y %I:%M:%S %p')
    df_file['DateTimeSend'] = pd.to_datetime(df_file['DateTimeSend'],
                                              format='%m/%d/%Y %I:%M:%S %p')
    df_file['OpenDate'] = pd.to_datetime(df_file['OpenDate'],
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
    formatos = ['unimarc','alvi']
    # Set all clients

    gbq_client = bigquery.Client()
    #input files

    #table definitions jsons
    json = 'CRM_DATA_SF_PUSH_EVENT.json'
    for formato in formatos:
        logging.info(f'Starting extraction of Reporte PUSH {formato} from SFTP Marketing Cloud')
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
        formato_name =  formato.capitalize()
        logging.info(f'Getting file Reporte Push{formato.capitalize()}')
        zip_file_prefix = f'Reporte_{formato_name}_Push_'

        latest = 0
        latestfile = None

        for fileattr in ftp.listdir_attr(path='reports/'):
            if fileattr.filename.startswith(zip_file_prefix) and fileattr.st_mtime > latest:
                latest = fileattr.st_mtime
                latestfile = fileattr.filename

        if latestfile is not None:
            logging.info(f'Got file {latestfile}')
            modified = datetime.fromtimestamp(latest, tz=pytz.timezone('America/Santiago'))
            logging.info(f'Latest file found from date {modified}')
            if modified.strftime('%Y%m%d') != execution_date:
                logging.info('Not Updated today')
                return
            ftp.get(f'reports/{latestfile}', latestfile)
        else:
            logging.info('File not found')
            return
        #close sftp
        ssh_session.close()

        logging.info('Unzipping File')
        with zipfile.ZipFile(latestfile,'r') as zip_ref:
            zip_ref.extractall()

        logging.info('Removing zip file')
        os.remove(latestfile)
        file = 'MobilePush Message Detail Report'

        logging.info(F'Getting {file}.csv into Dataframe')
        df_file = pd.read_csv(f'{file}.csv', sep=',')

        df_file = cleaning_func(df_file, execution_date,formato_name)
        # Upload data
        logging.info('Create table if not exists')
        gbq_extended.createTableFromJSON(
                    table_ddl_json_path=os.path.join('gbq_objects', json),
                    project=gcp_project_id,
                    gbq_client=gbq_client,
                    if_exists='ignore',
                )

        logging.info('Delete from table so that data is not duplicated')
        schema = 'CRM'
        table = json.split('.')[0]
        table_ref = f'{gcp_project_id}.{schema}.{table}'
        gbq_extended.deleteFromTable(table_ref= table_ref,
                                     where_clause=f"""(FECHA_CARGA=parse_date('%Y%m%d',"{execution_date}")
                                     AND BUSINESS_UNIT = "{formato_name}") """,
                                     gbq_client=gbq_client
                                    )

        logging.info('Uploading data from Dataframe')
        gbq_extended.uploadFrame(
                df_file,
                table_ddl_json_path=os.path.join('gbq_objects', json),
                project=gcp_project_id,
                gbq_client=gbq_client,
                if_exists='append',
            )


        logging.info('Removing file')
        os.remove(f'{file}.csv')
    logging.info('Process ended!')

if __name__ == '__main__':
    main()
