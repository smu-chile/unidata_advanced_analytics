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
def cleaning_func(df_file):
    print('Before cleaning:', df_file)
    df_file['ENVIADO'] = pd.to_datetime(df_file['ENVIADO'],
                                              format='%Y/%m/%d %H:%M:%S')

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
    archivos = ['EMAIL','SMS', 'PUSH']
    # Set all clients

    gbq_client = bigquery.Client()
    #input files

    #table definitions jsons
    jsons = {
        'EMAIL' : 'CRM_DATA_SF_CONSOLIDADO_EMAIL_TEMPORAL.json',
        'SMS' : 'CRM_DATA_SF_CONSOLIDADO_SMS_TEMPORAL.json',
        'PUSH' : 'CRM_DATA_SF_CONSOLIDADO_PUSH_TEMPORAL.json'

    }
    for archivo in archivos:
        logging.info(f'Starting extraction of Consolidado {archivo} from SFTP Marketing Cloud')
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
        logging.info(f'Getting file CONSOLIDADO_{archivo}_{execution_date}')
        excel_name = f'CONSOLIDADO_{archivo}_{execution_date}.xls'
        ftp.get(f'/Import/CONSOLIDADO/{excel_name}',excel_name)

        logging.info(F'Getting {excel_name} into Dataframe')
        df_file = pd.read_excel(f'{excel_name}')

        df_file = cleaning_func(df_file)
        # Upload data
        logging.info('Uploading data from Dataframe')
        gbq_extended.uploadFrame(
                df_file,
                table_ddl_json_path=os.path.join('gbq_objects', jsons[archivo]),
                project=gcp_project_id,
                gbq_client=gbq_client,
                if_exists='replace',
            )


        logging.info('Removing file')
        os.remove(f'{excel_name}')
    logging.info('Process ended!')

if __name__ == '__main__':
    main()
