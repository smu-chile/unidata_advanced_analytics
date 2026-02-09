"""Script Creación Consolidado y egesta hacia sftp."""
# Default
import os
import logging
import argparse
from logging import config

import paramiko

# pip
from google.cloud.bigquery import Client

import common.gcp_extended.secretsmanager as secretmanager

# Own
from common.constants import LOGGING_CONFIG
from common.gcp_extended.bigquery import readBigQuery


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
# SQL QUERIES
# -------------------------------------------------------------------------

SQL_QUERIES = {
    'EMAIL' :
    """
    SELECT
        REPORT_ID,
        MAILING_ID,
        CANAL,
        ORGANIZATION_ID,
        TIPO_AUDIENCIA,
        FORMATO,
        TIPO_COMUNICACION,
        TIPO_CAMPANA,
        CAMPANA,
        MAILING_NAME,
        AB,
        ENVIADO,
        MAILING_SUBJECT,
        BASE,
        SUPPRESSED,
        TASA_SUPPRESS,
        SENT,
        HARD_BOUNCE,
        SOFT_BOUNCE,
        REPLY,
        DELIVERED,
        TASA_BOUNCE,
        OPEN,
        TASA_OPEN,
        OPEN_UNICO,
        TASA_OPEN_UNICO,
        CLICK,
        TASA_CLICK_OPEN,
        TASA_CLICK_SENT,
        CLICK_UNICO,
        TASA_CLICK_OPEN_UNICO,
        TASA_CLICK_SENT_UNICO,
        UNSUBSCRIBE
    FROM cl-cda-unidata-prod.DS_UNIDATA_CRM.VW_FACT_EVENTS_REPORT_EMAIL A
    WHERE
        DATE(A.ENVIADO) >= '2023-12-01'
    ORDER BY 12 DESC;
    """,
    'SMS' : """
    SELECT *
    FROM cl-cda-unidata-prod.DS_UNIDATA_CRM.VW_FACT_EVENTS_REPORT_SMS A
    ORDER BY 9 DESC;
    """,
    'PUSH' : """
    SELECT *
    FROM cl-cda-unidata-prod.DS_UNIDATA_CRM.VW_FACT_EVENTS_TOTAL_REPORT_PUSH_SALESFORCE
    ORDER BY 9 DESC;
    """
}

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


    for archivo in archivos:
        #Read Query
        logging.info(f'Reading Query for {archivo}')
        consolidado_df = readBigQuery(
            query=SQL_QUERIES[archivo],
            user='csotob',
            gbq_client = Client()
        )

        #Create excel from dataframe
        logging.info(f'Creating excel file for {archivo}')
        excel_name = f'CONSOLIDADO_{archivo}_{execution_date}.xlsx'
        consolidado_df.to_excel(
        excel_name,
        sheet_name=f'Consolidado {archivo.capitalize()}',
        index=False,
        header=True
    )


        logging.info(f'Starting upload of Consolidado {archivo} into SFTP Marketing Cloud')
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
        ftp.put(excel_name,f'/Import/CONSOLIDADO/{excel_name}')

        logging.info(f'removing {excel_name} from local')
        os.remove(f'{excel_name}')
    logging.info('Process ended!')

if __name__ == '__main__':
    main()
