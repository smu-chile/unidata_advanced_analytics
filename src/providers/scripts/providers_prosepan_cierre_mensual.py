"""Script Creación Consolidado y egesta hacia sftp.
"""
# Default
import io
import logging
import argparse
from logging import config

import pandas as pd

# pip
from google.cloud.bigquery import Client

import common.gcp_extended.secretsmanager as secretmanager
import common.office365_extended.sharepoint as sp

# Own
from common.constants import LOGGING_CONFIG
from common.databases.queries import QueryDict
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
    '--execution_month', type=str,
    help='DAG execution month'
)

# -------------------------------------------------------------------------
# SQL QUERIES
# -------------------------------------------------------------------------

SQL_QUERIES = QueryDict({
    'Prosepan Weights' :
    """
    SELECT * FROM cl-cda-unidata-prod.DS_UNIDATA_PROVEEDORES.PROVEEDORES_PROSEPAN_WEIGHTS
    WHERE YEAR = ${year}
    AND MONTH = ${month}
    """,
    'Prosepan Visits' : """
    SELECT * FROM cl-cda-unidata-prod.DS_UNIDATA_PROVEEDORES.PROVEEDORES_PROSEPAN_VISITS
    WHERE YEAR = ${year}
    AND MONTH = ${month}
    """,
    'Store Visits' : """
    SELECT * FROM cl-cda-unidata-prod.DS_UNIDATA_PROVEEDORES.PROVEEDORES_PROSEPAN_STORE_VISITS
    WHERE YEAR = ${year}
    AND MONTH = ${month}
    """,
})

# -------------------------------------------------------------------------
# Main function
# -------------------------------------------------------------------------
def main() -> None:  # noqa: D103
    # Parse input variables
    args = vars(parser.parse_args())
    gcp_project_id: str = args['project_id']
    execution_month: str = args['execution_month']
    transacciones = ['Prosepan Weights','Prosepan Visits','Store Visits']
    # Set year and month
    year = execution_month.split('-')[0]
    month = execution_month.split('-')[1]

    excel_transacciones = f'Prosepan Cierre Mensual {execution_month}.xlsx'
    offset_buffer = io.BytesIO()
    with pd.ExcelWriter(offset_buffer, engine='openpyxl') as writer:
        for hoja in transacciones:
            #Read Query
            logging.info(f'Reading Query for {hoja}')
            transacciones_df = readBigQuery(
                query=SQL_QUERIES[hoja].substitute(
                    year = year,
                    month = month
                ),
                user='csotob',
                gbq_client = Client()
            )

            logging.info (f'Query result: {transacciones_df.head()}')

            #Create excel from dataframe
            logging.info(f'Creating excel file for Transacciones Prosepan {execution_month}')
            transacciones_df.to_excel(
                    writer,
                    sheet_name=hoja,
                    index=False,
                    header=True
                    )
    offset_buffer.seek(0)
    file_content = offset_buffer.getvalue()  # noqa: ERA001

    logging.info(f'Starting upload of {excel_transacciones} into SharePoint')
    file_site = '/sites/BigDatayAdvancedAnalytics/Documentos compartidos/Proveedores/Prosepan'
    input_file =  f'{file_site}/{excel_transacciones}'
    sp_cred = secretmanager.getSecret(
        'bdaa_sharepoint_credentials',
        project=gcp_project_id
    )
    sharepoint = sp.SharePointFolder(
        **sp_cred,
        server_relative_folder=file_site
    )
    if not transacciones_df.empty:
        sharepoint.upload_file(input_file,file_content)

    logging.info('Process ended!')

if __name__ == '__main__':
    main()
