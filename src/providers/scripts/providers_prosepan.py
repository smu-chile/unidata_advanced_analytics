"""Script Egesta data semanal desde BQ Hacia SP.
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
    '--execution_date', type=str,
    help='DAG execution date'
)

# -------------------------------------------------------------------------
# SQL QUERIES
# -------------------------------------------------------------------------

SQL_QUERIES = QueryDict({
    'TRANSACCIONES_PA' :
    """
    SELECT STORE_ID AS `Store Code`,
        Modelo,
        Zone as Zona,
        store_name as Store_Name,
        region as Region,
        transacciones_actual as Transacciones,
        transacciones_PA as `Transacciones PA`,
        Var_transacciones_PA as `Var % Transacciones Totales`,
        transacciones_PG_actual as `Transacciones Pan Granel`,
        transacciones_PG_PA as `Transacciones Pan Granel PA`,
        Var_transacciones_PG_PA as `Var % Transacciones Pan Granel`,
        venta_bruta_PG_actual as `Venta Bruta Pan Granel`,
        venta_bruta_PG_PA as `Venta Bruta Pan Granel PA`,
        Var_venta_PG_PA as `Var % Venta Brutal Pan Granel`,
        penetracion_actual as `Penetración`,
        penetracion_PA as `Penetración PA`,
        Var_penetracion_PA as `Var % Penetración`,
        peso_PG_actual as `Peso Pan Granel`,
        peso_PG_PA as `Peso Pan Granel PA`,
        Var_peso_PG_PA as `Var % Peso Pan Granel`,
        peso_por_visita_actual as `Peso por Visita Pan Granel`,
        peso_por_visita_PA as `Peso por Visita Pan Granel PA`,
        Var_peso_por_visita_PG_PA as `Var % Peso por Visita Pan Granel`,
        frecuencia_actual as Frecuencia,
        frecuencia_PA as `Frecuencia PA`,
        Var_frecuencia_PA as `Var % Frecuencia`
    FROM
    `cl-cda-unidata-prod.DS_UNIDATA_PROVEEDORES.PROVEEDORES_PROSEPAN_VENTAS_TRANSACCIONES_PENETRACION`

    WHERE SEMANA_ACTUAL_2 = '${execution_date}'
    """,
    'TRANSACCIONES_AA' : """
    SELECT STORE_ID AS `Store Code`,
        Modelo,
        Zone as Zona,
        store_name as Store_Name,
        region as Region,
        transacciones_actual as Transacciones,
        transacciones_AA as `Transacciones AA`,
        Var_transacciones_AA as `Var % Transacciones Totales`,
        transacciones_PG_actual as `Transacciones Pan Granel`,
        transacciones_PG_AA as `Transacciones Pan Granel AA`,
        Var_transacciones_PG_AA as `Var % Transacciones Pan Granel`,
        venta_bruta_PG_actual as `Venta Bruta Pan Granel`,
        venta_bruta_PG_AA as `Venta Bruta Pan Granel AA`,
        Var_venta_PG_AA as `Var % Venta Brutal Pan Granel`,
        penetracion_actual as `Penetración`,
        penetracion_AA as `Penetración AA`,
        Var_penetracion_AA as `Var % Penetración`,
        peso_PG_actual as `Peso Pan Granel`,
        peso_PG_AA as `Peso Pan Granel AA`,
        Var_peso_PG_AA as `Var % Peso Pan Granel`,
        peso_por_visita_actual as `Peso por Visita Pan Granel`,
        peso_por_visita_AA as `Peso por Visita Pan Granel AA`,
        Var_peso_por_visita_PG_AA as `Var % Peso por Visita Pan Granel`,
        frecuencia_actual as Frecuencia,
        frecuencia_AA as `Frecuencia AA`,
        Var_frecuencia_AA as `Var % Frecuencia`
    FROM
    `cl-cda-unidata-prod.DS_UNIDATA_PROVEEDORES.PROVEEDORES_PROSEPAN_VENTAS_TRANSACCIONES_PENETRACION`

    WHERE SEMANA_ACTUAL_2 = '${execution_date}'
    """,
})

# -------------------------------------------------------------------------
# Main function
# -------------------------------------------------------------------------
def main() -> None:  # noqa: D103
    # Parse input variables
    args = vars(parser.parse_args())
    gcp_project_id: str = args['project_id']
    execution_date: str = args['execution_date']
    transacciones = ['TRANSACCIONES_PA','TRANSACCIONES_AA']
    # Set all clients
    semanas = f'{execution_date}-{int(execution_date) -1}'
    excel_transacciones = f'Transacciones Venta y Penetración {semanas}.xlsx'
    offset_buffer = io.BytesIO()
    with pd.ExcelWriter(offset_buffer, engine='openpyxl') as writer:
        for hoja in transacciones:
            #Read Query
            logging.info(f'Reading Query for {hoja}')
            transacciones_df = readBigQuery(
                query=SQL_QUERIES[hoja].substitute(
                    execution_date = execution_date
                ),
                user='csotob',
                gbq_client = Client()
            )

            logging.info (f'Query result: {transacciones_df.head()}')

            #Create excel from dataframe
            logging.info(f'Creating excel file for Transacciones Prosepan {semanas}')
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
