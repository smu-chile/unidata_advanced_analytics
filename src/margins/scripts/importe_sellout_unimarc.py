# Default
import os
import logging
import argparse
from logging import config

import pandas as pd

# pip
from google.cloud import bigquery
from office365.runtime.client_request_exception import ClientRequestException

import common.gcp_extended.bigquery as gbq_extended
import common.gcp_extended.secretsmanager as secretmanager

# Own
import common.office365_extended.sharepoint as sp
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
parser.add_argument(
    '--execution_month', type=str,
    help='DAG execution month'
)
parser.add_argument(
    '--execution_week', type=str,
    help='DAG execution week'
)



# -------------------------------------------------------------------------
# Cleaning Func
# -------------------------------------------------------------------------
def cleaning_func(df,mes,semana_carga):
    print('Before cleaning:', df)
    #Drop first 2 rows and last 4 columns
    df = df[2:]  # noqa: PD901
    df = df.iloc[:, :17]  # noqa: PD901
    logging.info('shape: %s', df.shape)
    #rename columns
    df.columns = ['id','nombre_proveedor', 'proveedor','n_cabecera','tipo_evento',
                  'fecha_inicio','fecha_termino','articulo','ean','descripcion',
                  'umv','importe_sell_out','id_promotions_consola_3','promocion',
                  'pvp_normal','factor','locales']

    #Limpiar
    df['n_cabecera'] = df['n_cabecera'].replace('-', '0')
    df['n_cabecera'] = df['n_cabecera'].replace('0', None)
    df['n_cabecera'] = df['n_cabecera'].astype('Float64').astype('Int64')


    df['importe_sell_out'] = df['importe_sell_out'].astype('Float64').astype('Int64')
    df['factor'] = df['factor'].astype('Float64').astype('Int64')
    #Agregar semana carga y mes datos
    df['mes'] = pd.to_datetime(mes,format='%Y%m')
    df['semana_carga'] = semana_carga
    print('After cleaning:', df)
    return df

# -------------------------------------------------------------------------
# Main function
# -------------------------------------------------------------------------
def main() -> None:  # noqa: D103
    # Parse input variables
    args = vars(parser.parse_args())
    gcp_project_id: str = args['project_id']
    execution_date: str = args['execution_date']
    execution_month: str = args['execution_month']
    execution_week: str = args['execution_week']

    # Set all clients
    sp_cred = secretmanager.getSecret('sellout_sharepoint_credentials')
    gbq_client = bigquery.Client()
    #input files
    file_site = '/sites/SellOut/Documentos compartidos'
    #TODO(csotob): Cambiar nombre de input:file y la logica de renombrar a
    # 'Procesado-' en GCP cuando se depreque en aws, eventualmente
    input_file =  f'{file_site}/PROCESADO-Sellout_{execution_month}.xlsx'
    #table definitions jsons
    json = 'sellout_unimarc.json'
    schema = 'ML_LAB'
    table_ref =  f'{gcp_project_id}.{schema}.REPORTE_MARGEN_SELLOUT_UNIMARC'

    logging.info(f'Starting extraction of -- sellout unimarc {input_file} -- from Sharepoint')
    sharepoint = sp.SharePointFile(sp_cred['client_id'],
                                   sp_cred['client_secret'],input_file)
    modified = False
    try:
        last_time_modified = sharepoint.lastTimeModified()
        logging.info(f'Last time modified: {last_time_modified}')
        logging.info(f'Execution date: {execution_date}')
        if last_time_modified.strftime('%Y-%m-%d') == execution_date:
            modified = True
    except ClientRequestException:
        logging.info('Error getting file')
        return

    if modified:
        logging.info('Archivo existe y fue modificado hoy')
        df_file = sharepoint.toFrame()
        df_file =cleaning_func(df_file,execution_month,execution_week)
        # Upload data
        logging.info('Uploading data')
        logging.info('Creating table')
        gbq_extended.createTableFromJSON(
                    table_ddl_json_path=os.path.join('gbq_objects', json),
                    project=gcp_project_id,
                    gbq_client=gbq_client,
                    if_exists='ignore',
                )
        where_clause=f'semana_carga="{execution_week}"'  # noqa: E501
        #Delete from table so that data is not duplicated
        logging.info('Delete from to avoid duplicates')
        gbq_extended.deleteFromTable(table_ref=table_ref,
                                    where_clause= where_clause,
                                    gbq_client=gbq_client
                                    )
        logging.info('Uploading dataframe')
        gbq_extended.uploadFrame(
            df_file,
            table_ddl_json_path=os.path.join('gbq_objects', json),
            project=gcp_project_id,
            gbq_client=gbq_client,
            if_exists='append',
        )
        logging.info('Data uploaded')
    logging.info('Process ended!')

if __name__ == '__main__':
    main()
