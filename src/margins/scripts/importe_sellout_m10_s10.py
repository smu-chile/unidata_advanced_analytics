# Default
import os
import logging
import argparse
from logging import config

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



# -------------------------------------------------------------------------
# Cleaning Func
# -------------------------------------------------------------------------
def cleaning_func(df,mes_carga):
    print('Before cleaning:', df)
    mes = df.columns[1].split(' ')[3]
    #Drop first 2 rows and last 4 columns
    df = df[2:]  # noqa: PD901
    logging.info('shape: %s', df.shape)
    df = df.iloc[:, :17]  # noqa: PD901

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
    #Agregar mes
    df['mes'] = mes
    df['mes_carga'] = mes_carga
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
    formatos = ['s10','m10']

    # Set all clients
    sp_cred = secretmanager.getSecret('sellout_sharepoint_credentials')
    gbq_client = bigquery.Client()
    #input files
    file_site = '/sites/SellOut/Documentos compartidos/SellOut Consolidado/'
    #TODO(csotob): Cambiar nombre de input:file y la logica de renombrar a
    # 'Procesado-' en GCP cuando se depreque en aws, eventualmente
    input_files = {
        's10': f'{file_site}/S10/PROCESADO-Sellout_S10_{execution_month}.xlsx',
        'm10': f'{file_site}/M10/PROCESADO-Sellout_M10_{execution_month}.xlsx',
            }
    #table definitions jsons
    jsons = {
        's10' : 'sellout_id0_s10.json',
        'm10' : 'sellout_id0_m10.json'
    }
    schema = 'ML_LAB'
    table_ref = {
        's10' : f'{gcp_project_id}.{schema}.REPORTE_MARGEN_SELLOUT_ID0_S10',
        'm10' : f'{gcp_project_id}.{schema}.REPORTE_MARGEN_SELLOUT_ID0_M10'
    }
    for file in formatos:
        logging.info(f'Starting extraction of -- sellout id0 {file} -- from Sharepoint')
        sharepoint = sp.SharePointFile(sp_cred['client_id'],
                                       sp_cred['client_secret'],input_files[file])
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
        #TODO(csotob): Agregar logica de verificar si archivo existe
        if modified:
            continue

        df_file = sharepoint.toFrame(sheet_name = 'ID 0')
        df_file =cleaning_func(df_file,execution_month)

        # Upload data
        logging.info('Uploading data')
        #Delete from table so that data is not duplicated
        gbq_extended.deleteFromTable(table_ref=table_ref[file],
                                     where_clause=f'mes_carga="{execution_month}"',
                                     gbq_client=gbq_client
                                     )
        gbq_extended.uploadFrame(
            df_file,
            table_ddl_json_path=os.path.join('gbq_objects', jsons[file]),
            project=gcp_project_id,
            gbq_client=gbq_client,
            if_exists='append',
        )
    logging.info('Process ended!')

if __name__ == '__main__':
    main()
